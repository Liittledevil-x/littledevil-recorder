"""Restart-safe Binance Spot aggTrade corpus builder.

This is deliberately separate from the live recorder and its ``ParquetWriter``.
It writes one immutable, legacy-compatible Parquet file per ``(symbol, UTC
day)`` below a caller-provided *corpus* root, never the live data root.  A small
SQLite manifest records source and output checksums, coverage and validation
metadata only; it never stores trade rows.

The historical corpus is Class-A evidence: candles are reconstructed from its
aggTrades by the engine.  It does not claim to provide historical L2 depth.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import fcntl
import hashlib
import io
import json
import logging
import os
import sqlite3
import uuid
import zipfile
from collections.abc import Iterator, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

from littledevil_recorder.backfill import AGGTRADE_COLUMNS, ARCHIVE_HOST, archive_microseconds_to_datetime
from littledevil_recorder.storage import TRADES_SCHEMA

logger = logging.getLogger(__name__)

DEFAULT_STUDY_START = date(2025, 9, 19)
DEFAULT_STUDY_END = date(2026, 9, 13)
DEFAULT_STUDY_SYMBOLS = (
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "TRXUSDT", "ADAUSDT",
    "DOGEUSDT", "LTCUSDT", "LINKUSDT", "AVAXUSDT", "DOTUSDT", "ATOMUSDT", "NEARUSDT",
    "SUIUSDT", "APTUSDT", "ARBUSDT", "OPUSDT", "UNIUSDT", "AAVEUSDT", "HBARUSDT",
    "INJUSDT", "PEPEUSDT", "SHIBUSDT", "WIFUSDT",
)
PARSE_BATCH_SIZE = 5_000


class CorpusError(RuntimeError):
    """A permanent integrity/configuration problem in a corpus run."""


class SourceUnavailable(CorpusError):
    """Binance does not expose this symbol/day archive."""


@dataclass(frozen=True)
class CorpusConfig:
    root: Path
    symbols: tuple[str, ...]
    start: date
    end: date
    concurrency: int = 1

    def __post_init__(self) -> None:
        if self.start > self.end:
            raise ValueError("start must not be after end")
        if not self.symbols:
            raise ValueError("at least one symbol is required")
        if self.concurrency < 1:
            raise ValueError("concurrency must be positive")

    def specification(self) -> dict[str, Any]:
        return {
            "format": 1,
            "source": "binance-spot-daily-aggtrades",
            "evidence_class": "A",
            "symbols": list(self.symbols),
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
        }


@dataclass(frozen=True)
class DayResult:
    symbol: str
    day: str
    status: str
    row_count: int = 0
    detail: str | None = None


def _days(start: date, end: date) -> Iterator[date]:
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fsync_directory(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class HistoricalCorpus:
    """Build and validate an isolated, immutable historical corpus."""

    def __init__(self, config: CorpusConfig) -> None:
        self.config = config
        self.root = config.root.resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self._lock_file = None
        self.conn = sqlite3.connect(self.root / "corpus_manifest.sqlite", timeout=30, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS days (
                symbol TEXT NOT NULL, day TEXT NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('complete', 'unavailable', 'failed')),
                source_path TEXT, source_sha256 TEXT, source_bytes INTEGER,
                output_path TEXT, output_sha256 TEXT, output_bytes INTEGER,
                row_count INTEGER, first_trade_id INTEGER, last_trade_id INTEGER,
                first_ts TEXT, last_ts TEXT, error TEXT, updated_at TEXT NOT NULL,
                source_duplicate_count INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(symbol, day)
            );
            CREATE INDEX IF NOT EXISTS complete_id_ranges ON days(symbol, status, first_trade_id, last_trade_id);
            CREATE TABLE IF NOT EXISTS partial_artifacts (
                relative_path TEXT PRIMARY KEY, kind TEXT NOT NULL, detail TEXT NOT NULL,
                detected_at TEXT NOT NULL
            );
        """)
        try:
            self.conn.execute("ALTER TABLE days ADD COLUMN source_duplicate_count INTEGER NOT NULL DEFAULT 0")
        except sqlite3.OperationalError as exc:
            if "duplicate column name" not in str(exc):
                raise
        self._assert_specification()

    def close(self) -> None:
        self.conn.close()
        if self._lock_file is not None:
            fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_UN)
            self._lock_file.close()
            self._lock_file = None

    def __enter__(self) -> "HistoricalCorpus":
        lock_path = self.root / ".historical-corpus.lock"
        self._lock_file = lock_path.open("a+")
        try:
            fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            self._lock_file.close()
            self._lock_file = None
            raise CorpusError(f"another historical-corpus run holds {lock_path}") from exc
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _assert_specification(self) -> None:
        requested = json.dumps(self.config.specification(), sort_keys=True, separators=(",", ":"))
        existing = self.conn.execute("SELECT value FROM metadata WHERE key = 'specification'").fetchone()
        if existing is None:
            self.conn.execute("INSERT INTO metadata(key, value) VALUES ('specification', ?)", (requested,))
        elif existing["value"] != requested:
            raise CorpusError(
                "corpus root is already bound to a different immutable study specification; "
                "choose a new empty root rather than mixing study windows or symbols"
            )

    def source_path(self, symbol: str, day: date) -> Path:
        return self.root / "source" / "spot" / "aggTrades" / symbol / f"{day.isoformat()}.zip"

    def output_path(self, symbol: str, day: date) -> Path:
        # Intentional legacy layout: ArchiveLoader reads it without a live-recorder dependency.
        return self.root / "trades" / symbol / f"{day.isoformat()}.parquet"

    def _row(self, symbol: str, day: date) -> sqlite3.Row | None:
        return self.conn.execute("SELECT * FROM days WHERE symbol = ? AND day = ?", (symbol, day.isoformat())).fetchone()

    def _record(self, result: DayResult, **fields: Any) -> None:
        now = datetime.now(UTC).isoformat()
        payload = {"symbol": result.symbol, "day": result.day, "status": result.status,
                   "updated_at": now, **fields}
        columns = ", ".join(payload)
        values = ", ".join(f":{key}" for key in payload)
        updates = ", ".join(f"{key} = excluded.{key}" for key in payload if key not in {"symbol", "day"})
        self.conn.execute(
            f"INSERT INTO days ({columns}) VALUES ({values}) "  # noqa: S608 -- column set is static
            f"ON CONFLICT(symbol, day) DO UPDATE SET {updates}", payload,
        )

    def _quarantine(self, path: Path, reason: str) -> None:
        if not path.exists():
            return
        quarantined = path.with_name(f"{path.name}.corrupt-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}")
        os.replace(path, quarantined)
        _fsync_directory(path.parent)
        logger.warning("quarantined %s as %s (%s)", path, quarantined, reason)

    def _quarantine_stale_partials(self, path: Path, kind: str) -> None:
        """Make an interrupted download/write visible instead of silently replacing it."""
        if not path.parent.exists():
            return
        for partial in path.parent.glob(f"{path.name}.partial-*"):
            forensic = partial.with_name(
                f"{partial.name}.interrupted-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"
            )
            os.replace(partial, forensic)
            _fsync_directory(partial.parent)
            self.conn.execute(
                "INSERT OR REPLACE INTO partial_artifacts(relative_path, kind, detail, detected_at) VALUES (?, ?, ?, ?)",
                (str(forensic.relative_to(self.root)), kind, "interrupted partial recovered before resume", datetime.now(UTC).isoformat()),
            )
            logger.warning("preserved interrupted %s partial as %s", kind, forensic)

    def _complete_output_is_healthy(self, row: sqlite3.Row, path: Path) -> bool:
        if not path.exists() or path.stat().st_size != row["output_bytes"]:
            return False
        try:
            parquet = pq.ParquetFile(path)
            try:
                return parquet.metadata.num_rows == row["row_count"] and parquet.schema_arrow == TRADES_SCHEMA
            finally:
                parquet.close()
        except Exception:
            return False

    async def _download_source(self, client: httpx.AsyncClient, symbol: str, day: date) -> tuple[Path, str, int]:
        final = self.source_path(symbol, day)
        final.parent.mkdir(parents=True, exist_ok=True)
        self._quarantine_stale_partials(final, "source")
        if final.exists():
            try:
                with zipfile.ZipFile(final) as archive:
                    if archive.testzip() is None:
                        return final, _sha256(final), final.stat().st_size
            except (OSError, zipfile.BadZipFile):
                pass
            self._quarantine(final, "cached source failed ZIP integrity check")

        temporary = final.with_name(f"{final.name}.partial-{uuid.uuid4().hex}")
        url = f"{ARCHIVE_HOST}/data/spot/daily/aggTrades/{symbol}/{symbol}-aggTrades-{day.isoformat()}.zip"
        try:
            async with client.stream("GET", url) as response:
                if response.status_code == 404:
                    raise SourceUnavailable(f"archive unavailable: {symbol}/{day.isoformat()}")
                response.raise_for_status()
                digest = hashlib.sha256()
                with temporary.open("wb") as target:
                    async for chunk in response.aiter_bytes(1024 * 1024):
                        digest.update(chunk)
                        target.write(chunk)
                    target.flush()
                    os.fsync(target.fileno())
            with zipfile.ZipFile(temporary) as archive:
                invalid_member = archive.testzip()
                if invalid_member is not None:
                    raise CorpusError(f"ZIP CRC failed for member {invalid_member}")
            os.replace(temporary, final)
            _fsync_directory(final.parent)
            return final, digest.hexdigest(), final.stat().st_size
        finally:
            if temporary.exists():
                temporary.unlink()

    def _assert_non_overlapping_id_range(self, symbol: str, first_id: int, last_id: int, day: date) -> None:
        overlapping = self.conn.execute(
            """SELECT day FROM days WHERE symbol = ? AND status = 'complete' AND day != ?
               AND NOT (last_trade_id < ? OR first_trade_id > ?) LIMIT 1""",
            (symbol, day.isoformat(), first_id, last_id),
        ).fetchone()
        if overlapping:
            raise CorpusError(
                f"aggTrade ID range overlaps completed {symbol}/{overlapping['day']}; refusing duplicate corpus data"
            )

    def _write_day(self, source: Path, destination: Path, symbol: str, day: date) -> dict[str, Any]:
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_name(f"{destination.name}.partial-{uuid.uuid4().hex}")
        spool = temporary.with_name(f"{temporary.name}.ids.sqlite")
        row_count = 0
        source_duplicate_count = 0
        first_id: int | None = None
        last_id: int | None = None
        first_ts: datetime | None = None
        last_ts: datetime | None = None
        try:
            # Binance's public daily files are normally ID-ordered, but two
            # production files in this corpus contain a unique lower-ID block
            # after a later block.  Spooling IDs on disk lets us reject true
            # duplicates without an unbounded Python set and publish the
            # final day in deterministic trade-ID order.
            spool_db = sqlite3.connect(spool)
            spool_db.execute("PRAGMA journal_mode=OFF")
            spool_db.execute("PRAGMA synchronous=OFF")
            spool_db.execute("CREATE TABLE rows (trade_id INTEGER PRIMARY KEY, ts_us INTEGER NOT NULL, price REAL NOT NULL, qty REAL NOT NULL, is_buyer_maker INTEGER NOT NULL)")
            with zipfile.ZipFile(source) as archive:
                members = [member for member in archive.infolist() if not member.is_dir()]
                if len(members) != 1:
                    raise CorpusError(f"expected exactly one CSV member in {source}, got {len(members)}")
                with archive.open(members[0]) as raw, io.TextIOWrapper(raw, encoding="utf-8", newline="") as text:
                    for csv_row in csv.reader(text):
                        if len(csv_row) != len(AGGTRADE_COLUMNS):
                            raise CorpusError(f"unexpected aggTrade column count {len(csv_row)} in {source}")
                        trade_id = int(csv_row[0])
                        values = (trade_id, int(csv_row[5]), float(csv_row[1]), float(csv_row[2]),
                                  csv_row[6].lower() == "true")
                        try:
                            spool_db.execute(
                                "INSERT INTO rows(trade_id, ts_us, price, qty, is_buyer_maker) VALUES (?, ?, ?, ?, ?)",
                                values,
                            )
                        except sqlite3.IntegrityError as exc:
                            previous = spool_db.execute(
                                "SELECT trade_id, ts_us, price, qty, is_buyer_maker FROM rows WHERE trade_id = ?",
                                (trade_id,),
                            ).fetchone()
                            if previous == values:
                                source_duplicate_count += 1
                                continue
                            raise CorpusError(f"conflicting duplicate aggTrade ID {trade_id} in {symbol}/{day}") from exc
                        row_count += 1
            spool_db.commit()
            with pq.ParquetWriter(temporary, TRADES_SCHEMA, compression="zstd") as writer:
                rows: list[tuple[Any, ...]] = []
                for trade_id, ts_us, price, qty, is_buyer_maker in spool_db.execute(
                    "SELECT trade_id, ts_us, price, qty, is_buyer_maker FROM rows ORDER BY trade_id"
                ):
                    timestamp = archive_microseconds_to_datetime(ts_us)
                    first_id = trade_id if first_id is None else first_id
                    first_ts = timestamp if first_ts is None else first_ts
                    last_id, last_ts = trade_id, timestamp
                    rows.append((trade_id, timestamp, timestamp, price, qty, bool(is_buyer_maker), "binance_spot_archive"))
                    if len(rows) == PARSE_BATCH_SIZE:
                        writer.write_table(pa.Table.from_pylist(
                            [dict(zip(TRADES_SCHEMA.names, row, strict=True)) for row in rows], schema=TRADES_SCHEMA
                        ))
                        rows.clear()
                if rows:
                    writer.write_table(pa.Table.from_pylist(
                        [dict(zip(TRADES_SCHEMA.names, row, strict=True)) for row in rows], schema=TRADES_SCHEMA
                    ))
            spool_db.close()
            if row_count == 0 or first_id is None or last_id is None or first_ts is None or last_ts is None:
                raise CorpusError(f"empty aggTrade archive for {symbol}/{day}")
            self._assert_non_overlapping_id_range(symbol, first_id, last_id, day)
            with temporary.open("rb") as output:
                os.fsync(output.fileno())
            os.replace(temporary, destination)
            _fsync_directory(destination.parent)
            return {
                "row_count": row_count, "first_trade_id": first_id, "last_trade_id": last_id,
                "first_ts": first_ts.isoformat(), "last_ts": last_ts.isoformat(),
                "source_duplicate_count": source_duplicate_count,
                "output_sha256": _sha256(destination), "output_bytes": destination.stat().st_size,
            }
        finally:
            if temporary.exists():
                temporary.unlink()
            if spool.exists():
                spool.unlink()

    async def process_day(self, client: httpx.AsyncClient, symbol: str, day: date) -> DayResult:
        existing = self._row(symbol, day)
        output = self.output_path(symbol, day)
        self._quarantine_stale_partials(output, "output")
        if existing and existing["status"] == "complete" and self._complete_output_is_healthy(existing, output):
            return DayResult(symbol, day.isoformat(), "skipped", int(existing["row_count"]))
        if existing and existing["status"] == "complete":
            self._quarantine(output, "manifest/file integrity mismatch before resume")
        if existing and existing["status"] == "unavailable":
            return DayResult(symbol, day.isoformat(), "unavailable", detail=existing["error"])
        try:
            source, source_sha, source_bytes = await self._download_source(client, symbol, day)
            details = self._write_day(source, output, symbol, day)
            self._record(DayResult(symbol, day.isoformat(), "complete", details["row_count"]),
                         source_path=str(source.relative_to(self.root)), source_sha256=source_sha,
                         source_bytes=source_bytes, output_path=str(output.relative_to(self.root)), error=None, **details)
            return DayResult(symbol, day.isoformat(), "complete", details["row_count"])
        except SourceUnavailable as exc:
            result = DayResult(symbol, day.isoformat(), "unavailable", detail=str(exc))
            self._record(result, error=str(exc))
            return result
        except Exception as exc:
            result = DayResult(symbol, day.isoformat(), "failed", detail=repr(exc))
            self._record(result, error=repr(exc))
            logger.exception("historical corpus failed: %s/%s", symbol, day)
            return result

    async def run(self, client: httpx.AsyncClient | None = None) -> list[DayResult]:
        """Run every requested day; failures are manifest-recorded and do not hide other results."""
        own_client = client is None
        active_client = client or httpx.AsyncClient(timeout=httpx.Timeout(120.0, connect=20.0), follow_redirects=True)
        semaphore = asyncio.Semaphore(self.config.concurrency)

        async def one(symbol: str) -> list[DayResult]:
            async with semaphore:
                results: list[DayResult] = []
                for current_day in _days(self.config.start, self.config.end):
                    results.append(await self.process_day(active_client, symbol, current_day))
                return results

        try:
            return [result for group in await asyncio.gather(*(one(symbol) for symbol in self.config.symbols)) for result in group]
        finally:
            if own_client:
                await active_client.aclose()

    def validate(self, *, verify_hashes: bool = False) -> list[dict[str, Any]]:
        """Stream each completed file for acceptance reporting; never materializes a day."""
        report: list[dict[str, Any]] = []
        for row in self.conn.execute("SELECT * FROM days ORDER BY symbol, day"):
            item = dict(row)
            path = self.root / item["output_path"] if item["output_path"] else None
            status = item["status"]
            errors: list[str] = []
            observed_count = 0
            previous_id: int | None = None
            previous_ts: datetime | None = None
            if status == "complete":
                if path is None or not path.exists():
                    errors.append("output missing")
                else:
                    try:
                        parquet = pq.ParquetFile(path)
                        try:
                            for batch in parquet.iter_batches(columns=["trade_id", "ts_exchange"], batch_size=PARSE_BATCH_SIZE):
                                ids = batch.column("trade_id").to_pylist()
                                timestamps = batch.column("ts_exchange").to_pylist()
                                for trade_id, timestamp in zip(ids, timestamps, strict=True):
                                    observed_count += 1
                                    if previous_id is not None and trade_id <= previous_id:
                                        errors.append("duplicate/non-increasing trade_id")
                                        break
                                    if previous_ts is not None and timestamp < previous_ts:
                                        errors.append("non-monotonic timestamp")
                                        break
                                    previous_id, previous_ts = int(trade_id), timestamp
                                if errors:
                                    break
                        finally:
                            parquet.close()
                    except Exception as exc:
                        errors.append(f"Parquet read failure: {exc!r}")
                    if observed_count != item["row_count"]:
                        errors.append(f"row count {observed_count} != manifest {item['row_count']}")
                    if verify_hashes and not errors and _sha256(path) != item["output_sha256"]:
                        errors.append("output SHA-256 mismatch")
            item["observed_row_count"] = observed_count
            item["validation"] = "ok" if status == "complete" and not errors else ("unavailable" if status == "unavailable" else "failed")
            item["validation_errors"] = errors
            report.append(item)
        return report

    def partial_artifacts(self) -> list[dict[str, str]]:
        return [dict(row) for row in self.conn.execute("SELECT * FROM partial_artifacts ORDER BY detected_at, relative_path")]


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid ISO date: {value}") from exc


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Build an isolated, resumable Binance Spot aggTrade corpus")
    parser.add_argument("--root", required=True, type=Path, help="empty/dedicated corpus root; never the live recorder root")
    parser.add_argument("--symbols", default=",".join(DEFAULT_STUDY_SYMBOLS), help="comma-separated fixed Spot symbols")
    parser.add_argument("--start", type=_parse_date, default=DEFAULT_STUDY_START)
    parser.add_argument("--end", type=_parse_date, default=DEFAULT_STUDY_END)
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument("--verify-hashes", action="store_true", help="rehash every completed Parquet output during validation")
    args = parser.parse_args(argv)
    symbols = tuple(symbol.strip().upper() for symbol in args.symbols.split(",") if symbol.strip())
    config = CorpusConfig(args.root, symbols, args.start, args.end, args.concurrency)
    with HistoricalCorpus(config) as corpus:
        if not args.validate_only:
            results = asyncio.run(corpus.run())
            logger.info("corpus run: %s", {status: sum(r.status == status for r in results)
                                             for status in ("complete", "skipped", "unavailable", "failed")})
        report = corpus.validate(verify_hashes=args.verify_hashes)
        print(json.dumps({"days": report, "partial_artifacts": corpus.partial_artifacts()}, indent=2, sort_keys=True, default=str))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    main()
