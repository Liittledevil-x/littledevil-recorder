"""Crash-safe, append-friendly Parquet persistence.

Legacy recorder files remain at ``{kind}/{symbol}/{day}.parquet``. New
recording writes immutable, complete parts below ``{kind}/{symbol}/{day}/``
and records visible parts in ``_local/recorder_state.sqlite``. Readers must
use :func:`iter_parquet_paths`: it returns a legacy file (when present) and
then manifest-active parts. Temporary, orphaned and superseded parts are
never visible.

The manifest is also the durable trade-ID uniqueness boundary. A trade part
is fully written and atomically renamed before a SQLite transaction makes the
part and its IDs visible together. A crash before that commit leaves an
ignored orphan; a crash after it has both the part and index.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Iterator

import pyarrow as pa
import pyarrow.parquet as pq

TRADES_SCHEMA = pa.schema([
    ("trade_id", pa.int64()), ("ts_exchange", pa.timestamp("us", tz="UTC")),
    ("ts_received", pa.timestamp("us", tz="UTC")), ("price", pa.float64()),
    ("qty", pa.float64()), ("is_buyer_maker", pa.bool_()), ("venue", pa.string()),
])
DEPTH_SCHEMA = pa.schema([
    ("ts_exchange", pa.timestamp("us", tz="UTC")), ("ts_received", pa.timestamp("us", tz="UTC")),
    ("is_snapshot", pa.bool_()), ("bids", pa.string()), ("asks", pa.string()), ("seq", pa.int64()),
])

FLUSH_BATCH_SIZE = 20_000
STATE_FILENAME = "recorder_state.sqlite"


class FlushError(Exception):
    def __init__(self, kind: str, symbol: str, day: date, cause: Exception) -> None:
        super().__init__(f"failed to flush {kind}/{symbol}/{day.isoformat()}: {cause!r}")
        self.kind, self.symbol, self.day, self.cause = kind, symbol, day, cause


class FlushAllError(Exception):
    def __init__(self, written: list[Path], errors: list[FlushError]) -> None:
        super().__init__(f"{len(errors)} of {len(written) + len(errors)} flush(es) failed")
        self.written, self.errors = written, errors


class CompactionError(Exception):
    def __init__(self, kind: str, symbol: str, day: str, cause: Exception) -> None:
        super().__init__(f"failed to compact {kind}/{symbol}/{day}: {cause!r}")
        self.kind, self.symbol, self.day, self.cause = kind, symbol, day, cause


def _utc_date(ts: datetime) -> date:
    return ts.astimezone(UTC).date()


def _state_path(root: Path) -> Path:
    path = root / "_local" / STATE_FILENAME
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _fsync_directory(path: Path) -> None:
    """Persist a rename's directory entry before its SQLite publication."""
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class _State:
    """Small local manifest. It stores metadata and IDs, never Parquet rows."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.conn = sqlite3.connect(_state_path(root), timeout=30, isolation_level=None)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS parts (
                relative_path TEXT PRIMARY KEY,
                kind TEXT NOT NULL, symbol TEXT NOT NULL, day TEXT NOT NULL,
                row_count INTEGER NOT NULL,
                state TEXT NOT NULL CHECK(state IN ('active', 'superseded', 'quarantined')),
                created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS active_parts_by_day ON parts(kind, symbol, day, state);
            CREATE TABLE IF NOT EXISTS trade_ids (
                symbol TEXT NOT NULL, trade_id INTEGER NOT NULL, relative_path TEXT NOT NULL,
                PRIMARY KEY(symbol, trade_id)
            );
            CREATE TABLE IF NOT EXISTS legacy_trade_symbols (
                symbol TEXT PRIMARY KEY, indexed_at TEXT NOT NULL
            );
        """)

    def close(self) -> None:
        self.conn.close()

    def _bootstrap_legacy_symbol(self, symbol: str) -> None:
        """Index legacy files once in bounded batches, without touching them."""
        if self.conn.execute("SELECT 1 FROM legacy_trade_symbols WHERE symbol = ?", (symbol,)).fetchone():
            return
        base = self.root / "trades" / symbol
        if base.exists():
            for path in sorted(base.glob("????-??-??.parquet")):
                parquet = pq.ParquetFile(path)
                try:
                    for batch in parquet.iter_batches(columns=["trade_id"], batch_size=FLUSH_BATCH_SIZE):
                        self.conn.executemany(
                            "INSERT OR IGNORE INTO trade_ids(symbol, trade_id, relative_path) VALUES (?, ?, ?)",
                            ((symbol, int(trade_id), str(path.relative_to(self.root)))
                             for trade_id in batch.column("trade_id").to_pylist()),
                        )
                finally:
                    parquet.close()
        self.conn.execute("INSERT INTO legacy_trade_symbols(symbol, indexed_at) VALUES (?, ?)",
                          (symbol, datetime.now(UTC).isoformat()))

    def unseen_trade_ids(self, symbol: str, ids: set[int]) -> set[int]:
        self._bootstrap_legacy_symbol(symbol)
        seen: set[int] = set()
        ordered = list(ids)
        for start in range(0, len(ordered), 500):
            chunk = ordered[start:start + 500]
            marks = ",".join("?" for _ in chunk)
            seen.update(row[0] for row in self.conn.execute(
                f"SELECT trade_id FROM trade_ids WHERE symbol = ? AND trade_id IN ({marks})",  # noqa: S608
                (symbol, *chunk),
            ))
        return ids - seen

    def active_paths(self, kind: str, symbol: str, day: str) -> list[Path]:
        return [self.root / row[0] for row in self.conn.execute(
            "SELECT relative_path FROM parts WHERE kind = ? AND symbol = ? AND day = ? AND state = 'active' ORDER BY created_at, relative_path",
            (kind, symbol, day),
        )]

    def active_part_rows(self, kind: str, symbol: str, day: str) -> list[tuple[str, int]]:
        return list(self.conn.execute(
            "SELECT relative_path, row_count FROM parts WHERE kind = ? AND symbol = ? AND day = ? AND state = 'active' ORDER BY created_at, relative_path",
            (kind, symbol, day),
        ))

    def insert_part(self, *, relative_path: str, kind: str, symbol: str, day: str,
                    row_count: int, trade_ids: set[int] | None = None) -> None:
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            if trade_ids and self.unseen_trade_ids(symbol, trade_ids) != trade_ids:
                raise RuntimeError("trade ID set changed while publishing part")
            self.conn.execute(
                "INSERT INTO parts(relative_path, kind, symbol, day, row_count, state, created_at) VALUES (?, ?, ?, ?, ?, 'active', ?)",
                (relative_path, kind, symbol, day, row_count, datetime.now(UTC).isoformat()),
            )
            if trade_ids:
                self.conn.executemany("INSERT INTO trade_ids(symbol, trade_id, relative_path) VALUES (?, ?, ?)",
                                      ((symbol, trade_id, relative_path) for trade_id in trade_ids))
            self.conn.execute("COMMIT")
        except Exception:
            self.conn.execute("ROLLBACK")
            raise

    def compact(self, *, kind: str, symbol: str, day: str, relative_path: str,
                row_count: int, source_paths: list[str]) -> None:
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            current = {row[0] for row in self.conn.execute(
                "SELECT relative_path FROM parts WHERE kind = ? AND symbol = ? AND day = ? AND state = 'active'",
                (kind, symbol, day),
            )}
            if not set(source_paths).issubset(current):
                raise RuntimeError("parts changed while compaction was running")
            self.conn.execute(
                "INSERT INTO parts(relative_path, kind, symbol, day, row_count, state, created_at) VALUES (?, ?, ?, ?, ?, 'active', ?)",
                (relative_path, kind, symbol, day, row_count, datetime.now(UTC).isoformat()),
            )
            self.conn.executemany("UPDATE parts SET state = 'superseded' WHERE relative_path = ?",
                                  ((path,) for path in source_paths))
            self.conn.execute("COMMIT")
        except Exception:
            self.conn.execute("ROLLBACK")
            raise


def iter_parquet_paths(root: Path, kind: str, symbol: str, day: date | str) -> Iterator[Path]:
    """Yield legacy then active partitioned files for one logical day."""
    day_text = day.isoformat() if isinstance(day, date) else day
    legacy = root / kind / symbol / f"{day_text}.parquet"
    if legacy.exists():
        yield legacy
    state = _State(root)
    try:
        yield from state.active_paths(kind, symbol, day_text)
    finally:
        state.close()


def bootstrap_legacy_trade_index(root: Path, symbols: list[str]) -> None:
    """Prebuild the local ID index before a production cutover.

    It streams old Parquet files and writes only the new local SQLite sidecar;
    calling it again is safe and skips symbols already marked complete.
    """
    state = _State(root)
    try:
        for symbol in symbols:
            state._bootstrap_legacy_symbol(symbol)
    finally:
        state.close()


@dataclass
class _Buffer:
    rows: list[tuple] = field(default_factory=list)
    trade_ids: set[int] = field(default_factory=set)


class ParquetWriter:
    """Buffers one flush interval and appends complete immutable parts.

    No live flush reads or rewrites a published part or daily legacy file;
    memory and write I/O are O(current buffered rows).
    """

    def __init__(self, data_root: Path) -> None:
        self._root = data_root
        self._trades: dict[tuple[str, date], _Buffer] = {}
        self._depth: dict[tuple[str, date], _Buffer] = {}
        self._state = _State(data_root)

    def write_trade(self, symbol: str, *, trade_id: int, ts_exchange: datetime, ts_received: datetime,
                    price: float, qty: float, is_buyer_maker: bool, venue: str = "binance_spot") -> None:
        key = (symbol, _utc_date(ts_exchange))
        buf = self._trades.setdefault(key, _Buffer())
        if trade_id not in buf.trade_ids:
            buf.trade_ids.add(trade_id)
            buf.rows.append((trade_id, ts_exchange, ts_received, price, qty, is_buyer_maker, venue))

    def write_depth(self, symbol: str, *, ts_exchange: datetime, ts_received: datetime, is_snapshot: bool,
                    bids_json: str, asks_json: str, seq: int) -> None:
        self._depth.setdefault((symbol, _utc_date(ts_exchange)), _Buffer()).rows.append(
            (ts_exchange, ts_received, is_snapshot, bids_json, asks_json, seq)
        )

    def flush_trades(self, symbol: str, day: date) -> Path | None:
        return self._flush(self._trades, "trades", TRADES_SCHEMA, symbol, day)

    def flush_depth(self, symbol: str, day: date) -> Path | None:
        return self._flush(self._depth, "depth", DEPTH_SCHEMA, symbol, day)

    def flush_all(self) -> list[Path]:
        written: list[Path] = []
        errors: list[FlushError] = []
        for buffers, kind, schema in ((self._trades, "trades", TRADES_SCHEMA), (self._depth, "depth", DEPTH_SCHEMA)):
            for symbol, day in list(buffers):
                try:
                    path = self._flush(buffers, kind, schema, symbol, day)
                    if path:
                        written.append(path)
                except Exception as exc:  # a bad key must not block the rest
                    errors.append(FlushError(kind, symbol, day, exc))
        if errors:
            raise FlushAllError(written, errors)
        return written

    def _flush(self, buffers: dict[tuple[str, date], _Buffer], kind: str, schema: pa.Schema,
               symbol: str, day: date) -> Path | None:
        key = (symbol, day)
        buf = buffers.get(key)
        if not buf or not buf.rows:
            return None
        rows = buf.rows
        trade_ids: set[int] | None = None
        if kind == "trades":
            candidate = {row[0] for row in rows}
            trade_ids = self._state.unseen_trade_ids(symbol, candidate)
            rows = [row for row in rows if row[0] in trade_ids]
            if not rows:
                del buffers[key]
                return None

        part_dir = self._root / kind / symbol / day.isoformat()
        part_dir.mkdir(parents=True, exist_ok=True)
        stem = f"part-{uuid.uuid4().hex}"
        final_path = part_dir / f"{stem}.parquet"
        tmp_path = part_dir / f"{stem}.parquet.tmp"
        columns = list(zip(*rows, strict=True))
        table = pa.Table.from_arrays(
            [pa.array(col, type=field.type) for col, field in zip(columns, schema, strict=True)], schema=schema,
        )
        try:
            pq.write_table(table, tmp_path)
            fd = os.open(tmp_path, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
            os.replace(tmp_path, final_path)
            _fsync_directory(part_dir)
            self._state.insert_part(relative_path=str(final_path.relative_to(self._root)), kind=kind,
                                    symbol=symbol, day=day.isoformat(), row_count=len(rows), trade_ids=trade_ids)
        except Exception:
            # An already renamed, uncommitted part is forensic evidence only:
            # no manifest row means readers and recovery ignore it.
            raise
        del buffers[key]
        return final_path

    def close(self) -> None:
        self._state.close()


def compact_closed_day(root: Path, kind: str, symbol: str, day: date) -> Path | None:
    """Compact active parts for a closed day; never rewrite legacy history."""
    if day >= datetime.now(UTC).date():
        return None
    state = _State(root)
    try:
        sources = state.active_part_rows(kind, symbol, day.isoformat())
        if len(sources) < 2:
            return None
        schema = TRADES_SCHEMA if kind == "trades" else DEPTH_SCHEMA
        part_dir = root / kind / symbol / day.isoformat()
        stem = f"compacted-{uuid.uuid4().hex}"
        final_path, tmp_path = part_dir / f"{stem}.parquet", part_dir / f"{stem}.parquet.tmp"
        row_count = 0
        try:
            with pq.ParquetWriter(tmp_path, schema) as writer:
                for relative, _ in sources:
                    parquet = pq.ParquetFile(root / relative)
                    try:
                        for batch in parquet.iter_batches(batch_size=FLUSH_BATCH_SIZE):
                            writer.write_table(pa.Table.from_batches([batch]))
                            row_count += batch.num_rows
                    finally:
                        parquet.close()
            os.replace(tmp_path, final_path)
            _fsync_directory(part_dir)
            state.compact(kind=kind, symbol=symbol, day=day.isoformat(),
                          relative_path=str(final_path.relative_to(root)), row_count=row_count,
                          source_paths=[relative for relative, _ in sources])
            return final_path
        except Exception as exc:
            raise CompactionError(kind, symbol, day.isoformat(), exc) from exc
    finally:
        state.close()


def closed_part_days(root: Path, kind: str, symbol: str) -> list[date]:
    """Manifest-backed compaction candidates; never includes today's writes."""
    state = _State(root)
    try:
        today = datetime.now(UTC).date().isoformat()
        return [date.fromisoformat(row[0]) for row in state.conn.execute(
            "SELECT DISTINCT day FROM parts WHERE kind = ? AND symbol = ? AND state = 'active' AND day < ? ORDER BY day",
            (kind, symbol, today),
        )]
    finally:
        state.close()


def quarantine_part_day(root: Path, kind: str, symbol: str, day: str, timestamp: str) -> Path | None:
    """Remove every part for a logical day from reader visibility by rename,
    preserving the exact corrupt artifacts beside the live dataset."""
    source = root / kind / symbol / day
    if not source.exists():
        return None
    destination = source.with_name(f"{day}.quarantined-{timestamp}")
    os.replace(source, destination)
    state = _State(root)
    try:
        state.conn.execute(
            "UPDATE parts SET state = 'quarantined' WHERE kind = ? AND symbol = ? AND day = ? AND state = 'active'",
            (kind, symbol, day),
        )
    finally:
        state.close()
    return destination
