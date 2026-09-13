"""Parquet writers matching docs/data-and-events.md §2's layout exactly:

    data/trades/{symbol}/{date}.parquet
    data/depth/{symbol}/{date}.parquet     -- recording set only

Buffers rows in memory and flushes a whole UTC-day file at a time (or on
demand for tests). Never writes klines/features/funding_oi/liquidations --
those are Trunk-and-later (candle rebuild, feature engine, Positioning
Poller, stage 5 respectively).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

TRADES_SCHEMA = pa.schema(
    [
        ("trade_id", pa.int64()),
        ("ts_exchange", pa.timestamp("us", tz="UTC")),
        ("ts_received", pa.timestamp("us", tz="UTC")),
        ("price", pa.float64()),
        ("qty", pa.float64()),
        ("is_buyer_maker", pa.bool_()),
        ("venue", pa.string()),
    ]
)

DEPTH_SCHEMA = pa.schema(
    [
        ("ts_exchange", pa.timestamp("us", tz="UTC")),
        ("ts_received", pa.timestamp("us", tz="UTC")),
        ("is_snapshot", pa.bool_()),
        ("bids", pa.string()),  # JSON-encoded [[price, qty], ...]
        ("asks", pa.string()),
        ("seq", pa.int64()),
    ]
)


def _utc_date(ts: datetime) -> date:
    return ts.astimezone(UTC).date()


@dataclass
class _Buffer:
    rows: list[tuple] = field(default_factory=list)


class ParquetWriter:
    """One instance per data root; buffers rows per (kind, symbol, date)
    and flushes to disk. A flushed day's file is append-only for this
    process's own run -- restart recovery (main.py) reconciles overlap."""

    def __init__(self, data_root: Path) -> None:
        self._root = data_root
        self._trades: dict[tuple[str, date], _Buffer] = {}
        self._depth: dict[tuple[str, date], _Buffer] = {}

    def write_trade(
        self,
        symbol: str,
        *,
        trade_id: int,
        ts_exchange: datetime,
        ts_received: datetime,
        price: float,
        qty: float,
        is_buyer_maker: bool,
        venue: str = "binance_spot",
    ) -> None:
        key = (symbol, _utc_date(ts_exchange))
        buf = self._trades.setdefault(key, _Buffer())
        buf.rows.append((trade_id, ts_exchange, ts_received, price, qty, is_buyer_maker, venue))

    def write_depth(
        self,
        symbol: str,
        *,
        ts_exchange: datetime,
        ts_received: datetime,
        is_snapshot: bool,
        bids_json: str,
        asks_json: str,
        seq: int,
    ) -> None:
        key = (symbol, _utc_date(ts_exchange))
        buf = self._depth.setdefault(key, _Buffer())
        buf.rows.append((ts_exchange, ts_received, is_snapshot, bids_json, asks_json, seq))

    def flush_trades(self, symbol: str, day: date) -> Path | None:
        return self._flush(self._trades, "trades", TRADES_SCHEMA, symbol, day)

    def flush_depth(self, symbol: str, day: date) -> Path | None:
        return self._flush(self._depth, "depth", DEPTH_SCHEMA, symbol, day)

    def flush_all(self) -> list[Path]:
        written = []
        for symbol, day in list(self._trades.keys()):
            path = self.flush_trades(symbol, day)
            if path:
                written.append(path)
        for symbol, day in list(self._depth.keys()):
            path = self.flush_depth(symbol, day)
            if path:
                written.append(path)
        return written

    def _flush(
        self,
        buffers: dict[tuple[str, date], _Buffer],
        kind: str,
        schema: pa.Schema,
        symbol: str,
        day: date,
    ) -> Path | None:
        key = (symbol, day)
        buf = buffers.get(key)
        if buf is None or not buf.rows:
            return None

        out_dir = self._root / kind / symbol
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{day.isoformat()}.parquet"

        columns = list(zip(*buf.rows, strict=True))
        arrays = [pa.array(col, type=field.type) for col, field in zip(columns, schema, strict=True)]
        new_table = pa.Table.from_arrays(arrays, schema=schema)

        if out_path.exists():
            existing = pq.read_table(out_path)
            new_table = pa.concat_tables([existing, new_table])

        pq.write_table(new_table, out_path)
        buf.rows.clear()
        return out_path
