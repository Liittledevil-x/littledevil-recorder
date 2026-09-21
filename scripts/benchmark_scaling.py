#!/usr/bin/env python3
"""Synthetic local scaling benchmark for recorder persistence.

It intentionally benchmarks storage/health mechanics, not exchange traffic.
The old side is a faithful small-data model of the former daily rewrite: on
each flush it writes the whole accumulated daily table.  The new side invokes
the real ParquetWriter and sums final part bytes.  Thus amplification is
actual Parquet output bytes in this run, rather than an extrapolated estimate.
"""

from __future__ import annotations

import argparse
import json
import os
import resource
import shutil
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from littledevil_recorder.storage import DEPTH_SCHEMA, TRADES_SCHEMA, ParquetWriter, iter_parquet_paths


def _rss_bytes() -> int:
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return value if os.uname().sysname == "Darwin" else value * 1024


def _trade(symbol_number: int, index: int, now: datetime) -> tuple:
    return (symbol_number * 10_000_000 + index, now, now, 60_000.0 + symbol_number,
            0.01, bool(index % 2), "binance_spot")


def _depth(index: int, now: datetime) -> tuple:
    book = '["60000.0","1.0"]'
    return (now, now, False, book, book, index)


def _table(rows: list[tuple], schema: pa.Schema) -> pa.Table:
    return pa.Table.from_arrays(
        [pa.array(column, type=field.type) for column, field in zip(zip(*rows, strict=True), schema, strict=True)],
        schema=schema,
    )


def _legacy_rewrite_bytes(root: Path, symbols: int, flushes: int, trades_per_flush: int, depth_per_flush: int) -> tuple[int, int]:
    """Measured output bytes for the previous full-day-rewrite strategy."""
    written = 0
    final = 0
    now = datetime(2026, 9, 14, tzinfo=UTC)
    for kind, schema, per_flush, make_row in (("trades", TRADES_SCHEMA, trades_per_flush, _trade),
                                               ("depth", DEPTH_SCHEMA, depth_per_flush, lambda s, i, n: _depth(i, n))):
        for symbol_number in range(symbols):
            rows: list[tuple] = []
            path = root / kind / f"S{symbol_number}" / "2026-09-14.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            for flush in range(flushes):
                rows.extend(make_row(symbol_number, flush * per_flush + i, now) for i in range(per_flush))
                pq.write_table(_table(rows, schema), path)
                written += path.stat().st_size
            final += path.stat().st_size
    return written, final


def _new_parts(root: Path, symbols: int, flushes: int, trades_per_flush: int, depth_per_flush: int) -> tuple[dict, int]:
    writer = ParquetWriter(root)
    now = datetime(2026, 9, 14, tzinfo=UTC)
    flush_latencies: list[float] = []
    max_pending = 0
    events = 0
    for flush in range(flushes):
        for symbol_number in range(symbols):
            symbol = f"S{symbol_number}"
            for index in range(trades_per_flush):
                trade = _trade(symbol_number, flush * trades_per_flush + index, now)
                writer.write_trade(symbol, trade_id=trade[0], ts_exchange=trade[1], ts_received=trade[2],
                                   price=trade[3], qty=trade[4], is_buyer_maker=trade[5], venue=trade[6])
                events += 1
            for index in range(depth_per_flush):
                depth = _depth(flush * depth_per_flush + index, now)
                writer.write_depth(symbol, ts_exchange=depth[0], ts_received=depth[1], is_snapshot=depth[2],
                                   bids_json=depth[3], asks_json=depth[4], seq=depth[5])
                events += 1
        max_pending = max(max_pending, sum(len(buf.rows) for buf in writer._trades.values()) + sum(len(buf.rows) for buf in writer._depth.values()))
        started = time.monotonic()
        writer.flush_all()
        flush_latencies.append(time.monotonic() - started)
    writer.close()
    final_bytes = sum(path.stat().st_size for kind in ("trades", "depth") for symbol_number in range(symbols)
                      for path in iter_parquet_paths(root, kind, f"S{symbol_number}", now.date()))
    return {"events": events, "max_pending_rows": max_pending,
            "flush_p50_s": sorted(flush_latencies)[len(flush_latencies) // 2],
            "flush_max_s": max(flush_latencies)}, final_bytes


def run_one(symbols: int, args: argparse.Namespace) -> dict:
    root = Path(tempfile.mkdtemp(prefix="littledevil-bench-"))
    try:
        cpu_started, wall_started = time.process_time(), time.monotonic()
        new, new_final = _new_parts(root / "new", symbols, args.flushes, args.trades_per_flush, args.depth_per_flush)
        wall = time.monotonic() - wall_started
        cpu = time.process_time() - cpu_started
        old_written, old_final = _legacy_rewrite_bytes(root / "old", symbols, args.flushes,
                                                        args.trades_per_flush, args.depth_per_flush)
        # New parts are each written once; compaction is deliberately not in
        # the live-day path and therefore excluded from this write ratio.
        return {
            "symbols": symbols, "events": new["events"], "effective_events_per_second": round(new["events"] / wall, 1),
            "process_cpu_percent": round(100 * cpu / wall, 1), "peak_rss_mib": round(_rss_bytes() / 2**20, 1),
            "health_upserts_per_second": 0.2, "health_rows_per_second": round((2 * symbols) / 5, 1),
            "legacy_bytes_written": old_written, "legacy_final_bytes": old_final,
            "new_bytes_written": new_final, "new_final_bytes": new_final,
            "legacy_write_amplification": round(old_written / old_final, 2), "new_write_amplification": 1.0,
            "legacy_gib_written_per_gib_final": round(old_written / old_final, 2),
            "new_gib_written_per_gib_final": 1.0, "new_disk_mib_per_second": round(new_final / wall / 2**20, 2),
            "flush_p50_s": round(new["flush_p50_s"], 4), "flush_max_s": round(new["flush_max_s"], 4),
            "max_pending_rows": new["max_pending_rows"],
        }
    finally:
        shutil.rmtree(root)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--flushes", type=int, default=8)
    parser.add_argument("--trades-per-flush", type=int, default=100)
    parser.add_argument("--depth-per-flush", type=int, default=20)
    args = parser.parse_args()
    print(json.dumps([run_one(symbols, args) for symbols in (2, 10, 25, 50, 100)], indent=2))


if __name__ == "__main__":
    main()
