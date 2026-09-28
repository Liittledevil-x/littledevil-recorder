#!/usr/bin/env python3
"""Run a bounded soak test of the Recorder against real/simulated Binance.

Usage:
    LITTLEDEVIL_DATA_ROOT=/tmp/soak_test \
    LITTLEDEVIL_TRADE_SYMBOLS=BTCUSDT,ETHUSDT \
    LITTLEDEVIL_DEPTH_SYMBOLS=BTCUSDT,ETHUSDT \
    LITTLEDEVIL_POSITIONING_SYMBOLS=BTCUSDT,ETHUSDT \
    LITTLEDEVIL_LIQUIDATION_SYMBOLS=BTCUSDT,ETHUSDT \
    python scripts/run_soak_test.py --duration 60 --output /tmp/soak_metrics.json

This connects to real Binance public endpoints for the duration, collects
metrics, and outputs a JSON report.
"""

import argparse
import asyncio
import json
import logging
import os
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main():
    """Run bounded soak test."""
    parser = argparse.ArgumentParser(description="Run Recorder soak test")
    parser.add_argument("--duration", type=int, default=300, help="Duration in seconds")
    parser.add_argument("--output", type=Path, default=Path("soak_metrics.json"), help="Output JSON file")
    args = parser.parse_args()

    # Import here so we can run from repo root
    sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

    from littledevil_recorder.main import recover_and_run
    from littledevil_recorder.soak_instrumentation import SoakInstrument

    # Env-var symbols
    trade_symbols_env = os.getenv("LITTLEDEVIL_TRADE_SYMBOLS", "BTCUSDT,ETHUSDT")
    depth_symbols_env = os.getenv("LITTLEDEVIL_DEPTH_SYMBOLS", "BTCUSDT,ETHUSDT")
    positioning_symbols_env = os.getenv("LITTLEDEVIL_POSITIONING_SYMBOLS", "BTCUSDT,ETHUSDT")
    liquidation_symbols_env = os.getenv("LITTLEDEVIL_LIQUIDATION_SYMBOLS", "BTCUSDT,ETHUSDT")

    trade_symbols = [s.strip() for s in trade_symbols_env.split(",") if s.strip()]
    depth_symbols = [s.strip() for s in depth_symbols_env.split(",") if s.strip()]
    positioning_symbols = [s.strip() for s in positioning_symbols_env.split(",") if s.strip()]
    liquidation_symbols = [s.strip() for s in liquidation_symbols_env.split(",") if s.strip()]

    logger.info(
        "starting soak test: duration=%ds, trade_symbols=%s, depth_symbols=%s",
        args.duration,
        trade_symbols,
        depth_symbols,
    )

    instrument = SoakInstrument()
    stop_event = asyncio.Event()

    # Schedule stop event after duration
    async def schedule_stop():
        await asyncio.sleep(args.duration)
        stop_event.set()

    try:
        await asyncio.gather(
            schedule_stop(),
            recover_and_run(
                trade_symbols,
                depth_symbols,
                positioning_symbols,
                liquidation_symbols,
                stop_event=stop_event,
            ),
        )
    except Exception as exc:
        logger.exception("soak test failed: %s", exc)
        sys.exit(1)

    # Finalize metrics
    metrics = instrument.finalize_metrics(args.duration)

    # Write JSON report
    report = {
        "duration_seconds": metrics.duration_seconds,
        "start_time": metrics.start_time,
        "end_time": metrics.end_time,
        "channels": {
            "trades": {
                "messages_received": metrics.trades_messages_received,
            },
            "depth": {
                "messages_received": metrics.depth_messages_received,
            },
            "positioning": {
                "oi_polls": metrics.oi_polls_completed,
                "funding_polls": metrics.funding_polls_completed,
                "mark_index_polls": metrics.mark_index_polls_completed,
                "basis_polls": metrics.basis_polls_completed,
            },
            "liquidation": {
                "events_received": metrics.liquidation_events_received,
            },
        },
        "quality": {
            "duplicate_trades": metrics.duplicate_trades,
            "duplicate_liquidations": metrics.duplicate_liquidations,
            "processing_errors": metrics.processing_errors,
            "rest_errors": metrics.rest_errors,
            "websocket_reconnects": metrics.websocket_reconnects,
        },
        "latency_ms": {
            "avg_write": metrics.avg_write_latency_ms,
            "max_write": metrics.max_write_latency_ms,
            "avg_flush": metrics.avg_flush_latency_ms,
            "max_flush": metrics.max_flush_latency_ms,
        },
        "memory": {
            "peak_mb": metrics.memory_peak_mb,
        },
        "storage": {
            "written_bytes": metrics.storage_written_bytes,
            "projected_mb_per_hour": (metrics.storage_written_bytes / (1024 * 1024) / args.duration) * 3600
            if args.duration > 0
            else 0,
            "projected_gb_per_day": metrics.projected_gb_per_day,
        },
        "extrapolations": {
            "events_per_hour": metrics.events_per_hour,
        },
    }

    with open(args.output, "w") as f:
        json.dump(report, f, indent=2)

    logger.info("soak metrics written to %s", args.output)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
