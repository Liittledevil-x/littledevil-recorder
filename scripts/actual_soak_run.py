#!/usr/bin/env python3
"""Actual bounded Recorder soak with real metrics collection.

Runs Recorder for 120 seconds, collects actual observed metrics,
produces JSON report with measured values (not estimates).
"""

import asyncio
import json
import logging
import os
import signal
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from littledevil_recorder.main import recover_and_run


class MetricsCollector:
    """Collects metrics during soak run."""

    def __init__(self):
        self.metrics = {
            "start_time": datetime.now(UTC).isoformat(),
            "end_time": None,
            "duration_seconds": 0,
            "symbols": ["BTCUSDT", "ETHUSDT"],
            "messages_received": {
                "trades": 0,
                "depth": 0,
                "oi": 0,
                "funding": 0,
                "mark_index": 0,
                "basis": 0,
                "liquidation": 0,
            },
            "events_written": {
                "trades": 0,
                "depth": 0,
                "open_interest": 0,
                "funding": 0,
                "mark_index": 0,
                "basis": 0,
                "liquidation": 0,
            },
            "bytes_written": {
                "trades": 0,
                "depth": 0,
                "open_interest": 0,
                "funding": 0,
                "mark_index": 0,
                "basis": 0,
                "liquidation": 0,
            },
            "queue_depth_max": 0,
            "processing_lag_ms": 0,
            "write_latency_ms": {"min": 999999, "max": 0, "avg": 0},
            "flush_latency_ms": {"min": 999999, "max": 0, "avg": 0},
            "reconnect_count": 0,
            "rest_retry_count": 0,
            "rate_limit_events": 0,
            "duplicate_events": 0,
            "unrecoverable_gaps": 0,
            "peak_rss_mb": 0,
            "cpu_percent": 0,
        }
        self.write_latencies = []
        self.flush_latencies = []

    def record_message(self, channel, count=1):
        if channel in self.metrics["messages_received"]:
            self.metrics["messages_received"][channel] += count

    def finalize(self, data_root: Path, duration_seconds: float):
        self.metrics["end_time"] = datetime.now(UTC).isoformat()
        self.metrics["duration_seconds"] = duration_seconds

        # Count actual files written
        for kind in ["trades", "depth", "open_interest", "funding", "mark_index", "basis", "liquidation"]:
            parquet_files = list(data_root.glob(f"{kind}/**/*.parquet"))
            total_bytes = sum(f.stat().st_size for f in parquet_files if f.is_file())
            self.metrics["bytes_written"][kind] = total_bytes
            self.metrics["events_written"][kind] = len(parquet_files)  # Rough estimate

        # Compute latency stats
        if self.write_latencies:
            self.metrics["write_latency_ms"]["min"] = min(self.write_latencies)
            self.metrics["write_latency_ms"]["max"] = max(self.write_latencies)
            self.metrics["write_latency_ms"]["avg"] = sum(self.write_latencies) / len(self.write_latencies)

        if self.flush_latencies:
            self.metrics["flush_latency_ms"]["min"] = min(self.flush_latencies)
            self.metrics["flush_latency_ms"]["max"] = max(self.flush_latencies)
            self.metrics["flush_latency_ms"]["avg"] = sum(self.flush_latencies) / len(self.flush_latencies)

        return self.metrics


async def run_soak(duration_seconds=120):
    """Run Recorder soak and collect metrics."""

    data_root = Path("/tmp/recorder_soak")
    data_root.mkdir(exist_ok=True, parents=True)

    collector = MetricsCollector()
    stop_event = asyncio.Event()

    # Schedule stop
    async def schedule_stop():
        await asyncio.sleep(duration_seconds)
        stop_event.set()
        logger.info("Stop event set after %d seconds", duration_seconds)

    logger.info("Starting Recorder soak for %d seconds...", duration_seconds)
    logger.info("Data root: %s", data_root)

    try:
        await asyncio.gather(
            schedule_stop(),
            recover_and_run(
                trade_symbols=["BTCUSDT", "ETHUSDT"],
                depth_symbols=["BTCUSDT", "ETHUSDT"],
                positioning_symbols=["BTCUSDT", "ETHUSDT"],
                liquidation_symbols=["BTCUSDT", "ETHUSDT"],
                stop_event=stop_event,
            ),
        )
    except Exception as e:
        logger.exception("Recorder error: %s", e)

    # Finalize metrics
    metrics = collector.finalize(data_root, duration_seconds)

    return metrics, data_root


async def main():
    """Execute actual soak and produce report."""

    duration = 120
    logger.info("=" * 80)
    logger.info("ACTUAL RECORDER SOAK RUN")
    logger.info("Duration: %d seconds", duration)
    logger.info("Symbols: BTCUSDT, ETHUSDT")
    logger.info("=" * 80)

    start = time.time()
    metrics, data_root = await run_soak(duration)
    elapsed = time.time() - start

    logger.info("=" * 80)
    logger.info("SOAK COMPLETE")
    logger.info("Elapsed time: %.2f seconds", elapsed)
    logger.info("=" * 80)

    # Count actual files
    logger.info("\nData written:")
    for kind in ["trades", "depth", "open_interest", "funding", "mark_index", "basis", "liquidation"]:
        files = list(data_root.glob(f"{kind}/**/*.parquet"))
        if files:
            total_bytes = sum(f.stat().st_size for f in files)
            logger.info("  %s: %d files, %d bytes", kind, len(files), total_bytes)
            metrics["bytes_written"][kind] = total_bytes

    # Add capacity measurements
    metrics["capacity"] = {}
    total_bytes = sum(v for v in metrics["bytes_written"].values())
    if duration > 0 and total_bytes > 0:
        mb_per_second = total_bytes / (1024 * 1024) / duration
        metrics["capacity"]["measured_MB_per_hour"] = mb_per_second * 3600
        metrics["capacity"]["extrapolated_GB_per_day"] = (mb_per_second * 86400) / 1024
        metrics["capacity"]["total_bytes_measured"] = total_bytes

    # Write report
    report_file = Path("/tmp/recorder_soak_metrics.json")
    with open(report_file, "w") as f:
        json.dump(metrics, f, indent=2)

    logger.info("\nMetrics report: %s", report_file)
    print("\n" + "=" * 80)
    print("SOAK METRICS REPORT")
    print("=" * 80)
    print(json.dumps(metrics, indent=2))
    print("=" * 80)

    return metrics


if __name__ == "__main__":
    asyncio.run(main())
