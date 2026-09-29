#!/usr/bin/env python3
"""Run the canonical Recorder lifecycle for a measured bounded soak."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import tempfile
import time
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit

import aiohttp
import httpx
import psutil
import pyarrow.parquet as pq

from littledevil_recorder import main as recorder_main
from littledevil_recorder.db import connect


PHYSICAL_CHANNELS = (
    "trades", "depth", "open_interest", "funding", "mark_index",
    "basis", "liquidation",
)
POLL_URL_CHANNELS = {
    "/fapi/v1/openInterest": "open_interest",
    "/fapi/v1/premiumIndex": "mark_index",
    "/fapi/v1/fundingRate": "funding",
    "/futures/data/basis": "basis",
}


def utc_now() -> datetime:
    return datetime.now(UTC)


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, round((len(ordered) - 1) * fraction))]


def latency_summary(values: list[float]) -> dict[str, float | int | None]:
    return {
        "samples": len(values),
        "mean_ms": sum(values) / len(values) if values else None,
        "p50_ms": percentile(values, 0.50),
        "p95_ms": percentile(values, 0.95),
        "max_ms": max(values) if values else None,
    }


class Metrics:
    def __init__(self, symbols: list[str]) -> None:
        self.symbols = symbols
        self.messages = defaultdict(int)
        self.normalized = defaultdict(int)
        self.duplicates = 0
        self.processing_errors = 0
        self.rest_attempts = 0
        self.rest_retries = 0
        self.http_429 = 0
        self.rest_messages = defaultdict(int)
        self.reconnects = 0
        self._failed_rest: dict[str, float] = {}
        self._trade_ids: dict[str, set[int]] = defaultdict(set)
        self.write_ms: list[float] = []
        self.flush_ms: list[float] = []
        self.lag_ms: dict[str, list[float]] = defaultdict(list)
        self.peak_rss_mb = 0.0
        self.cpu_samples: list[float] = []
        self.process = psutil.Process(os.getpid())

    def received(self, channel: str, count: int = 1) -> None:
        self.messages[channel] += count

    def rest_request(self, url: str, *, status: int | None, failed: bool = False) -> None:
        self.rest_attempts += 1
        key = url
        now = time.monotonic()
        previous_failure = self._failed_rest.get(key)
        if previous_failure is not None and now - previous_failure <= 15:
            self.rest_retries += 1
        if status == 429:
            self.http_429 += 1
        if failed or (status is not None and (status == 429 or status >= 500)):
            self._failed_rest[key] = now
        else:
            self._failed_rest.pop(key, None)
        if status is not None and 200 <= status < 300:
            path = urlsplit(url).path
            for endpoint, channel in POLL_URL_CHANNELS.items():
                if path.endswith(endpoint):
                    self.rest_messages[channel] += 1
                    self.received(channel)
                    break

    def parquet_counts(self, root: Path) -> tuple[dict[str, dict[str, int]], dict[str, int]]:
        by_channel: dict[str, dict[str, int]] = {}
        rows_by_channel: dict[str, int] = {}
        for channel in PHYSICAL_CHANNELS:
            files = sorted(root.glob(f"{channel}/**/*.parquet"))
            row_count = 0
            byte_count = 0
            for path in files:
                if not path.is_file():
                    continue
                byte_count += path.stat().st_size
                row_count += pq.ParquetFile(path).metadata.num_rows
            by_channel[channel] = {
                "rows": row_count,
                "raw_bytes": byte_count,
                "parquet_files": len(files),
            }
            rows_by_channel[channel] = row_count
        return by_channel, rows_by_channel


class ObservedWriter:
    """Count accepted writer inputs and time the real synchronous writes."""

    _METHODS = {
        "write_trade": "trades",
        "write_depth": "depth",
        "write_open_interest": "open_interest",
        "write_funding": "funding",
        "write_mark_index": "mark_index",
        "write_basis": "basis",
        "write_liquidation": "liquidation",
    }

    def __init__(self, root: Path, metrics: Metrics) -> None:
        self._inner = _ORIGINAL_WRITER(root)
        self._metrics = metrics

    def __getattr__(self, name: str):
        return getattr(self._inner, name)

    def _write(self, method: str, channel: str, *args, **kwargs):
        started = time.perf_counter()
        received_at = kwargs.get("ts_received")
        if channel == "trades":
            self._metrics.normalized[channel] += 1
            symbol = args[0] if args else kwargs.get("symbol")
            trade_id = kwargs.get("trade_id")
            if trade_id in self._metrics._trade_ids[symbol]:
                self._metrics.duplicates += 1
            else:
                self._metrics._trade_ids[symbol].add(trade_id)
        try:
            result = getattr(self._inner, method)(*args, **kwargs)
            if channel != "trades":
                self._metrics.normalized[channel] += 1
            return result
        except Exception:
            self._metrics.processing_errors += 1
            raise
        finally:
            self._metrics.write_ms.append((time.perf_counter() - started) * 1000)
            if isinstance(received_at, datetime):
                self._metrics.lag_ms[channel].append(
                    max(0.0, (utc_now() - received_at).total_seconds() * 1000)
                )

    def flush_all(self):
        started = time.perf_counter()
        try:
            return self._inner.flush_all()
        finally:
            self._metrics.flush_ms.append((time.perf_counter() - started) * 1000)


_ORIGINAL_WRITER = recorder_main.ParquetWriter


class ReconnectLogCounter(logging.Handler):
    def __init__(self, metrics: Metrics) -> None:
        super().__init__(logging.WARNING)
        self.metrics = metrics

    def emit(self, record: logging.LogRecord) -> None:
        message = record.getMessage().lower()
        if "stream dropped" in message and "reconnect" in message:
            self.metrics.reconnects += 1
        if "stream task failed" in message:
            self.metrics.processing_errors += 1


class RestObserver:
    def __init__(self, metrics: Metrics) -> None:
        self.metrics = metrics
        self._httpx_get = httpx.AsyncClient.get
        self._aiohttp_request = aiohttp.ClientSession._request

    def install(self) -> None:
        metrics = self.metrics
        original_httpx_get = self._httpx_get
        original_aiohttp_request = self._aiohttp_request

        async def observed_httpx_get(client, url, *args, **kwargs):
            url_text = str(url)
            try:
                response = await original_httpx_get(client, url, *args, **kwargs)
            except Exception:
                metrics.rest_request(url_text, status=None, failed=True)
                raise
            metrics.rest_request(url_text, status=response.status_code)
            return response

        async def observed_aiohttp_request(session, method, url, *args, **kwargs):
            url_text = str(url)
            try:
                response = await original_aiohttp_request(session, method, url, *args, **kwargs)
            except Exception:
                metrics.rest_request(url_text, status=None, failed=True)
                raise
            metrics.rest_request(url_text, status=response.status)
            return response

        httpx.AsyncClient.get = observed_httpx_get
        aiohttp.ClientSession._request = observed_aiohttp_request

    def restore(self) -> None:
        httpx.AsyncClient.get = self._httpx_get
        aiohttp.ClientSession._request = self._aiohttp_request


async def read_health(symbols: list[str]) -> dict[str, dict[str, str | None]]:
    channels = [
        f"binance_{kind}_{symbol}"
        for symbol in symbols
        for kind in ("trades", "depth", "positioning", "liquidation")
    ]
    conn = await connect()
    try:
        async with conn.cursor() as cursor:
            await cursor.execute(
                "SELECT channel, status, gap_started_at, last_message_at FROM data_health WHERE channel = ANY(%s)",
                (channels,),
            )
            rows = await cursor.fetchall()
        return {
            row["channel"]: {
                "status": row["status"],
                "gap_started_at": row["gap_started_at"].isoformat() if row["gap_started_at"] else None,
                "last_message_at": row["last_message_at"].isoformat() if row["last_message_at"] else None,
            }
            for row in rows
        }
    finally:
        await conn.close()


async def sample_process(metrics: Metrics, stop_event: asyncio.Event) -> None:
    metrics.process.cpu_percent(interval=None)
    while not stop_event.is_set():
        try:
            metrics.peak_rss_mb = max(
                metrics.peak_rss_mb,
                metrics.process.memory_info().rss / (1024 * 1024),
            )
            metrics.cpu_samples.append(metrics.process.cpu_percent(interval=None))
        except psutil.Error:
            break
        await asyncio.sleep(1)


def capacity_entry(rows: int, raw_bytes: int, elapsed_seconds: float) -> dict:
    if rows == 0 or raw_bytes == 0 or elapsed_seconds <= 0:
        return {
            "status": "INSUFFICIENT SAMPLE",
            "observed_rows": rows,
            "raw_bytes": raw_bytes,
            "bytes_per_event": None,
            "measured_MB_per_hour": None,
            "extrapolated_GB_per_day": None,
        }
    return {
        "status": "SAMPLED",
        "observed_rows": rows,
        "raw_bytes": raw_bytes,
        "bytes_per_event": {
            "value": raw_bytes / rows,
            "label": "MEASURED",
        },
        "measured_MB_per_hour": {
            "value": raw_bytes / (1024**2) * 3600 / elapsed_seconds,
            "label": "MEASURED",
        },
        "extrapolated_GB_per_day": {
            "value": raw_bytes / (1024**3) * 86400 / elapsed_seconds,
            "label": "EXTRAPOLATED",
        },
    }


async def run_soak(args: argparse.Namespace) -> dict:
    symbols = [symbol.strip().upper() for symbol in args.symbols.split(",") if symbol.strip()]
    root = Path(args.data_root).expanduser().resolve() if args.data_root else Path(
        tempfile.mkdtemp(prefix="littledevil-recorder-soak-")
    )
    if root.exists() and any(root.iterdir()):
        raise ValueError(f"data root must be empty for a bounded measurement: {root}")
    root.mkdir(parents=True, exist_ok=True)
    os.environ["LITTLEDEVIL_DATA_ROOT"] = str(root)

    metrics = Metrics(symbols)
    stop_event = asyncio.Event()
    reconnect_counter = ReconnectLogCounter(metrics)
    root_logger = logging.getLogger()
    root_logger.addHandler(reconnect_counter)
    rest_observer = RestObserver(metrics)
    rest_observer.install()

    original_aggtrade = recorder_main.run_aggtrade_stream
    original_depth = recorder_main.run_depth_stream
    original_liquidation = recorder_main.run_liquidation_stream

    async def observed_aggtrade(stream_symbols, on_trade, *, stop_event=None):
        async def on_observed_trade(trade):
            metrics.received("trades")
            try:
                await on_trade(trade)
            except Exception:
                metrics.processing_errors += 1
                raise
        return await original_aggtrade(stream_symbols, on_observed_trade, stop_event=stop_event)

    async def observed_depth(stream_symbols, on_event, *, stop_event=None):
        async def on_observed_depth(symbol, book, event):
            metrics.received("depth")
            try:
                await on_event(symbol, book, event)
            except Exception:
                metrics.processing_errors += 1
                raise
        return await original_depth(stream_symbols, on_observed_depth, stop_event=stop_event)

    async def observed_liquidations(on_liquidation, *, stop_event=None):
        async def on_observed_liquidation(event):
            metrics.received("liquidation")
            metrics.normalized["liquidation"] += 1
            try:
                await on_liquidation(event)
            except Exception:
                metrics.processing_errors += 1
                raise
        return await original_liquidation(on_observed_liquidation, stop_event=stop_event)

    recorder_main.run_aggtrade_stream = observed_aggtrade
    recorder_main.run_depth_stream = observed_depth
    recorder_main.run_liquidation_stream = observed_liquidations
    recorder_main.ParquetWriter = lambda data_root: ObservedWriter(data_root, metrics)

    start_at = utc_now()

    async def stop_after_duration() -> None:
        await asyncio.sleep(args.duration_seconds)
        stop_event.set()

    sampler = asyncio.create_task(sample_process(metrics, stop_event))
    failure = None
    try:
        await asyncio.gather(
            stop_after_duration(),
            recorder_main.recover_and_run(
                trade_symbols=symbols,
                depth_symbols=symbols,
                positioning_symbols=symbols,
                liquidation_symbols=symbols,
                stop_event=stop_event,
            ),
        )
    except Exception as exc:
        failure = repr(exc)
        stop_event.set()
    finally:
        await sampler
        recorder_main.run_aggtrade_stream = original_aggtrade
        recorder_main.run_depth_stream = original_depth
        recorder_main.run_liquidation_stream = original_liquidation
        recorder_main.ParquetWriter = _ORIGINAL_WRITER
        rest_observer.restore()
        root_logger.removeHandler(reconnect_counter)

    end_at = utc_now()
    elapsed = (end_at - start_at).total_seconds()
    data, physical_rows = metrics.parquet_counts(root)
    health = await read_health(symbols)
    unrecoverable_gaps = sum(
        1 for channel, value in health.items()
        if "_liquidation_" in channel and value["gap_started_at"] is not None
    )

    for channel in PHYSICAL_CHANNELS:
        if channel != "liquidation":
            metrics.normalized.setdefault(channel, 0)
        metrics.messages.setdefault(channel, 0)
    persisted = {channel: data[channel]["rows"] for channel in PHYSICAL_CHANNELS}
    capacity = {
        channel: capacity_entry(
            data[channel]["rows"], data[channel]["raw_bytes"], elapsed
        )
        for channel in PHYSICAL_CHANNELS
    }
    # Mark and index are two fields in the same persisted mark_index row.
    for field in ("mark", "index"):
        capacity[field] = {
            **capacity_entry(
                data["mark_index"]["rows"], data["mark_index"]["raw_bytes"], elapsed
            ),
            "storage_shared_with": "mark_index",
            "physical_bytes_counted_once": True,
        }

    all_files = sum(value["parquet_files"] for value in data.values())
    report = {
        "proof": "1_scale_backpressure_and_2_capacity",
        "recorder_sha": os.getenv("RECORDER_SHA", "unrecorded"),
        "requested_duration_seconds": args.duration_seconds,
        "start_timestamp": start_at.isoformat(),
        "end_timestamp": end_at.isoformat(),
        "actual_duration_seconds": elapsed,
        "symbols": symbols,
        "data_root": str(root),
        "messages_received_per_channel": dict(metrics.messages),
        "events_normalized_per_channel": dict(metrics.normalized),
        "events_persisted_per_channel": persisted,
        "queue_depth": {
            "current": 0,
            "maximum": 0,
            "basis": "No Recorder-owned event queue; stream callbacks are awaited inline.",
        },
        "processing_lag_ms": {
            channel: latency_summary(values) for channel, values in metrics.lag_ms.items()
        },
        "write_latency_ms": latency_summary(metrics.write_ms),
        "flush_latency_ms": latency_summary(metrics.flush_ms),
        "reconnect_count": metrics.reconnects,
        "rest_request_attempts": metrics.rest_attempts,
        "rest_retry_count": metrics.rest_retries,
        "http_429_count": metrics.http_429,
        "duplicate_or_rejected_event_count": metrics.duplicates,
        "unrecoverable_gap_count": unrecoverable_gaps,
        "data_health": health,
        "peak_rss_mb": metrics.peak_rss_mb,
        "cpu_percent_mean": sum(metrics.cpu_samples) / len(metrics.cpu_samples) if metrics.cpu_samples else None,
        "cpu_percent_peak": max(metrics.cpu_samples) if metrics.cpu_samples else None,
        "raw_bytes_written_per_channel": {
            channel: data[channel]["raw_bytes"] for channel in PHYSICAL_CHANNELS
        },
        "compacted_bytes_per_channel": {channel: 0 for channel in PHYSICAL_CHANNELS},
        "compaction_occurred": False,
        "parquet_files_created": all_files,
        "parquet_rows_and_bytes_per_channel": data,
        "capacity": capacity,
        "measurement_failure": failure,
        "status": "PASS" if elapsed >= args.duration_seconds and failure is None else "FAIL",
    }
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration-seconds", type=float, default=120.0)
    parser.add_argument("--symbols", default="BTCUSDT,ETHUSDT")
    parser.add_argument("--data-root")
    parser.add_argument("--output", default="artifacts/session_b_scale_capacity.json")
    return parser.parse_args()


async def main() -> int:
    args = parse_args()
    report = await run_soak(args)
    output = Path(args.output).expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps(report, indent=2, sort_keys=True))
    print(f"metrics_artifact={output}")
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    raise SystemExit(asyncio.run(main()))
