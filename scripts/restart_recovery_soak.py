#!/usr/bin/env python3
"""Exercise the production Recorder process across a real stop and restart."""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import signal
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow.parquet as pq
import psycopg
from psycopg.rows import dict_row

from littledevil_recorder.positioning_recovery import recovery_capability
from littledevil_recorder.subscription_manager import DESIRED_SUBSCRIPTIONS_FILENAME

logger = logging.getLogger("restart_acceptance")
CHANNELS = (
    "trades", "depth", "open_interest", "funding", "mark_index", "basis", "liquidation",
)


def now() -> datetime:
    return datetime.now(UTC)


def iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def read_data_health(database_url: str) -> dict[str, dict]:
    with psycopg.connect(database_url, row_factory=dict_row, autocommit=True) as conn:
        rows = conn.execute(
            "SELECT channel, status, gap_started_at, last_message_at "
            "FROM data_health ORDER BY channel"
        ).fetchall()
    return {
        row["channel"]: {
            "status": row["status"],
            "gap_started_at": iso(row["gap_started_at"]),
            "last_message_at": iso(row["last_message_at"]),
        }
        for row in rows
    }


def parquet_snapshot(root: Path, restart_at: datetime | None = None) -> dict:
    per_channel = {}
    post_restart_live_rows = {}
    provenance = {}
    all_paths = []
    for channel in CHANNELS:
        paths = sorted(root.glob(f"{channel}/**/*.parquet"))
        rows = 0
        raw_bytes = 0
        newest = None
        newest_path = None
        live_rows = 0
        source_counts: dict[str, int] = {}
        recovered_trade_rows = 0
        trade_rows = []
        for path in paths:
            try:
                table = pq.ParquetFile(path).read()
            except Exception:
                # A Parquet part may be in the middle of a flush. The caller
                # retries snapshots until the independently readable rows exist.
                continue
            count = table.num_rows
            if count == 0:
                continue
            rows += count
            raw_bytes += path.stat().st_size
            all_paths.append(str(path))
            exchange_times = table["ts_exchange"].to_pylist()
            received_times = table["ts_received"].to_pylist() if "ts_received" in table.column_names else [None] * count
            local_newest = max(exchange_times)
            if newest is None or local_newest > newest:
                newest = local_newest
                newest_path = str(path)
            if restart_at is not None:
                live_rows += sum(
                    exchange is not None and received is not None
                    and exchange > restart_at and received > restart_at
                    for exchange, received in zip(exchange_times, received_times, strict=True)
                )
            if "source" in table.column_names:
                for source in table["source"].to_pylist():
                    if source is not None:
                        source_counts[source] = source_counts.get(source, 0) + 1
            if channel == "trades":
                trade_ids = table["trade_id"].to_pylist()
                trade_rows.extend(zip(exchange_times, received_times, trade_ids, strict=True))
        if channel == "trades" and restart_at is not None:
            recovered_trade_rows = sum(
                exchange is not None and received == exchange
                and exchange < restart_at
                for exchange, received, _trade_id in trade_rows
            )
        per_channel[channel] = {
            "rows": rows,
            "raw_bytes": raw_bytes,
            "parquet_files": len(paths),
            "newest_ts_exchange": iso(newest),
            "newest_partition_path": newest_path,
        }
        post_restart_live_rows[channel] = live_rows
        provenance[channel] = source_counts
    return {
        "per_channel": per_channel,
        "post_restart_live_rows": post_restart_live_rows,
        "source_counts": provenance,
        "recovered_trade_rows_by_ts_received_equals_ts_exchange": recovered_trade_rows,
        "exact_partition_paths": sorted(set(all_paths)),
    }


def desired_subscriptions(root: Path) -> dict:
    path = root / "_local" / DESIRED_SUBSCRIPTIONS_FILENAME
    payload = json.loads(path.read_text()) if path.exists() else {}
    return {
        "path": str(path),
        "desired_symbols": {
            channel: sorted(entry["symbol"] for entry in payload.get(channel, []))
            for channel in ("trades", "depth", "positioning", "liquidation")
        },
        "states": {
            channel: {entry["symbol"]: entry.get("status") for entry in payload.get(channel, [])}
            for channel in ("trades", "depth", "positioning", "liquidation")
        },
    }


def start_recorder(root: Path, log_path: Path, symbols: list[str]) -> tuple[subprocess.Popen, object]:
    env = os.environ.copy()
    env["LITTLEDEVIL_DATA_ROOT"] = str(root)
    for key in (
        "LITTLEDEVIL_TRADE_SYMBOLS", "LITTLEDEVIL_DEPTH_SYMBOLS",
        "LITTLEDEVIL_POSITIONING_SYMBOLS", "LITTLEDEVIL_LIQUIDATION_SYMBOLS",
    ):
        env[key] = ",".join(symbols)
    log_handle = log_path.open("w")
    proc = subprocess.Popen(
        [sys.executable, "-m", "littledevil_recorder.main"],
        cwd=Path(__file__).resolve().parents[1],
        env=env,
        stdout=log_handle,
        stderr=subprocess.STDOUT,
        text=True,
    )
    return proc, log_handle


def stop_recorder(
    proc: subprocess.Popen, log_handle, label: str, *, timeout_seconds: float = 90,
) -> tuple[datetime, datetime, int]:
    stop_requested_at = now()
    proc.send_signal(signal.SIGTERM)
    try:
        return_code = proc.wait(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        proc.kill()
        return_code = proc.wait(timeout=5)
    stopped_at = now()
    log_handle.close()
    logger.info(
        "%s stop requested at %s; process exited at %s with exit=%d",
        label, iso(stop_requested_at), iso(stopped_at), return_code,
    )
    return stop_requested_at, stopped_at, return_code


def parse_task_census(log_path: Path) -> dict[str, dict[str, int] | None]:
    text = log_path.read_text(errors="replace")
    result = {}
    for label, pattern in (
        ("before_shutdown", r"task census before shutdown: background=(\d+) stream_supervisor_tasks=(\d+)"),
        ("after_shutdown", r"task census after shutdown: background=(\d+) stream_supervisor_tasks=(\d+)"),
    ):
        matches = re.findall(pattern, text)
        result[label] = (
            {"background_tasks": int(matches[-1][0]), "stream_supervisor_tasks": int(matches[-1][1])}
            if matches else None
        )
    return result


def parse_recovery_logs(log_path: Path) -> dict:
    text = log_path.read_text(errors="replace")
    trades = {symbol: int(count) for symbol, count in re.findall(
        r"restart recovery: ([A-Z0-9]+) recovered (\d+) trades", text
    )}
    positioning_attempts = len(re.findall(r"recovering gap on positioning/", text))
    positioning_recovered = {}
    for label, pattern in (
        ("open_interest", r"recovered OI: (\d+) rows"),
        ("funding", r"recovered funding: (\d+) rows"),
        ("mark_index", r"recovered mark/index: (\d+) rows"),
        ("basis", r"recovered basis: (\d+) rows"),
    ):
        positioning_recovered[label] = sum(map(int, re.findall(pattern, text)))
    return {
        "trade_backfill_rows_by_symbol": trades,
        "trade_backfill_total_rows": sum(trades.values()),
        "positioning_gap_attempt_log_count": positioning_attempts,
        "positioning_recovered_rows_by_channel": positioning_recovered,
        "liquidation_unrecoverable_log_count": len(re.findall(
            r"gap on liquidation/.* is unrecoverable \(sampled stream\)", text
        )),
        "http_400_count": len(re.findall(r"HTTP/1\.1 400", text)),
        "http_429_count": len(re.findall(r"HTTP/1\.1 429", text)),
        "log_path": str(log_path),
    }


def stop_task_counts_are_clean(census: dict) -> bool:
    before = census.get("before_shutdown")
    after = census.get("after_shutdown")
    return bool(
        before and before["background_tasks"] >= 2 and before["stream_supervisor_tasks"] == 2
        and after and after["background_tasks"] == 0 and after["stream_supervisor_tasks"] == 0
    )


def wait_for_condition(proc: subprocess.Popen, predicate, *, timeout: float, description: str):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"Recorder exited early ({proc.returncode}) while waiting for {description}")
        last = predicate()
        if last:
            return last
        time.sleep(1)
    raise TimeoutError(f"timed out waiting for {description}; last observation={last!r}")


def run(args: argparse.Namespace) -> dict:
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is required and must point to the isolated acceptance database")
    root = Path(args.data_root).expanduser().resolve()
    if root.exists() and any(root.iterdir()):
        raise ValueError(f"restart data root must be empty: {root}")
    root.mkdir(parents=True, exist_ok=True)
    log_dir = root.parent / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    symbols = [value.strip().upper() for value in args.symbols.split(",") if value.strip()]

    report = {
        "proof": "3_actual_restart_recovery_lifecycle",
        "recorder_sha": os.getenv("RECORDER_SHA", "unrecorded"),
        "symbols": symbols,
        "data_root": str(root),
        "minimum_deliberate_gap_seconds": args.gap_seconds,
    }

    # First real production process.
    first_started_at = now()
    first_log = log_dir / "recorder_before_restart.log"
    first_proc, first_handle = start_recorder(root, first_log, symbols)
    logger.info("first Recorder pid=%d started at %s", first_proc.pid, iso(first_started_at))
    try:
        def pre_stop_observation():
            snapshot = parquet_snapshot(root)
            health = read_data_health(database_url)
            ready_channels = [
                health.get(f"binance_{channel}_{symbol}", {}).get("status") == "ok"
                for channel in ("trades", "depth") for symbol in symbols
            ]
            elapsed = (now() - first_started_at).total_seconds()
            has_market_rows = all(
                snapshot["per_channel"][channel]["rows"] > 0
                for channel in ("trades", "depth")
            )
            if elapsed >= args.first_run_min_seconds and has_market_rows and all(ready_channels):
                return {"snapshot": snapshot, "health": health, "elapsed": elapsed}
            return None

        pre_stop = wait_for_condition(
            first_proc, pre_stop_observation, timeout=args.first_run_timeout_seconds,
            description="independently persisted pre-stop trades/depth and healthy Data Health",
        )
        report["first_process"] = {
            "pid": first_proc.pid,
            "start_timestamp": iso(first_started_at),
            "independent_pre_stop_verification_timestamp": iso(now()),
            "actual_uptime_seconds_before_stop": pre_stop["elapsed"],
            "persisted_before_stop": pre_stop["snapshot"],
            "data_health_before_shutdown": pre_stop["health"],
            "desired_subscriptions_before_shutdown": desired_subscriptions(root),
            "log_path": str(first_log),
        }
        first_stop_requested, first_stop, first_rc = stop_recorder(first_proc, first_handle, "first Recorder")
        report["first_process"]["stop_requested_timestamp"] = iso(first_stop_requested)
        report["first_process"]["stop_timestamp"] = iso(first_stop)
        report["first_process"]["exit_code"] = first_rc
        report["first_process"]["task_census"] = parse_task_census(first_log)
        report["first_process"]["persisted_after_clean_stop"] = parquet_snapshot(root)
        health_after_stop = read_data_health(database_url)
        report["data_health_after_shutdown"] = health_after_stop
        report["process_alive_after_first_stop"] = first_proc.poll() is None

        gap_target = first_stop + timedelta(seconds=args.gap_seconds)
        while now() < gap_target:
            if first_proc.poll() is None:
                raise RuntimeError("first Recorder remained alive during deliberate gap")
            time.sleep(min(1.0, max(0.0, (gap_target - now()).total_seconds())))
        restart_at = now()
        gap_duration = (restart_at - first_stop).total_seconds()
        report["restart_timestamp"] = iso(restart_at)
        report["actual_gap_duration_seconds"] = gap_duration
        report["data_health_before_restart"] = read_data_health(database_url)
        report["subscriptions_requested_for_restart"] = desired_subscriptions(root)

        # Second production process, using the exact same persisted data root.
        second_log = log_dir / "recorder_after_restart.log"
        second_proc, second_handle = start_recorder(root, second_log, symbols)
        report["second_process"] = {
            "pid": second_proc.pid,
            "start_timestamp": iso(restart_at),
        }
        try:
            def post_restart_observation():
                snapshot = parquet_snapshot(root, restart_at)
                health = read_data_health(database_url)
                subscriptions = desired_subscriptions(root)
                desired_ok = subscriptions["desired_symbols"] == report["subscriptions_requested_for_restart"]["desired_symbols"]
                state_map = subscriptions["states"]
                streams_subscribed = all(
                    state_map[channel].get(symbol) == "subscribed"
                    for channel in ("trades", "depth") for symbol in symbols
                )
                live_market_data = all(
                    snapshot["post_restart_live_rows"][channel] > 0
                    for channel in ("trades", "depth")
                )
                health_ok = all(
                    health.get(f"binance_{channel}_{symbol}", {}).get("status") == "ok"
                    for channel in ("trades", "depth") for symbol in symbols
                )
                elapsed = (now() - restart_at).total_seconds()
                if elapsed >= args.second_run_min_seconds and desired_ok and streams_subscribed and live_market_data and health_ok:
                    return {
                        "snapshot": snapshot,
                        "health": health,
                        "subscriptions": subscriptions,
                        "elapsed": elapsed,
                    }
                return None

            post_restart = wait_for_condition(
                second_proc, post_restart_observation, timeout=args.second_run_timeout_seconds,
                description="restored desired subscriptions and independently persisted post-restart live trades/depth",
            )
            report["second_process"].update({
                "actual_uptime_before_verification_seconds": post_restart["elapsed"],
                "persisted_after_restart": post_restart["snapshot"],
                "data_health_after_recovery": post_restart["health"],
                "restored_subscriptions": post_restart["subscriptions"],
                "log_path": str(second_log),
            })
            second_stop_requested, second_stop, second_rc = stop_recorder(second_proc, second_handle, "restarted Recorder")
            report["second_process"]["stop_requested_timestamp"] = iso(second_stop_requested)
            report["second_process"]["stop_timestamp"] = iso(second_stop)
            report["second_process"]["exit_code"] = second_rc
            report["second_process"]["task_census"] = parse_task_census(second_log)
            report["second_process"]["process_alive_after_stop"] = second_proc.poll() is None
            report["final_persisted_snapshot"] = parquet_snapshot(root, restart_at)
            report["data_health_after_second_shutdown"] = read_data_health(database_url)
            report["restart_recovery_logs"] = parse_recovery_logs(second_log)
            report["restart_recovery_provenance"] = {
                "recovered_trade_rows_by_ts_received_equals_ts_exchange": report["final_persisted_snapshot"]["recovered_trade_rows_by_ts_received_equals_ts_exchange"],
                "positioning_parquet_source_counts": {
                    channel: report["final_persisted_snapshot"]["source_counts"][channel]
                    for channel in ("open_interest", "funding", "mark_index", "basis")
                },
            }
            liquidation_channels = [f"binance_liquidation_{symbol}" for symbol in symbols]
            report["liquidation_gap"] = {
                "capability": recovery_capability("liquidation"),
                "health_after_shutdown": {channel: health_after_stop.get(channel) for channel in liquidation_channels},
                "health_before_restart": {channel: report["data_health_before_restart"].get(channel) for channel in liquidation_channels},
                "health_after_restart_recovery": {channel: post_restart["health"].get(channel) for channel in liquidation_channels},
                "unrecoverable_log_count": report["restart_recovery_logs"]["liquidation_unrecoverable_log_count"],
            }
        finally:
            if second_proc.poll() is None:
                second_proc.send_signal(signal.SIGTERM)
                try:
                    second_proc.wait(timeout=90)
                except subprocess.TimeoutExpired:
                    second_proc.kill()
                    second_proc.wait(timeout=5)
            if not second_handle.closed:
                second_handle.close()
    finally:
        if first_proc.poll() is None:
            first_proc.send_signal(signal.SIGTERM)
            try:
                first_proc.wait(timeout=90)
            except subprocess.TimeoutExpired:
                first_proc.kill()
                first_proc.wait(timeout=5)
        if not first_handle.closed:
            first_handle.close()

    first = report.get("first_process", {})
    second = report.get("second_process", {})
    health_pre = first.get("data_health_before_shutdown", {})
    health_gap = report.get("data_health_before_restart", {})
    health_post = second.get("data_health_after_recovery", {})
    trade_recovery = report.get("restart_recovery_logs", {}).get("trade_backfill_total_rows", 0)
    position_attempts = report.get("restart_recovery_logs", {}).get("positioning_gap_attempt_log_count", 0)
    post_live = second.get("persisted_after_restart", {}).get("post_restart_live_rows", {})
    liquidation_gap = report.get("liquidation_gap", {})
    health_gap_ok = all(
        health_gap.get(f"binance_{channel}_{symbol}", {}).get("gap_started_at") is not None
        and health_gap.get(f"binance_{channel}_{symbol}", {}).get("status") in ("stale", "suspended")
        for channel in ("trades", "depth") for symbol in symbols
    )
    health_post_ok = all(
        health_post.get(f"binance_{channel}_{symbol}", {}).get("status") == "ok"
        for channel in ("trades", "depth", "positioning") for symbol in symbols
    )
    liquidations_explicit = (
        liquidation_gap.get("capability") == "unrecoverable"
        and liquidation_gap.get("unrecoverable_log_count", 0) > 0
        and all(value is not None for value in liquidation_gap.get("health_after_shutdown", {}).values())
    )
    report["acceptance_checks"] = {
        "pre_stop_market_data_independently_persisted": all(
            first.get("persisted_before_stop", {}).get("per_channel", {}).get(channel, {}).get("rows", 0) > 0
            for channel in ("trades", "depth")
        ),
        "pre_stop_trade_depth_health_ok": all(
            health_pre.get(f"binance_{channel}_{symbol}", {}).get("status") == "ok"
            for channel in ("trades", "depth") for symbol in symbols
        ),
        "clean_first_shutdown": first.get("exit_code") == 0 and first.get("process_alive_after_first_stop") is False,
        "data_health_gap_persisted_before_restart": health_gap_ok,
        "deliberate_gap_met": report.get("actual_gap_duration_seconds", 0) >= args.gap_seconds,
        "desired_subscriptions_restored": second.get("restored_subscriptions", {}).get("desired_symbols") == report.get("subscriptions_requested_for_restart", {}).get("desired_symbols") and all(
            second.get("restored_subscriptions", {}).get("states", {}).get(channel, {}).get(symbol) == "subscribed"
            for channel in ("trades", "depth") for symbol in symbols
        ),
        "trade_backfill_executed": trade_recovery > 0,
        "positioning_recovery_invoked": position_attempts > 0,
        "post_restart_live_trades_and_depth_persisted": all(post_live.get(channel, 0) > 0 for channel in ("trades", "depth")),
        "data_health_recovered_for_live_channels": health_post_ok,
        "liquidation_gap_explicitly_unrecoverable": liquidations_explicit,
        "no_stream_supervisor_or_background_tasks_left": (
            stop_task_counts_are_clean(first.get("task_census", {}))
            and stop_task_counts_are_clean(second.get("task_census", {}))
        ),
        "clean_second_shutdown": second.get("exit_code") == 0 and second.get("process_alive_after_stop") is False,
    }
    report["status"] = "PASS" if all(report["acceptance_checks"].values()) else "FAIL"
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", required=True)
    parser.add_argument("--output", default="artifacts/session_b_restart_recovery.json")
    parser.add_argument("--symbols", default="BTCUSDT,ETHUSDT")
    parser.add_argument("--first-run-min-seconds", type=float, default=65)
    parser.add_argument("--first-run-timeout-seconds", type=float, default=180)
    parser.add_argument("--gap-seconds", type=float, default=75)
    parser.add_argument("--second-run-min-seconds", type=float, default=65)
    parser.add_argument("--second-run-timeout-seconds", type=float, default=180)
    return parser.parse_args()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = parse_args()
    output = Path(args.output).expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    try:
        report = run(args)
    except Exception as exc:
        report = {
            "proof": "3_actual_restart_recovery_lifecycle",
            "recorder_sha": os.getenv("RECORDER_SHA", "unrecorded"),
            "status": "FAIL",
            "failure": repr(exc),
        }
        print(json.dumps(report, indent=2, sort_keys=True))
        output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
        logger.exception("restart acceptance failed")
        return 1
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps(report, indent=2, sort_keys=True))
    print(f"metrics_artifact={output}")
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
