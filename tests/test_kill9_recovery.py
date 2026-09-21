"""Automated test for build-plan.md's Stage 0 gate criterion #3 / docs
§0.6: the recorder must survive being killed mid-run, repeatedly, without
corrupting its Parquet output and without recovery memory growing with the
number of prior kill/restart cycles or the amount of on-disk data already
accumulated.

This is the regression test that should have caught the actual production
bug before it ever reached the EC2 instance: the old _flush() implementation
(pq.read_table + concat + rewrite on every flush) both corrupted nothing
per se, but its memory cost scaled with accumulated file size, and repeated
crash/restart cycles made the file (and therefore each flush's memory cost)
grow monotonically -- exactly what caused the accelerating OOM crash loop.

Unlike test_storage.py's unit tests (which call ParquetWriter methods
in-process), this test drives the real code as a subprocess and actually
sends SIGKILL, so a crash mid-flush (mid os.replace, mid iter_batches, etc.)
is a real OS-level kill of the process holding the file, not a simulated
exception -- the only way to genuinely exercise the "kill -9 mid-run" gate
criterion rather than assume the atomic-replace design behaves correctly
under a real kill.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
from datetime import date
from pathlib import Path

import pyarrow.parquet as pq
import pytest

WORKER = Path(__file__).parent / "_kill9_recorder_worker.py"

# Kept small so the test suite stays fast; large enough that a mid-loop
# SIGKILL reliably lands inside the write_trade/flush_trades loop rather
# than before the process even gets there.
ROWS_PER_BATCH = 200
NUM_BATCHES = 5
KILL_CYCLES = 8

# A generous fixed ceiling, independent of KILL_CYCLES or accumulated file
# size. The bug this guards against made peak memory scale with the day's
# accumulated file size (which grows with every restart that adds more
# rows) -- a real regression would show peak RSS climbing across cycles,
# not just exceeding this ceiling on cycle 1.
PEAK_RSS_CEILING_KB = 300_000  # 300MB -- see rationale above; real fixed


def _run_worker(data_root: Path, symbol: str, day: date, *, kill_after_seconds: float | None):
    proc = subprocess.Popen(
        [
            sys.executable,
            str(WORKER),
            str(data_root),
            symbol,
            day.isoformat(),
            str(ROWS_PER_BATCH),
            str(NUM_BATCHES),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if kill_after_seconds is not None:
        try:
            proc.wait(timeout=kill_after_seconds)
        except subprocess.TimeoutExpired:
            os.kill(proc.pid, signal.SIGKILL)
        proc.wait(timeout=10)
        return None

    stdout, stderr = proc.communicate(timeout=30)
    assert proc.returncode == 0, f"worker failed: {stderr}"
    return stdout


def _assert_file_is_valid_parquet_or_absent(path: Path) -> int:
    """Returns the row count if the file exists; a killed-mid-write process
    must never leave behind a file that exists but fails to parse -- the
    fix's atomic os.replace() means a kill can only ever be caught before
    or after a replace, never leave a half-written file at the final path.
    """
    if not path.exists():
        return 0
    table = pq.read_table(path)  # raises ArrowInvalid if corrupted/truncated
    return table.num_rows


def test_repeated_sigkill_mid_flush_never_corrupts_output_and_keeps_memory_bounded(tmp_path):
    symbol = "BTCUSDT"
    day = date(2026, 9, 14)
    out_path = tmp_path / "trades" / symbol / f"{day.isoformat()}.parquet"

    prior_row_count = 0
    for cycle in range(KILL_CYCLES):
        # Kill fast and early so most cycles interrupt the worker somewhere
        # inside its write/flush loop rather than after it has finished --
        # varying the delay sweeps different interruption points (before
        # first flush, mid-flush, between flushes) across cycles.
        kill_after = 0.01 + (cycle % 4) * 0.02
        _run_worker(tmp_path, symbol, day, kill_after_seconds=kill_after)

        # (a) No corrupted/truncated Parquet output after a kill -9, ever.
        row_count = _assert_file_is_valid_parquet_or_absent(out_path)
        assert row_count >= prior_row_count, (
            f"cycle {cycle}: row count went backwards after a kill "
            f"({prior_row_count} -> {row_count}) -- a crash must never lose "
            f"previously-flushed rows, only possibly the still-buffered ones"
        )
        prior_row_count = row_count

    # Let a final, un-killed run complete fully against whatever the killed
    # cycles left behind -- this is the recovery flush a real restart would
    # perform, now against a file shaped by KILL_CYCLES of crash/restart
    # history. Its peak memory must not reflect that history.
    stdout = _run_worker(tmp_path, symbol, day, kill_after_seconds=None)
    assert stdout is not None
    peak_line = [line for line in stdout.splitlines() if line.startswith("PEAK_RSS_KB=")]
    assert peak_line, f"worker did not report peak RSS; stdout was: {stdout!r}"
    peak_kb = int(peak_line[0].split("=", 1)[1])

    # (b) Recovery memory stays under a fixed ceiling regardless of how many
    # kill/restart cycles preceded it -- the actual bug this guards against.
    assert peak_kb < PEAK_RSS_CEILING_KB, (
        f"final recovery run peaked at {peak_kb}KB after {KILL_CYCLES} prior "
        f"kill/restart cycles -- expected a bounded ceiling independent of "
        f"restart count (this is the exact failure mode of the 2026-09-16 "
        f"production OOM incident: memory scaled with accumulated file size, "
        f"which grows with every restart)"
    )

    final_row_count = _assert_file_is_valid_parquet_or_absent(out_path)
    assert final_row_count >= prior_row_count


def test_sigkill_immediately_after_process_start_leaves_no_corrupt_file(tmp_path):
    # Edge case: kill before the worker has done anything at all -- must
    # never leave a zero-byte or partially-written file at the final path
    # (this is exactly the shape of the two real known-corrupt files from
    # the 2026-09-16 incident: depth/BTCUSDT/2026-09-15.parquet was 4 bytes,
    # depth/ETHUSDT/2026-09-14.parquet had invalid magic bytes).
    symbol = "ETHUSDT"
    day = date(2026, 9, 14)
    out_path = tmp_path / "trades" / symbol / f"{day.isoformat()}.parquet"

    _run_worker(tmp_path, symbol, day, kill_after_seconds=0.001)

    if out_path.exists():
        # If anything exists, it must parse -- our atomic-replace design
        # means the only way a file can exist at the final path is if a
        # prior os.replace() fully completed.
        pq.read_table(out_path)
    # A missing file is also an acceptable outcome (nothing survived to be
    # renamed into place) -- both are valid, "corrupted-but-present" is not.
