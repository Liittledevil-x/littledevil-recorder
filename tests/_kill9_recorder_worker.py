"""Standalone worker process for test_kill9_recovery.py.

Simulates one "life" of the recorder: on start, it writes a batch of trade
rows for a single (symbol, day) key and flushes them via the real
ParquetWriter/_flush code path (storage.py) -- the exact code that produced
the 2026-09-16 OOM incident and the exact code fixed to bound flush memory.
It is invoked as a subprocess so the driving test can SIGKILL it mid-flush
loop, the same way systemd's Restart=on-failure or an OOM killer would end
the real process, and then start a fresh one against the same data_root to
simulate the next restart.

Args: <data_root> <symbol> <day-iso> <rows_per_batch> <num_batches>

Writes its own peak RSS (via resource.getrusage) to stdout as the last line
of output, in kilobytes, so the driving test can assert it stays bounded
across many restarts without needing a separate profiler subprocess.
"""

from __future__ import annotations

import resource
import sys
from datetime import UTC, date, datetime, timedelta

from littledevil_recorder.storage import ParquetWriter


def main() -> None:
    data_root, symbol, day_iso, rows_per_batch, num_batches = sys.argv[1:6]
    day = date.fromisoformat(day_iso)
    rows_per_batch = int(rows_per_batch)
    num_batches = int(num_batches)

    writer = ParquetWriter(__import__("pathlib").Path(data_root))

    base_trade_id = int(datetime.now(UTC).timestamp() * 1000)
    for batch_idx in range(num_batches):
        for i in range(rows_per_batch):
            trade_id = base_trade_id + batch_idx * rows_per_batch + i
            ts = datetime(day.year, day.month, day.day, tzinfo=UTC) + timedelta(
                seconds=trade_id % 86400
            )
            writer.write_trade(
                symbol,
                trade_id=trade_id,
                ts_exchange=ts,
                ts_received=ts,
                price=100.0 + i,
                qty=1.0,
                is_buyer_maker=False,
            )
        writer.flush_trades(symbol, day)

    writer.close()

    peak_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # On Linux ru_maxrss is already KB; on macOS (Darwin) it's bytes.
    if sys.platform == "darwin":
        peak_kb //= 1024
    print(f"PEAK_RSS_KB={peak_kb}")


if __name__ == "__main__":
    main()
