from datetime import UTC, datetime

from littledevil_recorder.restart_recovery import last_recorded_trade
from littledevil_recorder.storage import ParquetWriter


def test_last_recorded_trade_returns_none_when_no_file_exists(tmp_path):
    plan = last_recorded_trade(tmp_path, "BTCUSDT", datetime(2026, 9, 14).date())
    assert plan.last_known_trade_id is None
    assert plan.last_known_ts is None


def test_last_recorded_trade_finds_the_highest_trade_id(tmp_path):
    writer = ParquetWriter(tmp_path)
    day = datetime(2026, 9, 14, tzinfo=UTC)
    for trade_id, minute in [(100, 0), (105, 1), (102, 2)]:  # out of order on purpose
        ts = datetime(2026, 9, 14, 12, minute, tzinfo=UTC)
        writer.write_trade(
            "BTCUSDT", trade_id=trade_id, ts_exchange=ts, ts_received=ts,
            price=1.0, qty=1.0, is_buyer_maker=False,
        )
    writer.flush_trades("BTCUSDT", day.date())

    plan = last_recorded_trade(tmp_path, "BTCUSDT", day.date())
    assert plan.last_known_trade_id == 105
    assert plan.last_known_ts == datetime(2026, 9, 14, 12, 1, tzinfo=UTC)


def test_last_recorded_trade_handles_empty_file(tmp_path):
    writer = ParquetWriter(tmp_path)
    day = datetime(2026, 9, 14, tzinfo=UTC)
    # write then flush with nothing buffered should be a no-op, so simulate
    # an existing-but-empty scenario is covered by the "no file" case above;
    # this test just confirms flushing nothing doesn't create a bogus file.
    assert writer.flush_trades("BTCUSDT", day.date()) is None
    plan = last_recorded_trade(tmp_path, "BTCUSDT", day.date())
    assert plan.last_known_trade_id is None
