"""A failed individual write must not kill periodic flushing for all keys."""

from datetime import UTC, datetime

from littledevil_recorder.local_manifest import read_flush_failure_log
from littledevil_recorder.main import _flush_all_logging_failures
from littledevil_recorder.storage import ParquetWriter


def _make_failing_depth_writer(tmp_path, monkeypatch):
    writer = ParquetWriter(tmp_path)
    original = writer._flush

    def fail_depth(buffers, kind, schema, symbol, day):
        if kind == "depth" and symbol == "ETHUSDT":
            raise OSError("simulated full disk")
        return original(buffers, kind, schema, symbol, day)

    monkeypatch.setattr(writer, "_flush", fail_depth)
    return writer


def test_flush_failure_is_logged_while_healthy_parts_are_published(tmp_path, monkeypatch):
    monkeypatch.setenv("LITTLEDEVIL_DATA_ROOT", str(tmp_path))
    writer = _make_failing_depth_writer(tmp_path, monkeypatch)
    ts = datetime(2026, 9, 18, 12, tzinfo=UTC)
    writer.write_trade("BTCUSDT", trade_id=1, ts_exchange=ts, ts_received=ts,
                       price=1.0, qty=1.0, is_buyer_maker=False)
    writer.write_depth("ETHUSDT", ts_exchange=ts, ts_received=ts, is_snapshot=False,
                       bids_json="[]", asks_json="[]", seq=1)

    written = _flush_all_logging_failures(writer)
    assert len(written) == 1
    assert written[0].parent.name == "2026-09-18"
    entries = read_flush_failure_log(tmp_path)
    assert len(entries) == 1
    assert entries[0]["kind"] == "depth"
    assert ("ETHUSDT", ts.date()) in writer._depth  # no silently lost buffer


def test_repeated_failing_cycles_keep_flushing_healthy_keys(tmp_path, monkeypatch):
    monkeypatch.setenv("LITTLEDEVIL_DATA_ROOT", str(tmp_path))
    writer = _make_failing_depth_writer(tmp_path, monkeypatch)
    for cycle in range(3):
        ts = datetime(2026, 9, 18, 12, cycle, tzinfo=UTC)
        writer.write_trade("BTCUSDT", trade_id=cycle, ts_exchange=ts, ts_received=ts,
                           price=1.0, qty=1.0, is_buyer_maker=False)
        writer.write_depth("ETHUSDT", ts_exchange=ts, ts_received=ts, is_snapshot=False,
                           bids_json="[]", asks_json="[]", seq=cycle)
        assert len(_flush_all_logging_failures(writer)) == 1
    assert len(read_flush_failure_log(tmp_path)) == 3
