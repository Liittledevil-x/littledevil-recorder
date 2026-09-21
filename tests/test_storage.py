from datetime import UTC, date, datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import littledevil_recorder.storage as storage
from littledevil_recorder.restart_recovery import last_recorded_trade
from littledevil_recorder.local_manifest import quarantine_file, read_quarantine_manifest
from littledevil_recorder.storage import (
    CompactionError,
    FlushAllError,
    ParquetWriter,
    compact_closed_day,
    iter_parquet_paths,
)


DAY = date(2026, 9, 14)


def _trade(writer, trade_id, *, symbol="BTCUSDT", day=DAY):
    ts = datetime(day.year, day.month, day.day, 12, trade_id % 60, tzinfo=UTC)
    writer.write_trade(symbol, trade_id=trade_id, ts_exchange=ts, ts_received=ts,
                       price=float(trade_id), qty=1.0, is_buyer_maker=False)


def _rows(root, kind, symbol, day=DAY):
    return [row for path in iter_parquet_paths(root, kind, symbol, day)
            for row in pq.read_table(path).to_pylist()]


def test_new_flush_publishes_an_immutable_part_and_reader_discovers_it(tmp_path):
    writer = ParquetWriter(tmp_path)
    _trade(writer, 1)
    path = writer.flush_trades("BTCUSDT", DAY)
    assert path and path.parent == tmp_path / "trades" / "BTCUSDT" / DAY.isoformat()
    assert path.name.startswith("part-")
    assert [row["trade_id"] for row in _rows(tmp_path, "trades", "BTCUSDT")] == [1]


def test_same_recovery_range_twice_and_overlapping_ranges_are_idempotent(tmp_path):
    first = ParquetWriter(tmp_path)
    for trade_id in (100, 101, 102):
        _trade(first, trade_id)
    first.flush_all()
    first.close()

    retry = ParquetWriter(tmp_path)
    for trade_id in (100, 101, 102, 103, 104):
        _trade(retry, trade_id)
    retry.flush_all()
    assert sorted(row["trade_id"] for row in _rows(tmp_path, "trades", "BTCUSDT")) == [100, 101, 102, 103, 104]


def test_duplicates_inside_one_batch_and_persisted_tail_are_written_once(tmp_path):
    writer = ParquetWriter(tmp_path)
    _trade(writer, 200)
    _trade(writer, 200)
    _trade(writer, 201)
    writer.flush_all()
    _trade(writer, 201)
    _trade(writer, 202)
    writer.flush_all()
    assert sorted(row["trade_id"] for row in _rows(tmp_path, "trades", "BTCUSDT")) == [200, 201, 202]


def test_crash_after_part_publish_before_manifest_commit_is_invisible_and_retry_safe(tmp_path, monkeypatch):
    writer = ParquetWriter(tmp_path)
    _trade(writer, 300)
    original = writer._state.insert_part
    monkeypatch.setattr(writer._state, "insert_part", lambda **_: (_ for _ in ()).throw(RuntimeError("crash")))
    with pytest.raises(FlushAllError, match="failed"):
        writer.flush_all()
    assert list(iter_parquet_paths(tmp_path, "trades", "BTCUSDT", DAY)) == []
    assert list((tmp_path / "trades" / "BTCUSDT" / DAY.isoformat()).glob("*.parquet"))  # forensic orphan
    writer.close()

    retry = ParquetWriter(tmp_path)
    _trade(retry, 300)
    _trade(retry, 301)
    retry.flush_all()
    assert sorted(row["trade_id"] for row in _rows(tmp_path, "trades", "BTCUSDT")) == [300, 301]


def test_recovery_high_water_mark_streams_legacy_and_active_parts(tmp_path):
    legacy = tmp_path / "trades" / "BTCUSDT" / f"{DAY.isoformat()}.parquet"
    legacy.parent.mkdir(parents=True)
    pq.write_table(pa.table({
        "trade_id": [1],
        "ts_exchange": [datetime(2026, 9, 14, tzinfo=UTC)],
        "ts_received": [datetime(2026, 9, 14, tzinfo=UTC)],
        "price": [1.0], "qty": [1.0], "is_buyer_maker": [False], "venue": ["binance_spot"],
    }), legacy)
    writer = ParquetWriter(tmp_path)
    _trade(writer, 2)
    writer.flush_all()
    plan = last_recorded_trade(tmp_path, "BTCUSDT", DAY)
    assert plan.last_known_trade_id == 2
    assert sorted(row["trade_id"] for row in _rows(tmp_path, "trades", "BTCUSDT")) == [1, 2]


def test_legacy_overlap_is_indexed_without_rewriting_legacy_history(tmp_path):
    legacy = tmp_path / "trades" / "BTCUSDT" / f"{DAY.isoformat()}.parquet"
    legacy.parent.mkdir(parents=True)
    pq.write_table(pa.table({
        "trade_id": [400],
        "ts_exchange": [datetime(2026, 9, 14, tzinfo=UTC)],
        "ts_received": [datetime(2026, 9, 14, tzinfo=UTC)],
        "price": [1.0], "qty": [1.0], "is_buyer_maker": [False], "venue": ["binance_spot"],
    }), legacy)
    original_bytes = legacy.read_bytes()
    writer = ParquetWriter(tmp_path)
    _trade(writer, 400)
    _trade(writer, 401)
    writer.flush_all()
    assert legacy.read_bytes() == original_bytes
    assert sorted(row["trade_id"] for row in _rows(tmp_path, "trades", "BTCUSDT")) == [400, 401]


def test_depth_parts_accumulate_without_rewriting_prior_parts(tmp_path):
    writer = ParquetWriter(tmp_path)
    for sequence in (1, 2):
        ts = datetime(2026, 9, 14, 12, sequence, tzinfo=UTC)
        writer.write_depth("ETHUSDT", ts_exchange=ts, ts_received=ts, is_snapshot=False,
                           bids_json="[]", asks_json="[]", seq=sequence)
        writer.flush_all()
    paths = list(iter_parquet_paths(tmp_path, "depth", "ETHUSDT", DAY))
    assert len(paths) == 2
    assert [row["seq"] for row in _rows(tmp_path, "depth", "ETHUSDT")] == [1, 2]


def test_part_quarantine_renames_data_and_removes_it_from_manifest_readers(tmp_path):
    writer = ParquetWriter(tmp_path)
    _trade(writer, 450)
    writer.flush_all()
    quarantined = quarantine_file(tmp_path, kind="trades", symbol="BTCUSDT", day=DAY.isoformat(), reason="test")
    assert quarantined and ".quarantined-" in quarantined.name
    assert list(iter_parquet_paths(tmp_path, "trades", "BTCUSDT", DAY)) == []
    assert read_quarantine_manifest(tmp_path)[-1]["reason"] == "test"


def test_closed_day_compaction_replaces_visible_parts_not_history(tmp_path):
    writer = ParquetWriter(tmp_path)
    _trade(writer, 500)
    writer.flush_all()
    _trade(writer, 501)
    writer.flush_all()
    source_paths = list(iter_parquet_paths(tmp_path, "trades", "BTCUSDT", DAY))
    compacted = compact_closed_day(tmp_path, "trades", "BTCUSDT", DAY)
    assert compacted and compacted.name.startswith("compacted-")
    assert list(iter_parquet_paths(tmp_path, "trades", "BTCUSDT", DAY)) == [compacted]
    assert all(path.exists() for path in source_paths)  # retained for rollback/forensics
    assert sorted(row["trade_id"] for row in _rows(tmp_path, "trades", "BTCUSDT")) == [500, 501]


def test_compaction_failure_leaves_source_parts_visible(tmp_path, monkeypatch):
    writer = ParquetWriter(tmp_path)
    _trade(writer, 600)
    writer.flush_all()
    _trade(writer, 601)
    writer.flush_all()
    before = list(iter_parquet_paths(tmp_path, "trades", "BTCUSDT", DAY))
    monkeypatch.setattr(storage._State, "compact", lambda *_, **__: (_ for _ in ()).throw(RuntimeError("disk full")))
    with pytest.raises(CompactionError):
        compact_closed_day(tmp_path, "trades", "BTCUSDT", DAY)
    assert list(iter_parquet_paths(tmp_path, "trades", "BTCUSDT", DAY)) == before


def test_buffers_are_removed_after_flush_across_many_symbol_days(tmp_path):
    writer = ParquetWriter(tmp_path)
    for offset in range(100):
        day = date.fromordinal(DAY.toordinal() + offset)
        _trade(writer, offset, symbol=f"S{offset}", day=day)
        writer.flush_all()
    assert not writer._trades
