from datetime import UTC, datetime

import pyarrow.parquet as pq
import pytest

from littledevil_recorder.storage import ParquetWriter


@pytest.fixture
def writer(tmp_path):
    return ParquetWriter(tmp_path)


def test_write_trade_and_flush_roundtrip(writer, tmp_path):
    ts = datetime(2026, 9, 14, 12, 0, 0, tzinfo=UTC)
    writer.write_trade(
        "BTCUSDT",
        trade_id=1,
        ts_exchange=ts,
        ts_received=ts,
        price=65000.5,
        qty=0.01,
        is_buyer_maker=False,
    )
    path = writer.flush_trades("BTCUSDT", ts.date())
    assert path is not None
    assert path == tmp_path / "trades" / "BTCUSDT" / "2026-09-14.parquet"

    table = pq.read_table(path)
    assert table.num_rows == 1
    row = table.to_pylist()[0]
    assert row["trade_id"] == 1
    assert row["price"] == 65000.5
    assert row["venue"] == "binance_spot"


def test_flushing_twice_appends_without_duplicating_prior_rows(writer, tmp_path):
    ts = datetime(2026, 9, 14, 12, 0, 0, tzinfo=UTC)
    writer.write_trade(
        "BTCUSDT", trade_id=1, ts_exchange=ts, ts_received=ts,
        price=1.0, qty=1.0, is_buyer_maker=False,
    )
    writer.flush_trades("BTCUSDT", ts.date())

    ts2 = datetime(2026, 9, 14, 12, 5, 0, tzinfo=UTC)
    writer.write_trade(
        "BTCUSDT", trade_id=2, ts_exchange=ts2, ts_received=ts2,
        price=2.0, qty=1.0, is_buyer_maker=True,
    )
    writer.flush_trades("BTCUSDT", ts.date())

    table = pq.read_table(tmp_path / "trades" / "BTCUSDT" / "2026-09-14.parquet")
    assert table.num_rows == 2
    trade_ids = sorted(row["trade_id"] for row in table.to_pylist())
    assert trade_ids == [1, 2]


def test_flush_with_no_buffered_rows_returns_none(writer):
    assert writer.flush_trades("BTCUSDT", datetime.now(UTC).date()) is None


def test_write_depth_and_flush_roundtrip(writer, tmp_path):
    ts = datetime(2026, 9, 14, 12, 0, 0, tzinfo=UTC)
    writer.write_depth(
        "ETHUSDT",
        ts_exchange=ts,
        ts_received=ts,
        is_snapshot=True,
        bids_json='[["3000.0", "1.5"]]',
        asks_json='[["3000.5", "2.0"]]',
        seq=100,
    )
    path = writer.flush_depth("ETHUSDT", ts.date())
    assert path == tmp_path / "depth" / "ETHUSDT" / "2026-09-14.parquet"
    table = pq.read_table(path)
    assert table.num_rows == 1
    assert table.to_pylist()[0]["seq"] == 100


def test_flush_all_writes_every_buffered_symbol_and_day(writer, tmp_path):
    ts = datetime(2026, 9, 14, 12, 0, 0, tzinfo=UTC)
    writer.write_trade(
        "BTCUSDT", trade_id=1, ts_exchange=ts, ts_received=ts,
        price=1.0, qty=1.0, is_buyer_maker=False,
    )
    writer.write_depth(
        "BTCUSDT", ts_exchange=ts, ts_received=ts,
        is_snapshot=True, bids_json="[]", asks_json="[]", seq=1,
    )
    written = writer.flush_all()
    assert len(written) == 2
    assert (tmp_path / "trades" / "BTCUSDT" / "2026-09-14.parquet") in written
    assert (tmp_path / "depth" / "BTCUSDT" / "2026-09-14.parquet") in written
