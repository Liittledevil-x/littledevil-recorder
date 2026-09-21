from datetime import UTC, datetime

from littledevil_recorder.restart_recovery import backfill_missed_trades, last_recorded_trade
from littledevil_recorder.storage import ParquetWriter, iter_parquet_paths


class _RecoveryResponse:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    def raise_for_status(self) -> None:
        pass

    def json(self) -> list[dict]:
        return self._rows


class _RecoveryClient:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows
        self.from_ids: list[int] = []

    async def get(self, _url: str, *, params: dict) -> _RecoveryResponse:
        self.from_ids.append(params["fromId"])
        return _RecoveryResponse(self._rows)


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


async def test_executing_the_same_recovery_range_twice_persists_each_trade_id_once(tmp_path):
    # A process may crash after a REST recovery response has been written but
    # before it can advance any in-memory recovery state. The next process
    # is allowed to repeat that exact fromId range; storage, not a fragile
    # assumption about request execution, enforces the durable invariant.
    base_ms = int(datetime(2026, 9, 19, tzinfo=UTC).timestamp() * 1000)
    rows = [
        {"a": 10, "T": base_ms, "p": "100.0", "q": "1.0", "m": False},
        {"a": 11, "T": base_ms + 1_000, "p": "101.0", "q": "2.0", "m": True},
        {"a": 12, "T": base_ms + 2_000, "p": "102.0", "q": "3.0", "m": False},
    ]
    first = ParquetWriter(tmp_path)
    first_client = _RecoveryClient(rows)
    assert await backfill_missed_trades(first_client, first, "BTCUSDT", from_trade_id=9) == 3
    first.flush_all()

    retry = ParquetWriter(tmp_path)
    retry_client = _RecoveryClient(rows)
    assert await backfill_missed_trades(retry_client, retry, "BTCUSDT", from_trade_id=9) == 3
    retry.flush_all()

    assert first_client.from_ids == [10]
    assert retry_client.from_ids == [10]
    import pyarrow.parquet as pq

    rows = [row for path in iter_parquet_paths(tmp_path, "trades", "BTCUSDT", datetime(2026, 9, 19).date())
            for row in pq.read_table(path).to_pylist()]
    assert sorted(row["trade_id"] for row in rows) == [10, 11, 12]
