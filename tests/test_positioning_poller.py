"""Positioning poller tests: open interest / funding / mark-index polling
from three distinct Binance USDⓈ-M REST endpoints.

Network-isolated by default: every test patches aiohttp at the session level
or the module-level `_poll_*` functions, never making a real request.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import pytest

from littledevil_recorder.positioning_poller import (
    _poll_open_interest,
    _poll_premium_index,
    _poll_funding_rate,
    run_positioning_poller,
)
from littledevil_recorder.storage import ParquetWriter
from littledevil_recorder.subscription_manager import SubscriptionManager

# Real-shaped fixtures, field-for-field matching the official response
# schemas verified against developers.binance.com during this pass.
OPEN_INTEREST_FIXTURE = {"openInterest": "10659.509", "symbol": "BTCUSDT", "time": 1589437530011}

PREMIUM_INDEX_FIXTURE = {
    "symbol": "BTCUSDT",
    "markPrice": "50000.12345678",
    "indexPrice": "50001.23456789",
    "estimatedSettlePrice": "50000.00000000",
    "lastFundingRate": "0.00010000",
    "interestRate": "0.00010000",
    "nextFundingTime": 1589446800000,
    "time": 1589437530011,
}

FUNDING_RATE_FIXTURE = [{
    "symbol": "BTCUSDT",
    "fundingRate": "0.00010000",
    "fundingTime": 1589414400000,
    "markPrice": "49999.00000000",
    "rateType": "REGULAR",
}]


class _FakeWriter:
    """Minimal stand-in for ParquetWriter, capturing per-source writes."""

    def __init__(self):
        self.open_interest_writes: list[dict] = []
        self.funding_writes: list[dict] = []
        self.mark_index_writes: list[dict] = []

    def write_open_interest(self, symbol, **kwargs):
        self.open_interest_writes.append({"symbol": symbol, **kwargs})

    def write_funding(self, symbol, **kwargs):
        self.funding_writes.append({"symbol": symbol, **kwargs})

    def write_mark_index(self, symbol, **kwargs):
        self.mark_index_writes.append({"symbol": symbol, **kwargs})


class _FakeResponse:
    def __init__(self, payload, status=200):
        self._payload = payload
        self.status = status

    async def json(self):
        return self._payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class _FakeSession:
    """Routes GET calls to the fixture matching the URL, ignoring params."""

    def __init__(self, responses: dict[str, object]):
        self._responses = responses

    def get(self, url, **kwargs):
        return _FakeResponse(self._responses[url])


# ---- Field-mapping correctness (the original defect) ----


@pytest.mark.asyncio
async def test_open_interest_endpoint_never_reads_funding_or_price_fields():
    """The OI endpoint response has no funding/mark/index field at all --
    this must never silently read/zero-fill them (the original defect)."""
    from littledevil_recorder.positioning_poller import OPEN_INTEREST_URL

    writer = _FakeWriter()
    session = _FakeSession({OPEN_INTEREST_URL: OPEN_INTEREST_FIXTURE})

    await _poll_open_interest(session, writer, "BTCUSDT")

    assert len(writer.open_interest_writes) == 1
    row = writer.open_interest_writes[0]
    assert row["open_interest"] == 10659.509
    assert row["ts_exchange"] == datetime.fromtimestamp(1589437530011 / 1000, tz=UTC)
    assert writer.funding_writes == []
    assert writer.mark_index_writes == []


@pytest.mark.asyncio
async def test_open_interest_missing_field_raises_rather_than_defaulting():
    """A malformed/incomplete OI response must fail loudly, never write a
    fabricated 0.0 in place of a genuinely absent field."""
    from littledevil_recorder.positioning_poller import OPEN_INTEREST_URL

    writer = _FakeWriter()
    broken = {"symbol": "BTCUSDT", "time": 1589437530011}  # openInterest missing
    session = _FakeSession({OPEN_INTEREST_URL: broken})

    with pytest.raises(KeyError):
        await _poll_open_interest(session, writer, "BTCUSDT")
    assert writer.open_interest_writes == []


@pytest.mark.asyncio
async def test_premium_index_writes_mark_index_only_not_a_funding_record():
    """premiumIndex has no authoritative funding SETTLEMENT timestamp (only
    nextFundingTime, the upcoming one) -- it must never fabricate a funding
    event record from poll time."""
    from littledevil_recorder.positioning_poller import PREMIUM_INDEX_URL

    writer = _FakeWriter()
    session = _FakeSession({PREMIUM_INDEX_URL: PREMIUM_INDEX_FIXTURE})

    await _poll_premium_index(session, writer, "BTCUSDT")

    assert len(writer.mark_index_writes) == 1
    row = writer.mark_index_writes[0]
    assert row["mark_price"] == 50000.12345678
    assert row["index_price"] == 50001.23456789
    assert row["estimated_settle_price"] == 50000.0
    assert row["last_funding_rate"] == 0.0001
    assert row["interest_rate"] == 0.0001
    assert row["next_funding_time"] == datetime.fromtimestamp(1589446800000 / 1000, tz=UTC)
    assert writer.funding_writes == []


@pytest.mark.asyncio
async def test_funding_rate_endpoint_gives_true_settlement_timestamp():
    """The dedicated fundingRate history endpoint's fundingTime is the real
    settlement time and must be used as-is, never derived from poll time."""
    from littledevil_recorder.positioning_poller import FUNDING_RATE_URL

    writer = _FakeWriter()
    session = _FakeSession({FUNDING_RATE_URL: FUNDING_RATE_FIXTURE})

    await _poll_funding_rate(session, writer, "BTCUSDT")

    assert len(writer.funding_writes) == 1
    row = writer.funding_writes[0]
    assert row["funding_rate"] == 0.0001
    assert row["funding_time"] == datetime.fromtimestamp(1589414400000 / 1000, tz=UTC)
    assert row["rate_type"] == "REGULAR"
    assert row["mark_price"] == 49999.0


@pytest.mark.asyncio
async def test_funding_rate_empty_history_raises():
    from littledevil_recorder.positioning_poller import FUNDING_RATE_URL

    writer = _FakeWriter()
    session = _FakeSession({FUNDING_RATE_URL: []})

    with pytest.raises(RuntimeError, match="empty funding rate history"):
        await _poll_funding_rate(session, writer, "BTCUSDT")


@pytest.mark.asyncio
async def test_http_error_status_raises_for_each_endpoint():
    class _ErrorResponse(_FakeResponse):
        pass

    class _ErrorSession:
        def get(self, url, **kwargs):
            return _FakeResponse({}, status=418)

    writer = _FakeWriter()
    session = _ErrorSession()
    with pytest.raises(RuntimeError, match="HTTP 418"):
        await _poll_open_interest(session, writer, "BTCUSDT")
    with pytest.raises(RuntimeError, match="HTTP 418"):
        await _poll_premium_index(session, writer, "BTCUSDT")
    with pytest.raises(RuntimeError, match="HTTP 418"):
        await _poll_funding_rate(session, writer, "BTCUSDT")


# ---- Poller loop / subscription scheduling behavior ----


@pytest.mark.asyncio
async def test_run_poller_reads_desired_symbols_and_polls_when_available(tmp_path):
    subscriptions = SubscriptionManager(tmp_path)
    writer = _FakeWriter()
    stop_event = asyncio.Event()
    subscriptions.add_symbols("positioning", ["BTCUSDT"])

    poll_count = 0

    async def mock_poll_batch(session, w, symbols, on_symbol_polled=None):
        nonlocal poll_count
        poll_count += 1
        if symbols == ["BTCUSDT"]:
            w.write_open_interest("BTCUSDT", ts_exchange=datetime.now(UTC), ts_received=datetime.now(UTC), open_interest=100.0, source="binance_usdm")
        if poll_count >= 1:
            stop_event.set()

    with patch("littledevil_recorder.positioning_poller._poll_batch", side_effect=mock_poll_batch):
        await run_positioning_poller(subscriptions, writer, poll_interval=0.01, stop_event=stop_event)

    assert poll_count == 1
    assert len(writer.open_interest_writes) == 1
    assert writer.open_interest_writes[0]["symbol"] == "BTCUSDT"


@pytest.mark.asyncio
async def test_poller_skips_poll_when_no_desired_symbols(tmp_path):
    subscriptions = SubscriptionManager(tmp_path)
    writer = _FakeWriter()
    stop_event = asyncio.Event()
    poll_count = 0

    async def mock_poll_batch(session, w, symbols, on_symbol_polled=None):
        nonlocal poll_count
        poll_count += 1

    with patch("littledevil_recorder.positioning_poller._poll_batch", side_effect=mock_poll_batch):
        stop_event.set()
        await run_positioning_poller(subscriptions, writer, poll_interval=0.01, stop_event=stop_event)

    assert poll_count == 0


@pytest.mark.asyncio
async def test_poller_runtime_symbol_additions(tmp_path):
    subscriptions = SubscriptionManager(tmp_path)
    writer = _FakeWriter()
    stop_event = asyncio.Event()
    subscriptions.add_symbols("positioning", ["BTCUSDT"])

    poll_symbols_seen = []

    async def mock_poll_batch(session, w, symbols, on_symbol_polled=None):
        poll_symbols_seen.append(list(symbols))
        if len(poll_symbols_seen) == 1:
            subscriptions.add_symbols("positioning", ["ETHUSDT"])
        elif len(poll_symbols_seen) == 2:
            stop_event.set()

    with patch("littledevil_recorder.positioning_poller._poll_batch", side_effect=mock_poll_batch):
        await run_positioning_poller(subscriptions, writer, poll_interval=0.01, stop_event=stop_event)

    assert len(poll_symbols_seen) == 2
    assert poll_symbols_seen[0] == ["BTCUSDT"]
    assert set(poll_symbols_seen[1]) == {"BTCUSDT", "ETHUSDT"}


@pytest.mark.asyncio
async def test_poll_batch_calls_on_symbol_polled_with_none_error_on_success():
    from littledevil_recorder.positioning_poller import _poll_batch

    writer = _FakeWriter()
    calls = []

    async def fake_poll_symbol(session, w, symbol):
        return None

    session = object()
    with patch("littledevil_recorder.positioning_poller._poll_symbol", side_effect=fake_poll_symbol):
        await _poll_batch(session, writer, ["BTCUSDT"], on_symbol_polled=lambda s, e: calls.append((s, e)))

    assert calls == [("BTCUSDT", None)]


@pytest.mark.asyncio
async def test_poll_batch_calls_on_symbol_polled_with_error_on_failure():
    from littledevil_recorder.positioning_poller import _poll_batch

    writer = _FakeWriter()
    calls = []
    boom = RuntimeError("boom")

    async def fake_poll_symbol(session, w, symbol):
        raise boom

    session = object()
    with patch("littledevil_recorder.positioning_poller._poll_symbol", side_effect=fake_poll_symbol):
        await _poll_batch(session, writer, ["BTCUSDT"], on_symbol_polled=lambda s, e: calls.append((s, e)))

    assert calls == [("BTCUSDT", boom)]


@pytest.mark.asyncio
async def test_poll_symbol_raises_with_detail_when_one_source_fails_and_others_succeed():
    from littledevil_recorder.positioning_poller import _poll_symbol

    writer = _FakeWriter()

    async def ok(*a, **k):
        return None

    async def fail(*a, **k):
        raise RuntimeError("oi down")

    with patch("littledevil_recorder.positioning_poller._poll_open_interest", side_effect=fail), \
         patch("littledevil_recorder.positioning_poller._poll_premium_index", side_effect=ok), \
         patch("littledevil_recorder.positioning_poller._poll_funding_rate", side_effect=ok):
        with pytest.raises(RuntimeError, match="open_interest"):
            await _poll_symbol(object(), writer, "BTCUSDT")


# ---- Real ParquetWriter round-trip (schema correctness end-to-end) ----


@pytest.mark.asyncio
async def test_real_writer_persists_each_source_to_its_own_schema(tmp_path):
    from littledevil_recorder.positioning_poller import OPEN_INTEREST_URL, PREMIUM_INDEX_URL, FUNDING_RATE_URL

    writer = ParquetWriter(tmp_path)
    session = _FakeSession({
        OPEN_INTEREST_URL: OPEN_INTEREST_FIXTURE,
        PREMIUM_INDEX_URL: PREMIUM_INDEX_FIXTURE,
        FUNDING_RATE_URL: FUNDING_RATE_FIXTURE,
    })

    from littledevil_recorder.positioning_poller import _poll_symbol
    await _poll_symbol(session, writer, "BTCUSDT")

    day = datetime.fromtimestamp(1589437530011 / 1000, tz=UTC).date()
    assert writer.flush_open_interest("BTCUSDT", day) is not None
    assert writer.flush_mark_index("BTCUSDT", day) is not None
    funding_day = datetime.fromtimestamp(1589414400000 / 1000, tz=UTC).date()
    assert writer.flush_funding("BTCUSDT", funding_day) is not None
    writer.close()
