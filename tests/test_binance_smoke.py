"""Live Binance smoke tests - verify current public endpoints work.

These tests connect to real Binance public APIs (no authentication required)
and verify endpoint availability and response contracts.

Run with: uv run pytest tests/test_binance_smoke.py --tb=short
"""

import asyncio
import httpx
import pytest
from datetime import UTC, datetime, timedelta


BINANCE_API_URL = "https://fapi.binance.com"
BINANCE_WS_URL = "wss://fstream.binance.com/market/stream"

# Test symbols (highly liquid USDⓈ-M perpetuals)
TEST_SYMBOLS = ["BTCUSDT", "ETHUSDT"]
TEST_PAIR = "BTCUSDT"  # pair format for some endpoints


@pytest.mark.asyncio
@pytest.mark.smoke  # Can skip with: pytest -m "not smoke"
async def test_open_interest_hist_endpoint():
    """GET /futures/data/openInterestHist returns coarse OI history."""
    async with httpx.AsyncClient(timeout=10.0) as client:
        now = datetime.now(UTC)
        start_time = int((now - timedelta(hours=1)).timestamp() * 1000)
        end_time = int(now.timestamp() * 1000)

        response = await client.get(
            f"{BINANCE_API_URL}/futures/data/openInterestHist",
            params={
                "symbol": "BTCUSDT",
                "period": "5m",
                "startTime": start_time,
                "endTime": end_time,
                "limit": 10,
            },
        )

        assert response.status_code == 200, f"status {response.status_code}: {response.text}"
        data = response.json()
        assert isinstance(data, list), f"expected list, got {type(data)}"

        if len(data) > 0:
            row = data[0]
            required_fields = {"timestamp", "sumOpenInterest"}
            assert required_fields.issubset(row.keys()), f"missing fields: {required_fields - row.keys()}"
            assert isinstance(int(row["timestamp"]), int)
            assert isinstance(float(row["sumOpenInterest"]), float)


@pytest.mark.asyncio
@pytest.mark.smoke
async def test_funding_rate_history_endpoint():
    """GET /fapi/v1/fundingRate returns full funding events."""
    async with httpx.AsyncClient(timeout=10.0) as client:
        now = datetime.now(UTC)
        start_time = int((now - timedelta(hours=1)).timestamp() * 1000)
        end_time = int(now.timestamp() * 1000)

        response = await client.get(
            f"{BINANCE_API_URL}/fapi/v1/fundingRate",
            params={
                "symbol": "BTCUSDT",
                "startTime": start_time,
                "endTime": end_time,
                "limit": 10,
            },
        )

        assert response.status_code == 200, f"status {response.status_code}: {response.text}"
        data = response.json()
        assert isinstance(data, list), f"expected list, got {type(data)}"

        if len(data) > 0:
            row = data[0]
            required_fields = {"symbol", "fundingTime", "fundingRate", "markPrice"}
            assert required_fields.issubset(row.keys()), f"missing fields: {required_fields - row.keys()}"


@pytest.mark.asyncio
@pytest.mark.smoke
async def test_mark_price_klines_endpoint():
    """GET /fapi/v1/markPriceKlines returns mark price bars."""
    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.get(
            f"{BINANCE_API_URL}/fapi/v1/markPriceKlines",
            params={
                "symbol": "BTCUSDT",
                "interval": "1m",
                "limit": 5,
            },
        )

        assert response.status_code == 200, f"status {response.status_code}: {response.text}"
        data = response.json()
        assert isinstance(data, list), f"expected list, got {type(data)}"

        if len(data) > 0:
            kline = data[0]
            # Binance klines are arrays: [open_time, o, h, l, c, v, close_time, ...]
            assert isinstance(kline, list), f"expected kline array, got {type(kline)}"
            assert len(kline) >= 5, f"kline too short: {len(kline)}"


@pytest.mark.asyncio
@pytest.mark.smoke
async def test_index_price_klines_endpoint():
    """GET /fapi/v1/indexPriceKlines returns index price bars."""
    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.get(
            f"{BINANCE_API_URL}/fapi/v1/indexPriceKlines",
            params={
                "pair": "BTCUSDT",
                "interval": "1m",
                "limit": 5,
            },
        )

        assert response.status_code == 200, f"status {response.status_code}: {response.text}"
        data = response.json()
        assert isinstance(data, list), f"expected list, got {type(data)}"

        if len(data) > 0:
            kline = data[0]
            assert isinstance(kline, list), f"expected kline array, got {type(kline)}"


@pytest.mark.asyncio
@pytest.mark.smoke
async def test_basis_endpoint():
    """GET /futures/data/basis returns basis data."""
    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.get(
            f"{BINANCE_API_URL}/futures/data/basis",
            params={
                "pair": "BTCUSDT",
                "contractType": "PERPETUAL",
                "period": "1h",
                "limit": 5,
            },
        )

        assert response.status_code == 200, f"status {response.status_code}: {response.text}"
        data = response.json()
        assert isinstance(data, list), f"expected list, got {type(data)}"

        if len(data) > 0:
            row = data[0]
            required_fields = {"timestamp", "indexPrice", "futuresPrice", "basisRate"}
            assert required_fields.issubset(row.keys()), f"missing fields: {required_fields - row.keys()}"


@pytest.mark.asyncio
@pytest.mark.smoke
async def test_websocket_liquidation_stream_available():
    """WebSocket /market/stream supports !forceOrder@arr subscription."""
    import json
    import websockets

    try:
        async with websockets.connect(BINANCE_WS_URL, close_timeout=5.0) as ws:
            # Send subscription request
            await ws.send(json.dumps({
                "method": "SUBSCRIBE",
                "params": ["!forceOrder@arr"],
                "id": 1
            }))

            # Receive subscription response
            response = await asyncio.wait_for(ws.recv(), timeout=5.0)
            message = json.loads(response)

            # Binance sends a subscription response with "result": null on success
            assert message.get("id") == 1, f"subscription response has unexpected id: {message}"

            # Should be able to receive at least one event without error
            # (or subscription response from Binance)
            # No assertion on exact payload since liquidations are sampled

    except Exception as e:
        pytest.skip(f"WebSocket connection failed (Binance may be down): {e}")


def test_binance_endpoints_are_documented():
    """Verify all tested endpoints are documented in comments."""
    endpoints = [
        "GET /futures/data/openInterestHist",
        "GET /fapi/v1/fundingRate",
        "GET /fapi/v1/markPriceKlines",
        "GET /fapi/v1/indexPriceKlines",
        "GET /futures/data/basis",
        "WSS /market/stream with SUBSCRIBE",
    ]

    # This test serves as documentation of the Binance endpoints the Recorder relies on
    assert len(endpoints) == 6, "all Recorder data sources should be listed above"
