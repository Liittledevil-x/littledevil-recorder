from datetime import UTC, datetime

from littledevil_recorder.aggtrade import combined_stream_url, parse_agg_trade


def test_combined_stream_url_lowercases_and_joins_symbols():
    url = combined_stream_url(["BTCUSDT", "ETHUSDT"])
    assert url == "wss://data-stream.binance.vision/stream?streams=btcusdt@aggTrade/ethusdt@aggTrade"


def test_parse_agg_trade_matches_binance_payload_shape():
    # Real shape of a Binance combined-stream @aggTrade 'data' payload.
    raw = {
        "e": "aggTrade",
        "E": 1757858400123,
        "s": "BTCUSDT",
        "a": 123456789,
        "p": "65000.50",
        "q": "0.01200",
        "f": 100,
        "l": 105,
        "T": 1757858400100,
        "m": True,
    }
    parsed = parse_agg_trade(raw)
    assert parsed["symbol"] == "BTCUSDT"
    assert parsed["trade_id"] == 123456789
    assert parsed["price"] == 65000.50
    assert parsed["qty"] == 0.012
    assert parsed["is_buyer_maker"] is True
    assert parsed["ts_exchange"] == datetime.fromtimestamp(1757858400100 / 1000, tz=UTC)
