from decimal import Decimal

from littledevil_recorder.depth import LocalOrderBook, combined_depth_stream_url


def test_combined_depth_stream_url():
    url = combined_depth_stream_url(["BTCUSDT", "ETHUSDT"])
    assert url == "wss://data-stream.binance.vision/stream?streams=btcusdt@depth@100ms/ethusdt@depth@100ms"


def test_load_snapshot_populates_book():
    book = LocalOrderBook("BTCUSDT")
    book.load_snapshot(
        {
            "lastUpdateId": 1000,
            "bids": [["100.0", "1.5"], ["99.5", "2.0"]],
            "asks": [["100.5", "1.0"]],
        }
    )
    assert book.bids[Decimal("100.0")] == Decimal("1.5")
    assert book.asks[Decimal("100.5")] == Decimal("1.0")
    assert book.last_update_id == 1000
    assert book.synced is False


def test_is_stale_drops_events_entirely_before_snapshot():
    book = LocalOrderBook("BTCUSDT")
    book.load_snapshot({"lastUpdateId": 1000, "bids": [], "asks": []})
    assert book.is_stale({"U": 990, "u": 999, "b": [], "a": []}) is True
    assert book.is_stale({"U": 990, "u": 1001, "b": [], "a": []}) is False


def test_can_apply_first_requires_straddling_snapshot():
    book = LocalOrderBook("BTCUSDT")
    book.load_snapshot({"lastUpdateId": 1000, "bids": [], "asks": []})
    # Binance's rule: first event applied must have U <= lastUpdateId+1 <= u.
    assert book.can_apply_first({"U": 995, "u": 1005, "b": [], "a": []}) is True
    assert book.can_apply_first({"U": 1002, "u": 1010, "b": [], "a": []}) is False


def test_apply_updates_and_removes_levels():
    book = LocalOrderBook("BTCUSDT")
    book.load_snapshot(
        {"lastUpdateId": 1000, "bids": [["100.0", "1.0"]], "asks": [["101.0", "1.0"]]}
    )
    book.apply(
        {
            "U": 1001,
            "u": 1002,
            "b": [["100.0", "2.0"], ["99.0", "0.5"]],
            "a": [["101.0", "0"]],  # qty 0 removes the level
        }
    )
    assert book.bids[Decimal("100.0")] == Decimal("2.0")
    assert book.bids[Decimal("99.0")] == Decimal("0.5")
    assert Decimal("101.0") not in book.asks
    assert book.last_update_id == 1002
    assert book.synced is True


def test_top_n_sorts_bids_descending_and_asks_ascending():
    book = LocalOrderBook("BTCUSDT")
    book.load_snapshot(
        {
            "lastUpdateId": 1,
            "bids": [["99.0", "1"], ["100.0", "1"], ["98.0", "1"]],
            "asks": [["102.0", "1"], ["101.0", "1"], ["103.0", "1"]],
        }
    )
    bids, asks = book.top_n(2)
    assert bids == [("100.0", "1"), ("99.0", "1")]
    assert asks == [("101.0", "1"), ("102.0", "1")]
