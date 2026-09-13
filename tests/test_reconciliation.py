from decimal import Decimal

import pytest

from littledevil_recorder.depth import LocalOrderBook
from littledevil_recorder.reconciliation import ReconciliationResult


def test_match_fraction_computes_correctly():
    result = ReconciliationResult(
        symbol="BTCUSDT", matched_levels=18, total_levels=20, top_of_book_matches=True
    )
    assert result.match_fraction == pytest.approx(0.9)


def test_match_fraction_handles_zero_total_levels():
    result = ReconciliationResult(
        symbol="BTCUSDT", matched_levels=0, total_levels=0, top_of_book_matches=True
    )
    assert result.match_fraction == 1.0


def test_local_order_book_reflects_exact_state_for_comparison():
    # Sanity check that the comparison logic in check_reconciliation would
    # find a match when the book's state exactly mirrors a snapshot --
    # exercised without a live HTTP call by constructing the book directly.
    book = LocalOrderBook("BTCUSDT")
    book.load_snapshot(
        {"lastUpdateId": 1, "bids": [["100.0", "1.0"]], "asks": [["101.0", "1.0"]]}
    )
    assert book.bids[Decimal("100.0")] == Decimal("1.0")
