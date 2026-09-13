"""Measures Stage 0's book-reconstruction gate criterion directly: how often
does the locally maintained book match a fresh REST snapshot at the top N
levels? (docs/architecture-review.md §9: "snapshot+diff reconstruction
matches REST snapshot at aligned checkpoints on >= 99% of checks.")

This is a standalone check, not part of the always-on recorder loop --
run it periodically (or on demand) against a live LocalOrderBook to log a
pass/fail per checkpoint.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

import httpx

from littledevil_recorder.depth import LocalOrderBook, fetch_depth_snapshot


@dataclass
class ReconciliationResult:
    symbol: str
    matched_levels: int
    total_levels: int
    top_of_book_matches: bool

    @property
    def match_fraction(self) -> float:
        if self.total_levels == 0:
            return 1.0
        return self.matched_levels / self.total_levels


async def check_reconciliation(
    client: httpx.AsyncClient, book: LocalOrderBook, *, depth: int = 10
) -> ReconciliationResult:
    """Compares `book`'s current top `depth` levels per side against a
    freshly fetched REST snapshot. Price levels present in the fresh
    snapshot's top `depth` are checked for an exact quantity match in the
    local book; this is a point-in-time comparison, so a handful of
    mismatches from levels that changed between fetch and comparison is
    expected -- that's exactly why the gate is 99%, not 100%."""
    snapshot = await fetch_depth_snapshot(client, book.symbol, limit=depth)

    matched = 0
    total = 0
    for price_str, qty_str in snapshot["bids"][:depth]:
        total += 1
        if book.bids.get(Decimal(price_str)) == Decimal(qty_str):
            matched += 1
    for price_str, qty_str in snapshot["asks"][:depth]:
        total += 1
        if book.asks.get(Decimal(price_str)) == Decimal(qty_str):
            matched += 1

    top_bid_matches = bool(snapshot["bids"]) and book.bids.get(
        Decimal(snapshot["bids"][0][0])
    ) == Decimal(snapshot["bids"][0][1])
    top_ask_matches = bool(snapshot["asks"]) and book.asks.get(
        Decimal(snapshot["asks"][0][0])
    ) == Decimal(snapshot["asks"][0][1])

    return ReconciliationResult(
        symbol=book.symbol,
        matched_levels=matched,
        total_levels=total,
        top_of_book_matches=top_bid_matches and top_ask_matches,
    )
