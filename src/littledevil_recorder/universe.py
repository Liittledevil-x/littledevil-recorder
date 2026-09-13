"""Universe Refresh -> universe_membership (docs/scanner-attention-routing.md
§2; docs/data-and-events.md §1). Runs daily; every threshold below is the
document's own bracketed *starting point*, kept as named, overridable
constants -- not a guess, since the doc states these values explicitly, but
not frozen either: `docs/build-plan.md` §3 step 5 still applies once this
feeds a real ranking/detector, at which point these need Omar's
confirmation before being treated as final.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

MIN_LISTING_AGE_DAYS = 180
MIN_30D_MEDIAN_QUOTE_VOLUME_USD = 30_000_000
MAX_30D_MEDIAN_SPREAD_BPS = 5
MAX_30D_P95_SPREAD_BPS = 15
MIN_MEDIAN_DEPTH_25BPS_USD = 150_000
MIN_MEDIAN_TRADES_PER_MINUTE = 30
MAX_COINGECKO_RANK = 150
MAX_UNLOCK_PCT_WITHIN_DAYS = (2, 7)
MIN_FEED_HEALTH_7D_PCT = 99

HYSTERESIS_ENTER_AFTER_CONSECUTIVE_PASSES = 3
HYSTERESIS_EXIT_AFTER_CONSECUTIVE_FAILS = 5

EXCLUDED_CATEGORIES = frozenset({"stablecoin", "leveraged_token", "wrapped_bridged_duplicate"})

RECORDING_SET_SIZE = 25
RECORDING_SET_MATURITY_HOURS = 12


@dataclass
class SymbolMetrics:
    symbol: str
    listing_age_days: int
    median_quote_volume_30d_usd: float
    median_spread_bps: float
    p95_spread_bps: float
    median_depth_25bps_usd: float
    median_trades_per_minute: float
    coingecko_rank: int | None
    category: str | None
    feed_health_7d_pct: float
    trading_status: str = "TRADING"
    has_delisting_announcement: bool = False


@dataclass
class EligibilityCheck:
    eligible: bool
    reasons_failed: list[str]
    criteria_snapshot: dict


def check_eligibility(metrics: SymbolMetrics) -> EligibilityCheck:
    reasons: list[str] = []

    if metrics.trading_status != "TRADING":
        reasons.append(f"trading_status={metrics.trading_status}")
    if metrics.has_delisting_announcement:
        reasons.append("delisting_announcement")
    if metrics.listing_age_days < MIN_LISTING_AGE_DAYS:
        reasons.append(f"listing_age_days={metrics.listing_age_days}<{MIN_LISTING_AGE_DAYS}")
    if metrics.median_quote_volume_30d_usd < MIN_30D_MEDIAN_QUOTE_VOLUME_USD:
        reasons.append("median_quote_volume_30d_below_floor")
    if metrics.median_spread_bps > MAX_30D_MEDIAN_SPREAD_BPS:
        reasons.append("median_spread_above_ceiling")
    if metrics.p95_spread_bps > MAX_30D_P95_SPREAD_BPS:
        reasons.append("p95_spread_above_ceiling")
    if metrics.median_depth_25bps_usd < MIN_MEDIAN_DEPTH_25BPS_USD:
        reasons.append("median_depth_below_floor")
    if metrics.median_trades_per_minute < MIN_MEDIAN_TRADES_PER_MINUTE:
        reasons.append("median_trades_per_minute_below_floor")
    if metrics.coingecko_rank is None or metrics.coingecko_rank > MAX_COINGECKO_RANK:
        reasons.append("coingecko_rank_missing_or_above_ceiling")
    if metrics.category in EXCLUDED_CATEGORIES:
        reasons.append(f"excluded_category={metrics.category}")
    if metrics.feed_health_7d_pct < MIN_FEED_HEALTH_7D_PCT:
        reasons.append("feed_health_below_floor")

    snapshot = {
        "listing_age_days": metrics.listing_age_days,
        "median_quote_volume_30d_usd": metrics.median_quote_volume_30d_usd,
        "median_spread_bps": metrics.median_spread_bps,
        "p95_spread_bps": metrics.p95_spread_bps,
        "median_depth_25bps_usd": metrics.median_depth_25bps_usd,
        "median_trades_per_minute": metrics.median_trades_per_minute,
        "coingecko_rank": metrics.coingecko_rank,
        "category": metrics.category,
        "feed_health_7d_pct": metrics.feed_health_7d_pct,
        "reasons_failed": reasons,
    }
    return EligibilityCheck(eligible=not reasons, reasons_failed=reasons, criteria_snapshot=snapshot)


def apply_hysteresis(
    *, currently_eligible: bool, consecutive_pass_days: int, consecutive_fail_days: int, passed_today: bool
) -> tuple[bool, int, int]:
    """Returns (new_eligible, new_consecutive_pass_days, new_consecutive_fail_days).

    A pair already in the universe only exits after HYSTERESIS_EXIT_AFTER
    consecutive failing days; a pair outside only enters after
    HYSTERESIS_ENTER_AFTER consecutive passing days (scanner-attention-
    routing.md §2).
    """
    if passed_today:
        new_pass_days = consecutive_pass_days + 1
        new_fail_days = 0
    else:
        new_pass_days = 0
        new_fail_days = consecutive_fail_days + 1

    if currently_eligible:
        new_eligible = new_fail_days < HYSTERESIS_EXIT_AFTER_CONSECUTIVE_FAILS
    else:
        new_eligible = new_pass_days >= HYSTERESIS_ENTER_AFTER_CONSECUTIVE_PASSES

    return new_eligible, new_pass_days, new_fail_days
