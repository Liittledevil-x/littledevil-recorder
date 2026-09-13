from littledevil_recorder.universe import (
    EligibilityCheck,
    SymbolMetrics,
    apply_hysteresis,
    check_eligibility,
)


def _passing_metrics(**overrides) -> SymbolMetrics:
    defaults = dict(
        symbol="BTCUSDT",
        listing_age_days=2000,
        median_quote_volume_30d_usd=1_000_000_000,
        median_spread_bps=1.0,
        p95_spread_bps=3.0,
        median_depth_25bps_usd=5_000_000,
        median_trades_per_minute=500,
        coingecko_rank=1,
        category=None,
        feed_health_7d_pct=99.9,
    )
    defaults.update(overrides)
    return SymbolMetrics(**defaults)


def test_symbol_passing_every_criterion_is_eligible():
    result = check_eligibility(_passing_metrics())
    assert result.eligible is True
    assert result.reasons_failed == []


def test_new_listing_fails_age_floor():
    result = check_eligibility(_passing_metrics(listing_age_days=30))
    assert result.eligible is False
    assert any("listing_age_days" in r for r in result.reasons_failed)


def test_stablecoin_category_excluded():
    result = check_eligibility(_passing_metrics(category="stablecoin"))
    assert result.eligible is False
    assert "excluded_category=stablecoin" in result.reasons_failed


def test_low_liquidity_fails_multiple_criteria_simultaneously():
    result = check_eligibility(
        _passing_metrics(
            median_quote_volume_30d_usd=100,
            median_depth_25bps_usd=100,
            median_trades_per_minute=1,
        )
    )
    assert result.eligible is False
    assert len(result.reasons_failed) == 3


def test_delisting_announcement_fails_regardless_of_other_metrics():
    result = check_eligibility(_passing_metrics(has_delisting_announcement=True))
    assert result.eligible is False


def test_criteria_snapshot_always_includes_reasons_failed_key():
    result = check_eligibility(_passing_metrics(coingecko_rank=None))
    assert "reasons_failed" in result.criteria_snapshot
    assert result.criteria_snapshot["reasons_failed"] == result.reasons_failed


# --- hysteresis ---


def test_ineligible_pair_does_not_enter_before_three_consecutive_passes():
    eligible, pass_days, fail_days = False, 0, 0
    for day in range(1, 3):
        eligible, pass_days, fail_days = apply_hysteresis(
            currently_eligible=eligible,
            consecutive_pass_days=pass_days,
            consecutive_fail_days=fail_days,
            passed_today=True,
        )
        assert eligible is False, f"should not enter on day {day}"
    eligible, pass_days, fail_days = apply_hysteresis(
        currently_eligible=eligible,
        consecutive_pass_days=pass_days,
        consecutive_fail_days=fail_days,
        passed_today=True,
    )
    assert eligible is True  # 3rd consecutive pass


def test_eligible_pair_survives_up_to_four_consecutive_fails():
    eligible, pass_days, fail_days = True, 10, 0
    for day in range(1, 5):
        eligible, pass_days, fail_days = apply_hysteresis(
            currently_eligible=eligible,
            consecutive_pass_days=pass_days,
            consecutive_fail_days=fail_days,
            passed_today=False,
        )
        assert eligible is True, f"should still be eligible after {day} fails"
    eligible, pass_days, fail_days = apply_hysteresis(
        currently_eligible=eligible,
        consecutive_pass_days=pass_days,
        consecutive_fail_days=fail_days,
        passed_today=False,
    )
    assert eligible is False  # 5th consecutive fail


def test_a_single_pass_resets_consecutive_fail_count():
    eligible, pass_days, fail_days = True, 0, 3
    eligible, pass_days, fail_days = apply_hysteresis(
        currently_eligible=eligible,
        consecutive_pass_days=pass_days,
        consecutive_fail_days=fail_days,
        passed_today=True,
    )
    assert fail_days == 0
    assert eligible is True
