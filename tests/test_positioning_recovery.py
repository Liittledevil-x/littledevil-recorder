"""Tests for positioning channel gap recovery (OI/funding/mark-index/basis)."""

from datetime import UTC, datetime, timedelta

import pytest

from littledevil_recorder.positioning_recovery import recovery_capability, RecoveryResult


def test_recovery_capability_classification():
    """Verify channel recoverability classification."""
    assert recovery_capability("positioning") == "mixed"
    assert recovery_capability("open_interest") == "coarse"
    assert recovery_capability("funding") == "coarse"
    assert recovery_capability("mark_index") == "coarse"
    assert recovery_capability("basis") == "coarse"
    assert recovery_capability("liquidation") == "unrecoverable"
    assert recovery_capability("unknown") is None


def test_recovery_result_structure():
    """RecoveryResult captures all necessary metadata."""
    now = datetime.now(UTC)
    gap_start = now - timedelta(hours=1)
    gap_end = now

    result = RecoveryResult(
        channel="positioning",
        symbol="BTCUSDT",
        gap_start=gap_start,
        gap_end=gap_end,
        recovered_count=42,
        resolution="5m_coarse",
        source="openInterestHist",
        errors=[],
    )

    assert result.channel == "positioning"
    assert result.symbol == "BTCUSDT"
    assert result.recovered_count == 42
    assert result.resolution == "5m_coarse"
    assert result.source == "openInterestHist"
    assert len(result.errors) == 0


def test_recovery_result_with_errors():
    """RecoveryResult preserves errors from failed recovery."""
    now = datetime.now(UTC)
    result = RecoveryResult(
        channel="positioning",
        symbol="ETHUSDT",
        gap_start=now,
        gap_end=now,
        recovered_count=0,
        resolution="none",
        source="failed",
        errors=["HTTP 429 from basis history", "Connection timeout"],
    )

    assert len(result.errors) == 2
    assert "HTTP 429" in result.errors[0]
