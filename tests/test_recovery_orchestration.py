"""Tests for recovery orchestration from Data Health gaps."""

from littledevil_recorder.recovery_orchestration import recovery_capability


def test_recovery_classifies_positioning_channels():
    """Positioning channels are composite; orchestration classifies them correctly."""
    assert recovery_capability("positioning") == "mixed"
    assert recovery_capability("open_interest") == "coarse"
    assert recovery_capability("funding") == "coarse"
    assert recovery_capability("mark_index") == "coarse"
    assert recovery_capability("basis") == "coarse"
    assert recovery_capability("liquidation") == "unrecoverable"
    assert recovery_capability("trades") is None  # not a positioning channel
    assert recovery_capability("unknown") is None
