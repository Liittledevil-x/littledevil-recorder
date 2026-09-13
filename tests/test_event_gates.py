from datetime import UTC, datetime, timedelta

from littledevil_recorder.event_gates import (
    Announcement,
    ScheduledEvent,
    UnscheduledGate,
    check_scheduled_gate,
)


def test_scheduled_gate_fires_before_event():
    fomc = ScheduledEvent(name="FOMC", at=datetime(2026, 9, 17, 18, 0, tzinfo=UTC), source="fed.gov")
    result = check_scheduled_gate(
        [fomc], as_of=datetime(2026, 9, 17, 17, 30, tzinfo=UTC)
    )
    assert result.gated is True
    assert result.reason == "FOMC"


def test_scheduled_gate_fires_briefly_after_event():
    fomc = ScheduledEvent(name="FOMC", at=datetime(2026, 9, 17, 18, 0, tzinfo=UTC), source="fed.gov")
    result = check_scheduled_gate(
        [fomc], as_of=datetime(2026, 9, 17, 18, 10, tzinfo=UTC)
    )
    assert result.gated is True


def test_scheduled_gate_clear_well_outside_window():
    fomc = ScheduledEvent(name="FOMC", at=datetime(2026, 9, 17, 18, 0, tzinfo=UTC), source="fed.gov")
    result = check_scheduled_gate(
        [fomc], as_of=datetime(2026, 9, 17, 12, 0, tzinfo=UTC)
    )
    assert result.gated is False
    assert result.reason is None


def test_unscheduled_gate_flags_matching_symbol_only():
    gate = UnscheduledGate()
    ann = Announcement(
        symbol="SOLUSDT",
        title="Scheduled maintenance",
        published_at=datetime.now(UTC),
        source_url="https://binance.com/announcement/123",
    )
    newly_active = gate.ingest([ann])
    assert newly_active == [ann]
    assert gate.is_gated("SOLUSDT").gated is True
    assert gate.is_gated("SOLUSDT").source == "https://binance.com/announcement/123"
    assert gate.is_gated("BTCUSDT").gated is False


def test_unscheduled_gate_venue_wide_gates_every_symbol():
    gate = UnscheduledGate()
    ann = Announcement(
        symbol=None,
        title="Venue status: degraded",
        published_at=datetime.now(UTC),
        source_url="https://binance.com/status",
    )
    gate.ingest([ann])
    assert gate.is_gated("BTCUSDT").gated is True
    assert gate.is_gated("SOLUSDT").gated is True


def test_unscheduled_gate_does_not_reactivate_already_active_announcement():
    gate = UnscheduledGate()
    ann = Announcement(
        symbol="SOLUSDT", title="X", published_at=datetime.now(UTC), source_url="https://x"
    )
    first = gate.ingest([ann])
    second = gate.ingest([ann])
    assert first == [ann]
    assert second == []


def test_unscheduled_gate_clear_removes_it():
    gate = UnscheduledGate()
    ann = Announcement(
        symbol="SOLUSDT", title="X", published_at=datetime.now(UTC), source_url="https://x"
    )
    gate.ingest([ann])
    assert gate.clear("SOLUSDT") is True
    assert gate.is_gated("SOLUSDT").gated is False
    assert gate.clear("SOLUSDT") is False  # already cleared
