"""Event Gates: scheduled calendar gate + unscheduled announcement gate
(docs/architecture-review.md §10.11). Both are deterministic, no model
call -- they emit a flag and a source link; the human reads the news, the
model never does.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass
class ScheduledEvent:
    name: str
    at: datetime
    source: str


@dataclass
class GateResult:
    gated: bool
    reason: str | None
    source: str | None


def check_scheduled_gate(
    events: list[ScheduledEvent], *, as_of: datetime, minutes_before: int = 60, minutes_after: int = 15
) -> GateResult:
    """D4's scheduled-calendar half (architecture-review.md §4.2/§10.11):
    gates trading within `minutes_before` of a known high-impact event and
    for a short window after, since the event's immediate aftermath is
    still unpriced noise."""
    for event in events:
        window_start = event.at - timedelta(minutes=minutes_before)
        window_end = event.at + timedelta(minutes=minutes_after)
        if window_start <= as_of <= window_end:
            return GateResult(gated=True, reason=event.name, source=event.source)
    return GateResult(gated=False, reason=None, source=None)


@dataclass
class Announcement:
    symbol: str | None  # None = venue-wide (e.g. exchange status)
    title: str
    published_at: datetime
    source_url: str


class UnscheduledGate:
    """Polls official sources (exchange announcement feeds, venue status
    APIs) every 1-5 minutes (architecture-review.md §10.11) and closes D4
    for a matched symbol -- or the whole venue -- until cleared manually.
    No announcement text ever reaches a model; only this flag + source_url.
    """

    def __init__(self) -> None:
        self._active: dict[str | None, Announcement] = {}

    def ingest(self, announcements: list[Announcement]) -> list[Announcement]:
        """Records any new announcement as an active gate. Returns the
        newly-activated ones (for logging/notification)."""
        newly_active = []
        for ann in announcements:
            key = ann.symbol
            if key not in self._active:
                self._active[key] = ann
                newly_active.append(ann)
        return newly_active

    def is_gated(self, symbol: str) -> GateResult:
        if None in self._active:  # venue-wide gate
            ann = self._active[None]
            return GateResult(gated=True, reason=ann.title, source=ann.source_url)
        if symbol in self._active:
            ann = self._active[symbol]
            return GateResult(gated=True, reason=ann.title, source=ann.source_url)
        return GateResult(gated=False, reason=None, source=None)

    def clear(self, symbol: str | None) -> bool:
        """Manual clear -- returns True if something was actually cleared."""
        return self._active.pop(symbol, None) is not None
