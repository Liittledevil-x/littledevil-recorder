"""Bounded, coalesced data-health persistence.

Market callbacks only update in-memory channel state.  The existing five
second health-sweep cadence is also the persistence cadence: it bounds the
monitoring lag to five seconds while preserving the 30s stale / 120s
suspended semantics.  A failed database write is logged, retained as dirty
state, and never reaches the market-data callback.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime

import psycopg

logger = logging.getLogger(__name__)

STALE_AFTER_SECONDS = 30.0
SUSPENDED_AFTER_SECONDS = 120.0


@dataclass
class ChannelHealth:
    channel: str
    last_message_at: datetime
    status: str = "ok"
    gap_started_at: datetime | None = None


class DataHealthTracker:
    """In-memory health snapshots with bounded (one row/channel) DB writes."""

    def __init__(self, conn: psycopg.AsyncConnection, *, max_channels: int = 512) -> None:
        self._conn = conn
        self._channels: dict[str, ChannelHealth] = {}
        self._dirty: set[str] = set()
        self._max_channels = max_channels
        self.flush_failures = 0
        self.last_flush_error: str | None = None

    def register(self, channel: str) -> None:
        if channel in self._channels:
            return
        if len(self._channels) >= self._max_channels:
            raise ValueError(f"data-health channel limit ({self._max_channels}) reached")
        self._channels[channel] = ChannelHealth(channel=channel, last_message_at=datetime.now(UTC))
        self._dirty.add(channel)

    def record_message(self, channel: str) -> None:
        """Non-blocking market callback path: no await and no database I/O."""
        self.register(channel)
        state = self._channels[channel]
        state.last_message_at = datetime.now(UTC)
        state.status = "ok"
        state.gap_started_at = None
        self._dirty.add(channel)

    def sweep(self) -> None:
        """Update elapsed-time status; caller persists the coalesced snapshot."""
        now = datetime.now(UTC)
        for state in self._channels.values():
            elapsed = (now - state.last_message_at).total_seconds()
            new_status = "suspended" if elapsed >= SUSPENDED_AFTER_SECONDS else "stale" if elapsed >= STALE_AFTER_SECONDS else "ok"
            if new_status != state.status:
                if state.status == "ok" and new_status != "ok":
                    state.gap_started_at = state.last_message_at
                state.status = new_status
                self._dirty.add(state.channel)

    async def flush(self) -> bool:
        """Best-effort one-statement upsert of all currently dirty channels.

        Values are snapshotted before awaiting the database so a message that
        arrives during I/O remains dirty and is sent with its newer value on
        the next cadence.
        """
        dirty = sorted(self._dirty)
        if not dirty:
            return True
        snapshot = [
            (state.channel, state.last_message_at, state.gap_started_at, state.status)
            for state in (self._channels[channel] for channel in dirty)
        ]
        values = ", ".join(["(%s, %s, %s, %s)"] * len(snapshot))
        params: list[object] = []
        for state in snapshot:
            params.extend(state)
        try:
            async with self._conn.cursor() as cur:
                await cur.execute(
                    f"""
                    INSERT INTO data_health (channel, last_message_at, gap_started_at, status)
                    VALUES {values}
                    ON CONFLICT (channel) DO UPDATE SET
                        last_message_at = EXCLUDED.last_message_at,
                        gap_started_at = EXCLUDED.gap_started_at,
                        status = EXCLUDED.status
                    """,
                    params,
                )
        except Exception as exc:  # DB trouble must never stop ingestion
            self.flush_failures += 1
            self.last_flush_error = repr(exc)
            logger.exception("data_health batch flush failed; retaining %d dirty channel(s)", len(dirty))
            return False
        # Do not clear a channel changed while the await above was in flight.
        for channel, last_message_at, gap_started_at, status in snapshot:
            current = self._channels.get(channel)
            if current and (current.last_message_at, current.gap_started_at, current.status) == (last_message_at, gap_started_at, status):
                self._dirty.discard(channel)
        self.last_flush_error = None
        return True

    @property
    def pending_channels(self) -> int:
        return len(self._dirty)
