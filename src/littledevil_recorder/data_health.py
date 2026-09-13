"""Per-channel freshness tracking -> data_health (docs/data-and-events.md §1).

A channel is 'ok' while messages keep arriving inside its freshness window,
'stale' once that window is missed, and 'suspended' once staleness persists
past the suspend threshold. Every transition is written immediately -- a
gap is logged, never silently dropped (docs/CLAUDE.md: "Recording never
pauses, for any reason" -- a paused *channel* still gets recorded as a gap,
the *service* keeps running).
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime

import psycopg

STALE_AFTER_SECONDS = 30.0
SUSPENDED_AFTER_SECONDS = 120.0


@dataclass
class ChannelHealth:
    channel: str
    last_message_at: datetime
    status: str = "ok"
    gap_started_at: datetime | None = None


class DataHealthTracker:
    """In-memory channel state, flushed to Postgres on every change and on
    a periodic sweep (so staleness is detected even between messages)."""

    def __init__(self, conn: psycopg.AsyncConnection) -> None:
        self._conn = conn
        self._channels: dict[str, ChannelHealth] = {}
        self._lock = asyncio.Lock()

    async def record_message(self, channel: str) -> None:
        async with self._lock:
            now = datetime.now(UTC)
            state = self._channels.get(channel)
            if state is None:
                state = ChannelHealth(channel=channel, last_message_at=now)
                self._channels[channel] = state
            else:
                state.last_message_at = now
                state.status = "ok"
                state.gap_started_at = None
            await self._upsert(state)

    async def sweep(self) -> None:
        """Call periodically (e.g. every few seconds) to catch channels
        that have gone quiet between messages -- staleness is a function of
        elapsed time, not just of messages that do arrive."""
        async with self._lock:
            now = datetime.now(UTC)
            for state in self._channels.values():
                elapsed = (now - state.last_message_at).total_seconds()
                new_status = (
                    "suspended"
                    if elapsed >= SUSPENDED_AFTER_SECONDS
                    else "stale"
                    if elapsed >= STALE_AFTER_SECONDS
                    else "ok"
                )
                if new_status != state.status:
                    if state.status == "ok" and new_status != "ok":
                        state.gap_started_at = state.last_message_at
                    state.status = new_status
                    await self._upsert(state)

    async def _upsert(self, state: ChannelHealth) -> None:
        async with self._conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO data_health (channel, last_message_at, gap_started_at, status)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (channel) DO UPDATE SET
                    last_message_at = EXCLUDED.last_message_at,
                    gap_started_at = EXCLUDED.gap_started_at,
                    status = EXCLUDED.status
                """,
                (state.channel, state.last_message_at, state.gap_started_at, state.status),
            )

    def register(self, channel: str) -> None:
        if channel not in self._channels:
            self._channels[channel] = ChannelHealth(
                channel=channel, last_message_at=datetime.now(UTC)
            )
