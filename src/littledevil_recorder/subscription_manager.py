"""Dynamic subscription manager: runtime add/remove of recorder symbols and
channels without a process restart (build-plan.md Trunk prep; the recorder
must support the daily recording-set rotation `scanner-attention-routing.md`
§2 describes -- trades for the whole universe, depth for a 25-pair set
rotated daily with hysteresis -- without ever dropping to zero coverage for
an unrelated symbol while one channel is being changed).

Desired state is the single source of truth this module owns: which symbols
should be streamed, per channel ("trades", "depth"). It is persisted
atomically to disk (same pattern as local_manifest.py) so a restart resumes
the same desired state rather than falling back to a static env var list --
restart-safe by construction, since main.py reads it back on startup instead
of re-deriving symbols from LITTLEDEVIL_TRADE_SYMBOLS/LITTLEDEVIL_DEPTH_SYMBOLS
every time.

This module never decides *which* symbols belong in the universe or the
recording set -- that is Universe Refresh's job (universe.py,
universe_store.py) and, later, the ranking layer's. It only tracks and
persists "these are the symbols currently wanted" and exposes an idempotent
add/remove surface plus a diff against what is currently running, so the
caller (main.py) can reconcile actual stream tasks to desired state. No
Detector or ranking dependency.
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path

DESIRED_SUBSCRIPTIONS_FILENAME = "desired_subscriptions.json"

CHANNELS = ("trades", "depth", "positioning", "liquidation")


def _local_dir(data_root: Path) -> Path:
    d = data_root / "_local"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _write_json_atomic(path: Path, payload: dict) -> None:
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, indent=2, sort_keys=True))
    os.replace(tmp_path, path)


@dataclass
class SymbolChannelState:
    """Explicit per-symbol/channel state, independent of the underlying
    websocket connection's own status -- this is what a subscriber asked
    for and whether it is currently believed satisfied, not a raw
    connection flag."""

    symbol: str
    channel: str
    desired_since: str
    status: str = "pending"  # pending | subscribed | failed | removed
    last_error: str | None = None
    updated_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())


class SubscriptionManager:
    """Owns desired-state symbol sets per channel, persisted atomically.

    Every mutation (add/remove) is idempotent: adding an already-desired
    symbol or removing an already-absent one is a no-op that still returns
    a consistent result rather than raising. `diff_from(running)` is the
    read side a caller uses to figure out what actually needs to change on
    the wire -- this class never touches a websocket itself.
    """

    def __init__(self, data_root: Path) -> None:
        self._root = data_root
        self._path = _local_dir(data_root) / DESIRED_SUBSCRIPTIONS_FILENAME
        self._desired: dict[str, set[str]] = {c: set() for c in CHANNELS}
        self._states: dict[tuple[str, str], SymbolChannelState] = {}
        self._load()

    def _load(self) -> None:
        if not self._path.exists():
            return
        payload = json.loads(self._path.read_text())
        for channel in CHANNELS:
            for entry in payload.get(channel, []):
                symbol = entry["symbol"].upper()
                self._desired[channel].add(symbol)
                self._states[(channel, symbol)] = SymbolChannelState(
                    symbol=symbol,
                    channel=channel,
                    desired_since=entry.get("desired_since", datetime.now(UTC).isoformat()),
                    status=entry.get("status", "pending"),
                    last_error=entry.get("last_error"),
                    updated_at=entry.get("updated_at", datetime.now(UTC).isoformat()),
                )

    def _persist(self) -> None:
        payload = {
            channel: [asdict(self._states[(channel, symbol)]) for symbol in sorted(self._desired[channel])]
            for channel in CHANNELS
        }
        _write_json_atomic(self._path, payload)

    def desired_symbols(self, channel: str) -> list[str]:
        _validate_channel(channel)
        return sorted(self._desired[channel])

    def add_symbols(self, channel: str, symbols: list[str]) -> list[str]:
        """Idempotent: adds every symbol not already desired. Returns the
        symbols actually newly added (already-desired symbols are skipped,
        not re-added or re-timestamped)."""
        _validate_channel(channel)
        now = datetime.now(UTC).isoformat()
        added: list[str] = []
        for raw in symbols:
            symbol = raw.upper()
            if symbol in self._desired[channel]:
                continue
            self._desired[channel].add(symbol)
            self._states[(channel, symbol)] = SymbolChannelState(
                symbol=symbol, channel=channel, desired_since=now, status="pending", updated_at=now,
            )
            added.append(symbol)
        if added:
            self._persist()
        return added

    def remove_symbols(self, channel: str, symbols: list[str]) -> list[str]:
        """Idempotent: removing a symbol not currently desired is a no-op
        for that symbol. Returns the symbols actually removed."""
        _validate_channel(channel)
        removed: list[str] = []
        for raw in symbols:
            symbol = raw.upper()
            if symbol not in self._desired[channel]:
                continue
            self._desired[channel].discard(symbol)
            self._states.pop((channel, symbol), None)
            removed.append(symbol)
        if removed:
            self._persist()
        return removed

    def mark_subscribed(self, channel: str, symbols: list[str]) -> None:
        self._set_status(channel, symbols, "subscribed", error=None)

    def mark_failed(self, channel: str, symbols: list[str], error: str) -> None:
        self._set_status(channel, symbols, "failed", error=error)

    def _set_status(self, channel: str, symbols: list[str], status: str, *, error: str | None) -> None:
        _validate_channel(channel)
        now = datetime.now(UTC).isoformat()
        changed = False
        for raw in symbols:
            symbol = raw.upper()
            state = self._states.get((channel, symbol))
            if state is None or symbol not in self._desired[channel]:
                continue  # removed since the caller started subscribing; ignore stale result
            state.status = status
            state.last_error = error
            state.updated_at = now
            changed = True
        if changed:
            self._persist()

    def states(self, channel: str) -> list[SymbolChannelState]:
        _validate_channel(channel)
        return [self._states[(channel, symbol)] for symbol in sorted(self._desired[channel])]

    def diff_from(self, channel: str, running: list[str]) -> "SubscriptionDiff":
        """Compares desired state against `running` (the symbol list a
        stream task is currently actually connected with) and returns what
        must be added/removed to reconcile -- the read side main.py polls."""
        _validate_channel(channel)
        desired = self._desired[channel]
        running_set = {s.upper() for s in running}
        return SubscriptionDiff(
            channel=channel,
            to_add=sorted(desired - running_set),
            to_remove=sorted(running_set - desired),
        )


@dataclass
class SubscriptionDiff:
    channel: str
    to_add: list[str]
    to_remove: list[str]

    @property
    def changed(self) -> bool:
        return bool(self.to_add or self.to_remove)


def _validate_channel(channel: str) -> None:
    if channel not in CHANNELS:
        raise ValueError(f"unknown channel {channel!r}; expected one of {CHANNELS}")
