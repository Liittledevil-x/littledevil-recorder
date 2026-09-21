"""Local, file-based records that this repo owns directly, as opposed to
the shared Postgres schema in docs/data-and-events.md (owned by the
littledevil-docs submodule, not editable from here per this repo's
guardrails):

- restart_heartbeat: how many times this process has (re)started, and when.
  data_health's `status` only reflects current channel freshness, not
  restart history, so a process stuck in a crash loop looks identical to a
  healthy one to anything only reading data_health -- confirmed during the
  2026-09-16 production incident, where status stayed 'ok' throughout 474
  restarts. This gives a Data Health worker or a simple watchdog something
  to alarm on.

- quarantine manifest: permanent, queryable record of Parquet files pulled
  out of the recording set because they are corrupted beyond repair (e.g.
  crash-mid-write). Quarantining renames the file rather than deleting it,
  and this manifest is the durable record of *why* a (symbol, day) has no
  data -- so a gap doesn't just silently look like "recording was off" to
  anyone auditing later.

- flush failure log: append-only, queryable record of any flush_all() call
  that raised (storage.FlushAllError). Added after a 2026-09-18/09-19
  incident where a pre-existing corrupt file caused periodic_flush's
  unguarded writer.flush_all() call to raise, which silently killed that
  asyncio task for the rest of the process's life with *no log line and no
  record anywhere* -- write buffers then grew unbounded until the
  MemoryMax cgroup limit OOM-killed the process, roughly 7 hours later,
  twice in a row. periodic_flush now catches FlushAllError and keeps
  running (see main.py), but a caught-and-logged exception can still
  scroll out of journalctl's retention -- this file is the durable,
  independently-queryable trail so "flushing silently failed for key X
  since time Y" is a fact a watchdog or ops script can check without
  needing to keep the process's stdout/journal forever.

Both are plain JSON files under data_root/_local/, written atomically
(write to a .tmp path, then os.replace) so a crash mid-write never corrupts
the manifest itself.
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

RESTART_HEARTBEAT_FILENAME = "restart_heartbeat.json"
QUARANTINE_MANIFEST_FILENAME = "quarantine_manifest.json"
FLUSH_FAILURE_LOG_FILENAME = "flush_failure_log.json"
COMPACTION_FAILURE_LOG_FILENAME = "compaction_failure_log.json"


def _local_dir(data_root: Path) -> Path:
    d = data_root / "_local"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _write_json_atomic(path: Path, payload: dict) -> None:
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, indent=2, default=str))
    os.replace(tmp_path, path)


@dataclass
class RestartHeartbeat:
    restart_count: int
    first_started_at: str
    last_started_at: str
    last_pid: int


def record_process_start(data_root: Path, *, pid: int | None = None) -> RestartHeartbeat:
    """Call once, early in startup. Increments the on-disk restart count and
    records this start's timestamp -- a monotonically increasing counter
    that a watchdog can alarm on (e.g. "more than N restarts in the last
    hour"), independent of and complementary to data_health's point-in-time
    channel status."""
    path = _local_dir(data_root) / RESTART_HEARTBEAT_FILENAME
    now = datetime.now(UTC).isoformat()
    pid = pid if pid is not None else os.getpid()

    if path.exists():
        existing = json.loads(path.read_text())
        heartbeat = RestartHeartbeat(
            restart_count=existing["restart_count"] + 1,
            first_started_at=existing["first_started_at"],
            last_started_at=now,
            last_pid=pid,
        )
    else:
        heartbeat = RestartHeartbeat(
            restart_count=1,
            first_started_at=now,
            last_started_at=now,
            last_pid=pid,
        )

    _write_json_atomic(path, asdict(heartbeat))
    return heartbeat


@dataclass
class QuarantineEntry:
    kind: str
    symbol: str
    day: str
    quarantined_at: str
    quarantined_path: str
    reason: str


def quarantine_file(data_root: Path, *, kind: str, symbol: str, day: str, reason: str) -> Path | None:
    """Renames a legacy day-file or a partitioned day-directory out of the
    recording set, with a timestamped ``.quarantined`` suffix, and records a
    permanent entry in the quarantine manifest. Returns the new path, or
    None if neither representation existed.

    Never deletes -- quarantine is "this data is known-bad and excluded",
    not "this data never happened"; the manifest is the queryable record of
    the resulting gap for anything auditing recording continuity."""
    now = datetime.now(UTC)
    src = data_root / kind / symbol / f"{day}.parquet"
    if src.exists():
        dest = src.with_name(f"{src.stem}.quarantined-{now.strftime('%Y%m%dT%H%M%SZ')}{src.suffix}")
        os.replace(src, dest)
    else:
        # New append parts use a date directory.  Rename it out of the data
        # set and mark every active manifest part quarantined so transition
        # readers cannot accidentally rediscover it.
        from littledevil_recorder.storage import quarantine_part_day

        dest = quarantine_part_day(data_root, kind, symbol, day, now.strftime('%Y%m%dT%H%M%SZ'))
        if dest is None:
            return None

    manifest_path = _local_dir(data_root) / QUARANTINE_MANIFEST_FILENAME
    entries = json.loads(manifest_path.read_text())["entries"] if manifest_path.exists() else []
    entries.append(
        asdict(
            QuarantineEntry(
                kind=kind,
                symbol=symbol,
                day=day,
                quarantined_at=now.isoformat(),
                quarantined_path=str(dest),
                reason=reason,
            )
        )
    )
    _write_json_atomic(manifest_path, {"entries": entries})
    return dest


def read_quarantine_manifest(data_root: Path) -> list[dict]:
    manifest_path = _local_dir(data_root) / QUARANTINE_MANIFEST_FILENAME
    if not manifest_path.exists():
        return []
    return json.loads(manifest_path.read_text())["entries"]


@dataclass
class FlushFailureEntry:
    kind: str
    symbol: str
    day: str
    failed_at: str
    error: str


def record_flush_failure(
    data_root: Path, *, kind: str, symbol: str, day: str, error: str
) -> None:
    """Appends one entry to the flush failure log. Called from periodic_flush
    (and the startup recovery flush) whenever a key fails to flush, so the
    failure has a durable, independently-queryable trail even though the
    flush loop itself now keeps running past it (see this module's
    docstring). Not deduplicated -- a key that fails on every cycle (e.g. a
    still-unquarantined corrupt file) is expected to append one entry per
    cycle, which is itself useful signal (how long has this been failing,
    and how often)."""
    log_path = _local_dir(data_root) / FLUSH_FAILURE_LOG_FILENAME
    entries = json.loads(log_path.read_text())["entries"] if log_path.exists() else []
    entries.append(
        asdict(
            FlushFailureEntry(
                kind=kind,
                symbol=symbol,
                day=day,
                failed_at=datetime.now(UTC).isoformat(),
                error=error,
            )
        )
    )
    _write_json_atomic(log_path, {"entries": entries})


def read_flush_failure_log(data_root: Path) -> list[dict]:
    log_path = _local_dir(data_root) / FLUSH_FAILURE_LOG_FILENAME
    if not log_path.exists():
        return []
    return json.loads(log_path.read_text())["entries"]


def record_compaction_failure(
    data_root: Path, *, kind: str, symbol: str, day: str, error: str
) -> None:
    """Durably records an isolated background-compaction failure.  Live
    ingestion deliberately continues; this makes that fact observable."""
    log_path = _local_dir(data_root) / COMPACTION_FAILURE_LOG_FILENAME
    entries = json.loads(log_path.read_text())["entries"] if log_path.exists() else []
    entries.append({"kind": kind, "symbol": symbol, "day": day,
                    "failed_at": datetime.now(UTC).isoformat(), "error": error})
    _write_json_atomic(log_path, {"entries": entries})


def read_compaction_failure_log(data_root: Path) -> list[dict]:
    log_path = _local_dir(data_root) / COMPACTION_FAILURE_LOG_FILENAME
    return json.loads(log_path.read_text())["entries"] if log_path.exists() else []
