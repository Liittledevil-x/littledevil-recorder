"""Quarantines specific known-corrupt Parquet files: renames them out of the
recording set (never deletes) and records the resulting gap in the local
quarantine manifest (littledevil_recorder.local_manifest).

Not run automatically anywhere -- this repo's guardrails forbid touching
the production VM directly from an agent session. Omar runs this by hand
against the recorder's actual LITTLEDEVIL_DATA_ROOT once he's ready to
formally record the two 2026-09-16 incident files as permanent data loss:

    LITTLEDEVIL_DATA_ROOT=/path/to/data python scripts/quarantine_corrupt_files.py

Add more (kind, symbol, day, reason) entries to KNOWN_CORRUPT below for any
future confirmed-corrupt file; this script is idempotent per entry (a
missing source file, e.g. already quarantined, is reported and skipped).
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

from littledevil_recorder.local_manifest import quarantine_file

logger = logging.getLogger(__name__)

# The two files confirmed corrupted during the 2026-09-16 OOM crash-loop
# incident (see dev-journal.md): crash-mid-write left a 4-byte depth file
# for BTCUSDT and an invalid-magic-bytes depth file for ETHUSDT.
KNOWN_CORRUPT = [
    ("depth", "BTCUSDT", "2026-09-15", "4-byte file, crash-mid-write during 2026-09-16 OOM incident"),
    ("depth", "ETHUSDT", "2026-09-14", "invalid magic bytes, crash-mid-write during 2026-09-16 OOM incident"),
    (
        "depth",
        "ETHUSDT",
        "2026-09-18",
        "4-byte file (PAR1 magic only, no footer), crash-mid-write left over from the "
        "pre-fix process; discovered still sitting in the live write path on "
        "2026-09-18, and confirmed to be the root cause of two subsequent OOM "
        "incidents (2026-09-18 21:15:11 UTC, 2026-09-19 04:34:22 UTC): flush_all() "
        "raised on this file inside periodic_flush's unguarded while loop, silently "
        "killing that task and leaving write buffers to grow unbounded until the "
        "MemoryMax cgroup limit killed the process, ~7h later each time. Fixed "
        "separately (storage.flush_all per-key isolation + main._flush_all_logging_failures) "
        "before this file was quarantined.",
    ),
]


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    data_root = Path(os.getenv("LITTLEDEVIL_DATA_ROOT", "./data"))

    for kind, symbol, day, reason in KNOWN_CORRUPT:
        dest = quarantine_file(data_root, kind=kind, symbol=symbol, day=day, reason=reason)
        if dest is None:
            logger.warning(
                "%s/%s/%s: source file not found (already quarantined, or path differs) -- skipped",
                kind, symbol, day,
            )
        else:
            logger.info("%s/%s/%s: quarantined -> %s", kind, symbol, day, dest)


if __name__ == "__main__":
    main()
