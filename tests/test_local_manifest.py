import json

from littledevil_recorder.local_manifest import (
    quarantine_file,
    read_flush_failure_log,
    read_quarantine_manifest,
    record_flush_failure,
    record_process_start,
)


def test_record_process_start_starts_at_one(tmp_path):
    heartbeat = record_process_start(tmp_path, pid=111)
    assert heartbeat.restart_count == 1
    assert heartbeat.first_started_at == heartbeat.last_started_at
    assert heartbeat.last_pid == 111


def test_record_process_start_increments_across_calls_and_preserves_first_seen(tmp_path):
    first = record_process_start(tmp_path, pid=111)
    second = record_process_start(tmp_path, pid=222)
    third = record_process_start(tmp_path, pid=333)

    assert third.restart_count == 3
    assert third.first_started_at == first.first_started_at
    assert third.last_pid == 333
    assert second.restart_count == 2


def test_record_process_start_persists_to_disk_atomically(tmp_path):
    record_process_start(tmp_path, pid=1)
    record_process_start(tmp_path, pid=2)

    path = tmp_path / "_local" / "restart_heartbeat.json"
    assert path.exists()
    on_disk = json.loads(path.read_text())
    assert on_disk["restart_count"] == 2
    assert on_disk["last_pid"] == 2
    # no leftover .tmp file after an atomic write
    assert not (tmp_path / "_local" / "restart_heartbeat.json.tmp").exists()


def test_quarantine_file_renames_rather_than_deletes(tmp_path):
    src_dir = tmp_path / "depth" / "BTCUSDT"
    src_dir.mkdir(parents=True)
    src = src_dir / "2026-09-15.parquet"
    src.write_bytes(b"corrupt")

    dest = quarantine_file(
        tmp_path, kind="depth", symbol="BTCUSDT", day="2026-09-15",
        reason="4-byte file, crash-mid-write",
    )

    assert dest is not None
    assert not src.exists()
    assert dest.exists()
    assert dest.read_bytes() == b"corrupt"
    assert "quarantined" in dest.name


def test_quarantine_file_returns_none_when_source_missing(tmp_path):
    dest = quarantine_file(
        tmp_path, kind="depth", symbol="ETHUSDT", day="2026-09-14", reason="missing"
    )
    assert dest is None


def test_quarantine_file_records_a_queryable_manifest_entry(tmp_path):
    src_dir = tmp_path / "depth" / "ETHUSDT"
    src_dir.mkdir(parents=True)
    (src_dir / "2026-09-14.parquet").write_bytes(b"\x00\x00\x00\x00")

    quarantine_file(
        tmp_path, kind="depth", symbol="ETHUSDT", day="2026-09-14",
        reason="invalid magic bytes",
    )

    entries = read_quarantine_manifest(tmp_path)
    assert len(entries) == 1
    assert entries[0]["symbol"] == "ETHUSDT"
    assert entries[0]["day"] == "2026-09-14"
    assert entries[0]["reason"] == "invalid magic bytes"
    assert entries[0]["kind"] == "depth"


def test_quarantine_manifest_accumulates_multiple_entries(tmp_path):
    for kind, symbol, day in [
        ("depth", "BTCUSDT", "2026-09-15"),
        ("depth", "ETHUSDT", "2026-09-14"),
    ]:
        d = tmp_path / kind / symbol
        d.mkdir(parents=True)
        (d / f"{day}.parquet").write_bytes(b"bad")
        quarantine_file(tmp_path, kind=kind, symbol=symbol, day=day, reason="corrupt")

    entries = read_quarantine_manifest(tmp_path)
    assert len(entries) == 2
    assert {(e["symbol"], e["day"]) for e in entries} == {
        ("BTCUSDT", "2026-09-15"),
        ("ETHUSDT", "2026-09-14"),
    }


def test_read_quarantine_manifest_returns_empty_list_when_no_manifest_exists(tmp_path):
    assert read_quarantine_manifest(tmp_path) == []


def test_record_flush_failure_writes_a_queryable_entry(tmp_path):
    record_flush_failure(
        tmp_path, kind="depth", symbol="ETHUSDT", day="2026-09-18",
        error="ArrowInvalid: smaller than the minimum file footer",
    )

    entries = read_flush_failure_log(tmp_path)
    assert len(entries) == 1
    assert entries[0]["kind"] == "depth"
    assert entries[0]["symbol"] == "ETHUSDT"
    assert entries[0]["day"] == "2026-09-18"
    assert "footer" in entries[0]["error"]
    assert entries[0]["failed_at"]


def test_record_flush_failure_appends_one_entry_per_call_without_deduplicating(tmp_path):
    # A key that fails on every cycle (e.g. a still-unquarantined corrupt
    # file) is expected to append one entry per cycle -- how long and how
    # often it has been failing is itself useful signal, not noise to hide.
    for _ in range(3):
        record_flush_failure(
            tmp_path, kind="depth", symbol="ETHUSDT", day="2026-09-18", error="ArrowInvalid"
        )

    assert len(read_flush_failure_log(tmp_path)) == 3


def test_read_flush_failure_log_returns_empty_list_when_no_log_exists(tmp_path):
    assert read_flush_failure_log(tmp_path) == []
