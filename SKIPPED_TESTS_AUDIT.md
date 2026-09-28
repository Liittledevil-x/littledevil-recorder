# Skipped Tests Audit

## Summary
All 3 skipped tests require `DATABASE_URL` environment variable to connect to a real Postgres instance. They are legitimate Postgres integration tests, not blocked on missing code.

---

## Test 1: `test_data_health.py::test_new_channel_starts_ok`

**Why skipped:** `DATABASE_URL` not set
```python
pytestmark = pytest.mark.skipif(not DATABASE_URL, reason="DATABASE_URL not set")
```

**What it does:** 
- Connects to a real Postgres database
- Creates a `DataHealthTracker`
- Records a message on a new channel
- Verifies the channel is persisted with status='ok' and gap_started_at=NULL

**Dependency required:** Postgres database with `data_health` table

**Is dependency currently available?** 
- Local development: Yes, `littledevil-local-postgres-1` Docker container available
- CI environment: No, network-isolated

**Can skip be removed?** Yes, by setting `DATABASE_URL=postgresql://littledevil:littledevil@localhost:5433/littledevil_demoexec_test` in the test environment

**Is skip still justified?** 
- Justified in CI (network-isolated, no Postgres)
- Not justified for local interactive runs (Postgres available)

---

## Test 2: `test_data_health.py::test_sweep_marks_stale_then_suspended_and_recovery_clears_gap`

**Why skipped:** `DATABASE_URL` not set (same file as Test 1)

**What it does:**
- Connects to real Postgres
- Simulates a data health gap via backdating a channel's `last_message_at`
- Verifies `sweep()` marks the channel 'stale' then 'suspended' based on elapsed time
- Verifies recovery (receiving a message) clears the gap

**Dependency required:** Postgres database with `data_health` table

**Is dependency currently available?** Same as Test 1

**Can skip be removed?** Yes, same environment variable as Test 1

**Is skip still justified?** Same as Test 1

---

## Test 3: `test_universe_store.py::test_record_membership_upserts_and_is_idempotent`

**Why skipped:** `DATABASE_URL` not set

**What it does:**
- Connects to real Postgres
- Creates a test universe membership record
- Verifies idempotency (upsert does not duplicate on repeated calls)
- Verifies the record is queryable with correct field values

**Dependency required:** Postgres database with `universe_membership` table

**Is dependency currently available?** Same as Test 1

**Can skip be removed?** Yes, same environment variable as Test 1

**Is skip still justified?** Same as Test 1

---

## Recommendation

All 3 skips are **appropriate for CI** (network-isolated environments) but can be **run locally** by setting:
```bash
export DATABASE_URL=postgresql://littledevil:littledevil@localhost:5433/littledevil_demoexec_test
uv run pytest tests/ -q
```

This will run all 126 tests (currently 123 passed + 3 skipped).

No code changes needed to make these tests runnable. The skip is intentional and conditional on environment.
