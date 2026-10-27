from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import os
import stat
from threading import Barrier

import pytest

from app import store as module
from app.store import CLAIM_TTL_SECONDS, Store


@pytest.fixture
def clock(monkeypatch):
    clock = [datetime(2026, 10, 27, 9, 0, tzinfo=timezone.utc)]
    monkeypatch.setattr(module, "utcnow", lambda: clock[0])
    return clock


@pytest.fixture
def store(tmp_path, clock):
    result = Store(tmp_path / "calls.sqlite3")
    result.initialize()
    return result


def contact(store, suffix="01", timezone="Europe/Moscow"):
    return store.create_contact(
        "+799900000" + suffix, "Тест", "user requested service call", "2026-10-20T10:00:00Z", timezone
    )


def reserve(store, **kwargs):
    return store.reserve_next(
        **({"max_concurrent": 1, "daily_limit": 20, "window_start": 9, "window_end": 20} | kwargs)
    )


def running(store, *, mode="mock", suffix="01"):
    client = contact(store, suffix)
    job = store.enqueue(client["id"], f"request-{suffix}", mode=mode)
    store.set_paused(False)
    reserved = reserve(store, mode=mode)
    assert reserved["id"] == job["id"]
    return reserved


def test_queue_starts_paused_and_idempotency_does_not_create_second_job(store):
    person = contact(store)
    job = store.enqueue(person["id"], "same-key")
    assert store.control() == {"paused": True}
    assert reserve(store) is None
    assert store.enqueue(person["id"], "same-key")["id"] == job["id"]
    with pytest.raises(ValueError, match="pending"):
        store.enqueue(person["id"], "different-key")
    assert store.stats()["jobs"] == 1
    assert "dispatch_token" not in job


def test_idempotency_key_must_not_be_reused_for_another_payload(store):
    first, second = contact(store, "01"), contact(store, "02")
    store.enqueue(first["id"], "shared-key", mode="mock")
    for args in (
        (second["id"], None, "mock"),
        (first["id"], None, "live"),
        (first["id"], "2026-10-28T00:00:00Z", "mock"),
    ):
        with pytest.raises(ValueError, match="different request"):
            store.enqueue(args[0], "shared-key", args[1], mode=args[2])


def test_concurrent_operator_requests_allow_only_one_pending_job(store):
    person = contact(store)
    barrier = Barrier(2)

    def enqueue(key):
        barrier.wait()
        try:
            return store.enqueue(person["id"], key)
        except ValueError:
            return None

    with ThreadPoolExecutor(max_workers=2) as executor:
        jobs = list(executor.map(enqueue, ["one", "two"]))
    assert sum(job is not None for job in jobs) == 1


def test_concurrent_reservations_cannot_exceed_one_slot(store):
    for suffix in ("01", "02"):
        person = contact(store, suffix)
        store.enqueue(person["id"], suffix)
    store.set_paused(False)
    barrier = Barrier(2)

    def attempt(_):
        barrier.wait()
        return reserve(store)

    with ThreadPoolExecutor(max_workers=2) as executor:
        jobs = list(executor.map(attempt, range(2)))
    assert sum(job is not None for job in jobs) == 1
    assert store.stats()["active"] == 1
    assert store.stats()["attempts_today"] == 1


def test_concurrent_reservations_honor_daily_budget_even_with_free_slots(store):
    for suffix in ("01", "02"):
        store.enqueue(contact(store, suffix)["id"], suffix)
    store.set_paused(False)
    barrier = Barrier(2)

    def attempt(_):
        barrier.wait()
        return reserve(store, max_concurrent=2, daily_limit=1)

    with ThreadPoolExecutor(max_workers=2) as executor:
        jobs = list(executor.map(attempt, range(2)))
    assert sum(job is not None for job in jobs) == 1


def test_daily_budget_counts_failed_attempts_and_resets_at_utc_day(store, clock):
    first = running(store)
    store.mark_dispatch_failed(first["id"], "explicit_rejection")
    second = store.enqueue(contact(store, "02")["id"], "second")
    assert reserve(store, daily_limit=1) is None
    clock[0] += timedelta(days=1)
    assert reserve(store, daily_limit=1)["id"] == second["id"]
    assert store.stats()["attempts_today"] == 1
    assert store.stats()["budget_timezone"] == "UTC"


def test_per_contact_timezone_and_not_before_are_enforced(store, clock):
    # 09:00 UTC is noon Moscow, 02:00 Los Angeles: only Moscow may ring.
    la = store.enqueue(contact(store, "01", "America/Los_Angeles")["id"], "la")
    moscow = store.enqueue(contact(store, "02")["id"], "moscow", "2026-10-27T09:30:00Z")
    store.set_paused(False)
    assert reserve(store) is None
    clock[0] += timedelta(minutes=30)
    assert reserve(store)["id"] == moscow["id"]
    assert store.get_job(la["id"])["status"] == "queued"


def test_window_uses_actual_dst_offset_and_end_is_exclusive(store, clock):
    person = contact(store, "01", "America/New_York")
    job = store.enqueue(person["id"], "dst")
    store.set_paused(False)
    # DST has ended: New York 08:59 is UTC 13:59, not 12:59.
    clock[0] = datetime(2026, 11, 2, 13, 59, tzinfo=timezone.utc)
    assert reserve(store) is None
    clock[0] += timedelta(minutes=1)
    assert reserve(store)["id"] == job["id"]


def test_claim_rechecks_calling_window_at_boundary(store, clock):
    clock[0] = datetime(2026, 10, 27, 16, 59, 59, tzinfo=timezone.utc)
    job = running(store)
    clock[0] += timedelta(seconds=2)
    with pytest.raises(ValueError, match="window has closed"):
        store.claim(job["id"], job["dispatch_token"])
    assert store.get_job(job["id"])["status"] == "cancelled"


def test_dispatch_tokens_hidden_and_claim_is_exactly_once_under_race(store):
    job = running(store)
    assert store.verify_dispatch_token(job["id"], job["dispatch_token"])
    assert not store.verify_dispatch_token(job["id"], "incorrect")
    assert not store.verify_dispatch_token(job["id"], "я" * 40)
    with pytest.raises(ValueError, match="Invalid dispatch token"):
        store.claim(job["id"], "я" * 40)
    assert "dispatch_token" not in store.get_job(job["id"])
    assert "dispatch_token" not in store.list_jobs()[0]
    barrier = Barrier(2)

    def attempt(_):
        barrier.wait()
        try:
            return store.claim(job["id"], job["dispatch_token"])
        except ValueError:
            return None

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(attempt, range(2)))
    assert sum(result is not None for result in results) == 1
    assert store.get_job(job["id"])["claimed"] is True


@pytest.mark.parametrize("action", ["pause", "stop", "suppress"])
def test_claim_cannot_dial_after_intervening_operator_action(store, action):
    job = running(store)
    if action == "pause":
        store.set_paused(True)
    elif action == "stop":
        store.request_stop(job["id"])
    else:
        store.suppress(job["phone"], "withdrawn")
    with pytest.raises(ValueError):
        store.claim(job["id"], job["dispatch_token"])
    assert store.poll_control(job["id"]) == {"stop": True}
    assert store.get_job(job["id"])["claimed"] is False


def test_expired_claim_persists_unknown_and_pause_instead_of_retry(store, clock):
    job = running(store)
    clock[0] += timedelta(seconds=CLAIM_TTL_SECONDS)
    with pytest.raises(ValueError, match="expired"):
        store.claim(job["id"], job["dispatch_token"])
    assert store.get_job(job["id"])["status"] == "unknown"
    assert store.control()["paused"]
    with pytest.raises(ValueError, match="Resolve unknown"):
        store.set_paused(False)


def test_dnc_cancels_queued_and_blocks_new_attempts(store):
    person = contact(store)
    job = store.enqueue(person["id"], "first")
    store.suppress(person["phone"], "user_request")
    assert store.get_job(job["id"])["status"] == "cancelled"
    assert store.list_contacts()[0]["suppressed"]
    with pytest.raises(ValueError, match="do-not-call"):
        store.enqueue(person["id"], "second")
    store.set_paused(False)
    assert reserve(store) is None


def test_opt_out_is_durable_before_final_and_not_overwritten_by_interest(store):
    job = running(store)
    store.claim(job["id"], job["dispatch_token"])
    store.event(job["id"], "out", "opt_out", {"reason": "user_request", "outcome": "not_interested"})
    assert store.list_suppressions()[0]["phone"] == job["phone"]
    assert store.poll_control(job["id"])["stop"]
    final = store.event(job["id"], "end", "ended", {"status": "completed", "outcome": "interested"})
    assert final["outcome"] == "not_interested"


def test_events_are_idempotent_monotonic_and_preserve_terminal_result(store):
    job = running(store)
    job_id = job["id"]
    store.event(job_id, "answer", "answered", {})
    assert store.event(job_id, "late-ring", "ringing", {})["status"] == "answered"
    store.mark_dispatched(job_id, "history-1")
    assert store.get_job(job_id)["status"] == "answered"
    payload = {
        "status": "completed",
        "outcome": "interested",
        "duration_sec": 8,
        "transcript": [{"role": "assistant", "text": "Test"}],
        "transcript_truncated": True,
    }
    first = store.event(job_id, "finish", "ended", payload)
    assert store.event(job_id, "finish", "ended", payload) == first
    with pytest.raises(ValueError, match="reused"):
        store.event(job_id, "finish", "ended", {"status": "failed"})
    store.event(job_id, "late-answer", "answered", {})
    store.event(job_id, "late-failure", "ended", {"status": "failed", "outcome": "not_interested"})
    store.mark_dispatch_failed(job_id, "late_api_timeout", uncertain=True)
    result = store.get_job(job_id)
    assert result["status"] == "completed"
    assert result["outcome"] == "interested"
    assert result["transcript"] == payload["transcript"]
    assert result["transcript_truncated"] is True


def test_heartbeat_saves_tool_outcome_and_unknown_final_does_not_erase_it(store):
    job = running(store)
    store.event(job["id"], "save", "heartbeat", {"outcome": "callback"})
    assert store.get_job(job["id"])["outcome"] == "callback"
    final = store.event(job["id"], "final", "ended", {"status": "completed", "outcome": "unknown"})
    assert final["outcome"] == "callback"


def test_missing_final_event_uses_hard_age_even_with_heartbeats(store, clock):
    job = running(store)
    store.event(job["id"], "answer", "answered", {})
    clock[0] += timedelta(seconds=299)
    store.event(job["id"], "alive", "heartbeat", {})
    assert store.mark_unknown_stale(300) == 0
    clock[0] += timedelta(seconds=1)
    assert store.mark_unknown_stale(300) == 1
    assert store.get_job(job["id"])["status"] == "unknown"
    assert store.control()["paused"] is True
    assert store.poll_control(job["id"])["stop"] is True
    late = store.event(job["id"], "late-end", "ended", {"status": "completed", "conversation_id": "conv-proof"})
    assert late["status"] == "unknown"
    assert late["provider_final_status"] == "completed"
    assert late["conversation_id"] == "conv-proof"
    with pytest.raises(ValueError):
        store.set_paused(False)
    with pytest.raises(ValueError):
        store.resolve_unknown(job["id"], "queued", "try again")
    resolved = store.resolve_unknown(job["id"], "completed", "Operator checked provider history")
    assert resolved["status"] == "completed"
    assert store.control()["paused"]  # explicit separate resume required
    store.set_paused(False)
    assert reserve(store) is None  # old job was never automatically requeued


def test_mode_filter_prevents_old_mock_jobs_becoming_live_calls(store):
    mock = store.enqueue(contact(store, "01")["id"], "mock", mode="mock")
    live = store.enqueue(contact(store, "02")["id"], "live", mode="live")
    store.set_paused(False)
    assert reserve(store, mode="live")["id"] == live["id"]
    assert store.get_job(mock["id"])["status"] == "queued"


@pytest.mark.parametrize(
    "changes",
    [
        {"consent_source": ""},
        {"consent_at": "2030-01-01T00:00:00Z"},
        {"consent_at": "2026-10-20T10:00:00"},
        {"timezone": "Not/AZone"},
        {"phone": "123"},
    ],
)
def test_contact_validation_rejects_unproven_or_invalid_inputs(store, changes):
    data = {
        "phone": "+79990000001",
        "name": "Test",
        "consent_source": "form checkbox",
        "consent_at": "2026-10-20T10:00:00Z",
        "timezone": "Europe/Moscow",
    } | changes
    with pytest.raises(ValueError):
        store.create_contact(**data)


def test_memory_store_is_shared_between_operations_and_reinitialization_is_safe(clock):
    store = Store(":memory:")
    store.initialize()
    person = contact(store)
    store.initialize()
    assert store.list_contacts()[0]["id"] == person["id"]


def test_pending_guard_releases_only_after_terminal_outcome(store):
    job = running(store)
    with pytest.raises(ValueError):
        store.enqueue(job["contact_id"], "duplicate-click")
    store.event(job["id"], "ended", "ended", {"status": "busy"})
    next_job = store.enqueue(job["contact_id"], "explicit-new-operator-request")
    assert next_job["status"] == "queued"


def test_pause_stop_is_latched_even_after_immediate_resume(store):
    job = running(store)
    store.set_paused(True)
    store.set_paused(False)
    assert store.poll_control(job["id"])["stop"]
    with pytest.raises(ValueError, match="stopped"):
        store.claim(job["id"], job["dispatch_token"])


def test_uncertain_dispatch_after_claim_still_requires_manual_review(store):
    job = running(store)
    store.claim(job["id"], job["dispatch_token"])
    store.event(job["id"], "ring", "ringing", {})
    store.mark_dispatch_failed(job["id"], "response_lost", uncertain=True)
    assert store.get_job(job["id"])["status"] == "unknown"
    with pytest.raises(ValueError):
        store.set_paused(False)


def test_failed_opt_out_persistence_is_recovered_atomically_from_final(store):
    job = running(store)
    final = store.event(
        job["id"],
        "fallback",
        "ended",
        {"status": "failed", "error_code": "opt_out_persistence_failed", "outcome": "unknown"},
    )
    assert final["status"] == "failed"
    assert final["outcome"] == "not_interested"
    assert store.list_suppressions()[0]["phone"] == job["phone"]
    assert store.control()["paused"]
    with pytest.raises(ValueError, match="do-not-call"):
        store.enqueue(job["contact_id"], "should-not-ring")


@pytest.mark.skipif(os.name != "posix", reason="POSIX file mode semantics")
def test_database_and_new_directories_are_private_without_chmod_existing_parents(tmp_path, clock):
    shared = tmp_path / "existing-parent"
    shared.mkdir(mode=0o755)
    shared.chmod(0o755)
    db_path = shared / "new-private-parent" / "nested" / "calls.sqlite3"
    previous_umask = os.umask(0)
    try:
        store = Store(db_path)
        store.initialize()
        assert stat.S_IMODE(db_path.stat().st_mode) == 0o600
        assert stat.S_IMODE(db_path.parent.stat().st_mode) == 0o700
        assert stat.S_IMODE(db_path.parent.parent.stat().st_mode) == 0o700
        assert stat.S_IMODE(shared.stat().st_mode) == 0o755
        # Existing application databases are tightened on startup as well.
        db_path.chmod(0o644)
        store.initialize()
        assert stat.S_IMODE(db_path.stat().st_mode) == 0o600
        # Hold a read transaction open so WAL/SHM remain observable.
        with store._read() as connection:
            connection.execute("BEGIN")
            connection.execute("SELECT * FROM jobs").fetchall()
            contact(store)
            for suffix in ("-wal", "-shm"):
                sidecar = db_path.with_name(db_path.name + suffix)
                assert sidecar.exists()
                assert stat.S_IMODE(sidecar.stat().st_mode) == 0o600
    finally:
        os.umask(previous_umask)
