import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app import store as store_module
from app import worker as worker_module
from app.providers import DispatchRejected, DispatchUncertain
from app.store import Store
from app.worker import Worker


@pytest.fixture
def setup(tmp_path, monkeypatch):
    clock = [datetime(2026, 10, 27, 9, tzinfo=timezone.utc), 100.0]
    monkeypatch.setattr(store_module, "utcnow", lambda: clock[0])
    monkeypatch.setattr(worker_module.time, "monotonic", lambda: clock[1])
    store = Store(tmp_path / "worker.sqlite3")
    store.initialize()
    settings = SimpleNamespace(
        mode="mock",
        max_concurrent=1,
        daily_attempt_limit=20,
        max_duration_sec=180,
        ringing_timeout_sec=30,
        call_window_start=9,
        call_window_end=20,
        live_allowed_numbers=("+79990000001", "+79990000002"),
        allow_all_consented_numbers=False,
        worker_interval_sec=0.1,
        validate_live=lambda: None,
    )
    return store, settings, clock


def enqueue(store, suffix="01", mode="mock"):
    person = store.create_contact(
        "+799900000" + suffix, "Demo", "callback requested", "2026-10-20T00:00:00Z", "Europe/Moscow"
    )
    return store.enqueue(person["id"], "request-" + suffix, mode=mode)


def provider(*, side_effect=None, result=None):
    return SimpleNamespace(
        start=AsyncMock(side_effect=side_effect, return_value=result or {"provider_session_id": "history-123"}),
        close=AsyncMock(),
    )


def test_mock_is_entirely_local_and_never_claims_customer_interest(setup, monkeypatch):
    store, settings, clock = setup
    monkeypatch.setattr(
        worker_module, "VoximplantProvider", lambda _: pytest.fail("Mock constructed a network provider")
    )
    job = enqueue(store)
    worker = Worker(store, settings)

    async def scenario():
        await worker.tick()
        assert store.get_job(job["id"])["status"] == "queued"
        store.set_paused(False)
        await worker.tick()
        assert store.get_job(job["id"])["status"] == "ringing"
        clock[1] += 1
        await worker.tick()
        assert store.get_job(job["id"])["status"] == "answered"
        clock[1] += 2
        await worker.tick()
        final = store.get_job(job["id"])
        assert final["status"] == "completed"
        assert final["outcome"] == "unknown"
        assert final["provider_session_id"].startswith("mock-")
        assert "синтетический" in final["transcript"][0]["text"].lower()
        await worker.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("action", ["pause", "stop", "suppress"])
def test_mock_stops_safely_on_control_or_dnc(setup, action):
    store, settings, clock = setup
    job = enqueue(store)
    worker = Worker(store, settings)

    async def scenario():
        store.set_paused(False)
        await worker.tick()
        if action == "pause":
            store.set_paused(True)
        elif action == "stop":
            store.request_stop(job["id"])
        else:
            store.suppress(job["phone"], "requested")
        await worker.tick()
        assert store.get_job(job["id"])["status"] == "cancelled"
        assert store.stats()["attempts_today"] == 1
        await worker.close()

    asyncio.run(scenario())


def test_live_success_dispatches_once_and_waits_for_provider_events(setup):
    store, settings, clock = setup
    settings.mode = "live"
    job = enqueue(store, mode="live")
    fake = provider()
    worker = Worker(store, settings, fake)

    async def scenario():
        store.set_paused(False)
        await worker.tick()
        await worker.tick()
        assert fake.start.await_count == 1
        assert store.get_job(job["id"])["status"] == "starting"
        assert store.get_job(job["id"])["provider_session_id"] == "history-123"
        await worker.close()
        await worker.close()
        assert fake.close.await_count == 1

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "error", [DispatchUncertain("upstream contains a secret"), RuntimeError("sensitive unexpected failure")]
)
def test_ambiguous_dispatch_pauses_and_is_never_retried(setup, error, caplog):
    store, settings, clock = setup
    settings.mode = "live"
    job = enqueue(store, mode="live")
    queued = enqueue(store, "02", mode="live")
    fake = provider(side_effect=error)
    worker = Worker(store, settings, fake)

    async def scenario():
        store.set_paused(False)
        await worker.tick()
        await worker.tick()
        assert fake.start.await_count == 1
        assert store.get_job(job["id"])["status"] == "unknown"
        assert store.get_job(queued["id"])["status"] == "queued"
        assert store.control()["paused"]
        assert "secret" not in caplog.text
        assert "sensitive" not in caplog.text
        await worker.close()

    asyncio.run(scenario())


def test_explicit_rejection_fails_once_without_requeue(setup):
    store, settings, clock = setup
    settings.mode = "live"
    job = enqueue(store, mode="live")
    fake = provider(side_effect=DispatchRejected("bad credentials"))
    worker = Worker(store, settings, fake)

    async def scenario():
        store.set_paused(False)
        await worker.tick()
        await worker.tick()
        assert fake.start.await_count == 1
        assert store.get_job(job["id"])["status"] == "failed"
        await worker.close()

    asyncio.run(scenario())


def test_final_webhook_can_win_race_with_startscenarios_response(setup):
    store, settings, clock = setup
    settings.mode = "live"
    job = enqueue(store, mode="live")

    async def quick_call(reserved):
        store.claim(reserved["id"], reserved["dispatch_token"])
        store.event(reserved["id"], "end-fast", "ended", {"status": "busy"})
        return {"provider_session_id": "late-history"}

    fake = provider(side_effect=quick_call)
    worker = Worker(store, settings, fake)

    async def scenario():
        store.set_paused(False)
        await worker.tick()
        result = store.get_job(job["id"])
        assert result["status"] == "busy"
        assert result["provider_session_id"] == "late-history"
        await worker.close()

    asyncio.run(scenario())


def test_missing_final_event_becomes_unknown_even_after_restart(setup):
    store, settings, clock = setup
    settings.mode = "live"
    job = enqueue(store, mode="live")
    fake = provider()
    worker = Worker(store, settings, fake)

    async def scenario():
        store.set_paused(False)
        await worker.tick()
        await worker.close()
        clock[0] += timedelta(seconds=331)
        second_fake = provider()
        restarted = Worker(store, settings, second_fake)
        await restarted.tick()
        assert store.get_job(job["id"])["status"] == "unknown"
        assert store.control()["paused"]
        assert second_fake.start.await_count == 0
        await restarted.close()

    asyncio.run(scenario())


def test_worker_enforces_live_allowlist_before_network(setup):
    store, settings, clock = setup
    settings.mode = "live"
    settings.live_allowed_numbers = ("+79990000002",)
    job = enqueue(store, mode="live")
    fake = provider()
    worker = Worker(store, settings, fake)

    async def scenario():
        store.set_paused(False)
        await worker.tick()
        assert fake.start.await_count == 0
        assert store.get_job(job["id"])["reason"] == "number_not_allowlisted"
        await worker.close()

    asyncio.run(scenario())


def test_cancelling_inflight_dispatch_is_uncertain_not_requeued(setup):
    store, settings, clock = setup
    settings.mode = "live"
    job = enqueue(store, mode="live")

    async def scenario():
        entered = asyncio.Event()
        never_finishes = asyncio.Event()

        async def start(_):
            entered.set()
            await never_finishes.wait()

        fake = provider(side_effect=start)
        worker = Worker(store, settings, fake)
        store.set_paused(False)
        task = asyncio.create_task(worker.tick())
        await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert store.get_job(job["id"])["status"] == "unknown"
        assert store.control()["paused"]
        await worker.tick()
        assert fake.start.await_count == 1
        await worker.close()

    asyncio.run(scenario())


def test_worker_validates_live_configuration_again_and_pauses(setup):
    store, settings, clock = setup
    settings.mode = "live"
    job = enqueue(store, mode="live")

    def invalid():
        raise ValueError("missing settings")

    settings.validate_live = invalid
    fake = provider()
    worker = Worker(store, settings, fake)

    async def scenario():
        store.set_paused(False)
        await worker.tick()
        assert store.control()["paused"]
        assert store.get_job(job["id"])["status"] == "queued"
        assert fake.start.await_count == 0
        await worker.close()

    asyncio.run(scenario())
