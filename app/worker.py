"""Single-process dispatcher, with an entirely local synthetic mock mode."""

from __future__ import annotations

import asyncio
import logging
import time

from .providers import DispatchRejected, DispatchUncertain, VoximplantProvider
from .store import ACTIVE

log = logging.getLogger(__name__)


class Worker:
    def __init__(self, store, settings, provider=None):
        self.store = store
        self.settings = settings
        # Mock mode must never construct a network provider.
        self.provider = (
            provider if provider is not None else (VoximplantProvider(settings) if settings.mode == "live" else None)
        )
        self._tick_lock = asyncio.Lock()
        self._wake = asyncio.Event()
        self._stopping = False
        self._closed = False
        self._mock_calls: dict[str, dict] = {}

    def _event(self, job_id, suffix, event_type, payload):
        return self.store.event(job_id, f"mock:{job_id}:{suffix}", event_type, payload)

    def _progress_mock(self):
        now = time.monotonic()
        for job_id, simulation in list(self._mock_calls.items()):
            job = self.store.get_job(job_id)
            if job is None or job["status"] not in ACTIVE:
                del self._mock_calls[job_id]
                continue
            if self.store.poll_control(job_id)["stop"]:
                self._event(
                    job_id,
                    "stopped",
                    "ended",
                    {
                        "status": "cancelled",
                        "outcome": "unknown",
                        "duration_sec": max(0, int(now - simulation["started"])),
                        "error_code": "mock_stopped",
                        "source": "mock",
                        "transcript": [
                            {
                                "role": "assistant",
                                "text": "Синтетическая демонстрация остановлена. Реальный звонок не выполнялся.",
                            }
                        ],
                    },
                )
                del self._mock_calls[job_id]
                continue
            elapsed = now - simulation["started"]
            if elapsed >= 1 and not simulation["answered"]:
                self._event(job_id, "answered", "answered", {"source": "mock"})
                simulation["answered"] = True
            if elapsed >= 3:
                self._event(
                    job_id,
                    "ended",
                    "ended",
                    {
                        "status": "completed",
                        "outcome": "unknown",
                        "duration_sec": 2,
                        "conversation_id": f"mock-{job_id}",
                        "source": "mock",
                        "transcript": [
                            {
                                "role": "assistant",
                                "text": "Это синтетический тест интерфейса. Телефонный звонок не выполнялся.",
                            },
                            {"role": "user", "text": "Демонстрационная реплика; реальный человек не участвовал."},
                            {
                                "role": "assistant",
                                "text": "Тест завершён. Интерес клиента и качество телефонной связи не проверялись.",
                            },
                        ],
                    },
                )
                del self._mock_calls[job_id]

    async def tick(self):
        async with self._tick_lock:
            if self._stopping:
                return
            stale_age = self.settings.max_duration_sec + self.settings.ringing_timeout_sec + 120
            self.store.mark_unknown_stale(stale_age)
            if self.settings.mode == "mock":
                self._progress_mock()
            elif self.settings.mode == "live":
                try:
                    self.settings.validate_live()
                except ValueError:
                    self.store.set_paused(True)
                    log.warning("Live dispatch configuration is not ready; queue paused")
                    return
            else:
                self.store.set_paused(True)
                raise ValueError("Unknown worker mode")
            job = self.store.reserve_next(
                max_concurrent=self.settings.max_concurrent,
                daily_limit=self.settings.daily_attempt_limit,
                window_start=self.settings.call_window_start,
                window_end=self.settings.call_window_end,
                mode=self.settings.mode,
            )
            if job is None:
                return
            job_id = job["id"]
            # Extra defense if a custom Store implementation fails to filter.
            if job.get("mode") != self.settings.mode:
                self.store.mark_dispatch_failed(job_id, "dispatch_mode_mismatch")
                self.store.set_paused(True)
                return
            if self.settings.mode == "mock":
                try:
                    self.store.claim(job_id, job["dispatch_token"])
                except ValueError:
                    self.store.mark_dispatch_failed(job_id, "mock_claim_rejected")
                    return
                self.store.mark_dispatched(job_id, f"mock-{job_id}")
                self._event(job_id, "ringing", "ringing", {"source": "mock"})
                self._mock_calls[job_id] = {"started": time.monotonic(), "answered": False}
                return
            # Validate the per-number allowlist again immediately before the only
            # network operation. The scenario claim endpoint rechecks consent/DNC.
            if not self.settings.allow_all_consented_numbers and job["phone"] not in self.settings.live_allowed_numbers:
                self.store.mark_dispatch_failed(job_id, "number_not_allowlisted")
                return
            try:
                result = await self.provider.start(job)
            except DispatchRejected:
                self.store.mark_dispatch_failed(job_id, "provider_rejected")
            except DispatchUncertain:
                self.store.mark_dispatch_failed(job_id, "dispatch_uncertain_check_provider", uncertain=True)
            except asyncio.CancelledError:
                # Cancellation does not prove that the provider did not accept
                # the request. Never put it back into the queue.
                self.store.mark_dispatch_failed(job_id, "dispatch_interrupted_check_provider", uncertain=True)
                self.store.set_paused(True)
                raise
            except Exception:
                self.store.mark_dispatch_failed(job_id, "dispatch_error_check_provider", uncertain=True)
                log.warning("Provider dispatch failed unexpectedly; queue paused")
            else:
                # A scenario may have already posted its final result while
                # StartScenarios was in flight. Store preserves that final status.
                self.store.mark_dispatched(job_id, result.get("provider_session_id"))

    async def run(self):
        try:
            while not self._stopping:
                try:
                    await self.tick()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    self.store.set_paused(True)
                    log.error("Call worker failed; queue paused for operator review")
                if self._stopping:
                    break
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout=max(0.05, self.settings.worker_interval_sec))
                    self._wake.clear()
                except asyncio.TimeoutError:
                    pass
        finally:
            self.store.set_paused(True)

    async def stop(self):
        self._stopping = True
        self.store.set_paused(True)
        self._wake.set()
        # Do not cancel a network dispatch and then pretend it never happened.
        # Wait for its bounded provider timeout, after which stop controls remain on.
        async with self._tick_lock:
            if self.settings.mode == "mock":
                self._progress_mock()

    async def close(self):
        if self._closed:
            return
        await self.stop()
        if self.provider is not None:
            await self.provider.close()
        self._closed = True
