"""One-shot dispatch to Voximplant; ambiguous responses must never trigger redial."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import httpx

if TYPE_CHECKING:
    from app.config import Settings


START_SCENARIOS_URL = "https://api.voximplant.com/platform_api/StartScenarios/"
MAX_CUSTOM_DATA_BYTES = 199

# Only errors whose documented meaning proves a pre-execution rejection.
# Internal/media/billing-server errors, SESSION_TIMEOUT, TRY_LATER and unknown
# codes are intentionally excluded. The HTTP 5xx check takes precedence.
# https://docs.voximplant.ai/api-reference/management-api/errors
_REJECTION_CODES = frozenset({100, 101, 102, 103, 104, 115, 116, 117, 127, 131, 132, 134, 137, 147, 156})


class DispatchRejected(Exception):
    """The request was not sent or the API explicitly rejected it before execution."""


class DispatchUncertain(Exception):
    """The scenario may have started. Check provider history; never retry blindly."""


class VoximplantProvider:
    """Launch a single routing-rule session, without redirects or retries.

    An injected AsyncClient is owned by its caller and must not contain a retrying
    transport. This adapter does not log requests, responses, or upstream errors.
    The provider_session_id is the call history ID, not an authenticated URL.
    """

    def __init__(self, settings: Settings, client: httpx.AsyncClient | None = None):
        self._settings = settings
        self._owns_client = client is None
        self._client = (
            client
            if client is not None
            else httpx.AsyncClient(
                timeout=httpx.Timeout(15.0, connect=5.0),
                follow_redirects=False,
                trust_env=False,
                transport=httpx.AsyncHTTPTransport(retries=0, trust_env=False),
            )
        )
        self._closed = False

    async def start(self, job: dict[str, Any]) -> dict[str, str | None]:
        if self._closed:
            raise DispatchRejected("Provider is closed.")
        if getattr(self._settings, "mode", None) != "live":
            raise DispatchRejected("Live dispatch is disabled.")

        account_id = self._positive_id(getattr(self._settings, "voximplant_account_id", None))
        rule_id = self._positive_id(getattr(self._settings, "voximplant_rule_id", None))
        api_key = getattr(self._settings, "voximplant_api_key", None)
        if account_id is None or rule_id is None or not isinstance(api_key, str) or not api_key.strip():
            raise DispatchRejected("Voximplant dispatch credentials are incomplete.")

        job_id = job.get("id")
        dispatch_token = job.get("dispatch_token")
        if not isinstance(job_id, str) or not job_id or not isinstance(dispatch_token, str) or not dispatch_token:
            raise DispatchRejected("Dispatch identity is missing.")
        custom_data = json.dumps(
            {"job_id": job_id, "dispatch_token": dispatch_token},
            separators=(",", ":"),
            ensure_ascii=True,
        )
        if len(custom_data.encode("utf-8")) > MAX_CUSTOM_DATA_BYTES:
            raise DispatchRejected("Dispatch identity exceeds the custom data limit.")

        try:
            response = await self._client.post(
                START_SCENARIOS_URL,
                data={
                    "account_id": account_id,
                    "api_key": api_key,
                    "rule_id": rule_id,
                    "script_custom_data": custom_data,
                },
                headers={"Accept": "application/json"},
                timeout=httpx.Timeout(15.0, connect=5.0),
                follow_redirects=False,
            )
        except httpx.HTTPError:
            # A timeout can occur after Voximplant has already accepted the POST.
            # Suppress the upstream exception, which may contain credentials.
            raise DispatchUncertain("Voximplant response was not received; check call history.") from None

        if response.status_code >= 500 or 300 <= response.status_code < 400:
            raise DispatchUncertain("Voximplant returned an inconclusive HTTP response; check call history.")
        try:
            body = response.json()
        except (ValueError, UnicodeError):
            raise DispatchUncertain("Voximplant returned an unreadable response; check call history.") from None
        if not isinstance(body, dict):
            raise DispatchUncertain("Voximplant returned an unexpected response; check call history.")

        result = body.get("result")
        has_success = type(result) is int and result == 1
        error = body.get("error")
        has_session = any(
            key in body
            for key in ("call_session_history_id", "media_session_access_url", "media_session_access_secure_url")
        )
        if error is not None:
            code = error.get("code") if isinstance(error, dict) else None
            if (
                type(code) is int
                and code in _REJECTION_CODES
                and not has_success
                and not has_session
                and (200 <= response.status_code < 300 or 400 <= response.status_code < 500)
            ):
                raise DispatchRejected(f"Voximplant rejected the request (API code {code}).")
            raise DispatchUncertain("Voximplant could not confirm dispatch; check call history.")

        if not 200 <= response.status_code < 300 or not has_success:
            raise DispatchUncertain("Voximplant did not confirm dispatch; check call history.")

        session_id = body.get("call_session_history_id")
        if session_id is not None:
            session_id = self._positive_id(session_id)
            if session_id is None:
                raise DispatchUncertain("Voximplant returned an invalid history ID; check call history.")
        return {"provider_session_id": session_id}

    @staticmethod
    def _positive_id(value: Any) -> str | None:
        if type(value) is int and value > 0:
            return str(value)
        if isinstance(value, str) and value.isascii() and value.isdecimal() and 0 < len(value) <= 30 and int(value) > 0:
            return value
        return None

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._owns_client:
            await self._client.aclose()
