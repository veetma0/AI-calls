import asyncio
import json
from types import SimpleNamespace
from urllib.parse import parse_qs

import httpx
import pytest

from app.providers import DispatchRejected, DispatchUncertain, VoximplantProvider


SECRET = "never-print-provider-key"
TOKEN = "never-print-dispatch-token"


def settings(**overrides):
    values = dict(mode="live", voximplant_account_id="123", voximplant_api_key=SECRET, voximplant_rule_id="456")
    values.update(overrides)
    return SimpleNamespace(**values)


def job(**overrides):
    values = dict(id="job-123", dispatch_token=TOKEN, phone="+79990000000", product_facts="private product details")
    values.update(overrides)
    return values


def test_form_request_contains_only_dispatch_identity_and_returns_history_id():
    async def run():
        requests = []

        def respond(request):
            requests.append(request)
            assert request.method == "POST"
            assert str(request.url) == "https://api.voximplant.com/platform_api/StartScenarios/"
            assert not request.url.query
            assert request.headers["content-type"].startswith("application/x-www-form-urlencoded")
            form = parse_qs(request.content.decode())
            assert form == {
                "account_id": ["123"],
                "api_key": [SECRET],
                "rule_id": ["456"],
                "script_custom_data": [
                    json.dumps({"job_id": "job-123", "dispatch_token": TOKEN}, separators=(",", ":"))
                ],
            }
            assert len(form["script_custom_data"][0].encode()) < 200
            assert "+79990000000" not in request.content.decode()
            assert "private product details" not in request.content.decode()
            return httpx.Response(
                200,
                json={
                    "result": 1,
                    "call_session_history_id": 789,
                    "media_session_access_url": "http://127.0.0.1/private?token=do-not-follow",
                },
            )

        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            provider = VoximplantProvider(settings(), client)
            assert await provider.start(job()) == {"provider_session_id": "789"}
            await provider.close()
            assert not client.is_closed  # The caller owns an injected client.
        assert len(requests) == 1

    asyncio.run(run())


@pytest.mark.parametrize("body", [{"result": 1}, {"result": 1, "call_session_history_id": "789"}])
def test_documented_success_can_omit_history_id(body):
    async def run():
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda request: httpx.Response(200, json=body))
        ) as client:
            result = await VoximplantProvider(settings(), client).start(job())
            assert result == {"provider_session_id": body.get("call_session_history_id")}

    asyncio.run(run())


@pytest.mark.parametrize("status,code", [(200, 100), (403, 100), (200, 147), (200, 127), (400, 101)])
def test_provable_api_rejection_is_sanitized_and_not_retried(status, code, caplog):
    async def run():
        count = 0

        def respond(request):
            nonlocal count
            count += 1
            return httpx.Response(status, json={"error": {"code": code, "msg": f"{SECRET} {TOKEN}"}})

        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            with pytest.raises(DispatchRejected) as caught:
                await VoximplantProvider(settings(), client).start(job())
            assert SECRET not in str(caught.value)
            assert TOKEN not in str(caught.value)
        assert count == 1

    asyncio.run(run())
    assert SECRET not in caplog.text
    assert TOKEN not in caplog.text


@pytest.mark.parametrize(
    "status,body",
    [
        (502, {"error": {"code": 147}}),
        (500, {"result": 1}),
        (200, {"error": {"code": 2, "msg": SECRET}}),
        (200, {"error": {"code": 135}}),
        (200, {"error": {"code": 162}}),
        (200, {"error": {"code": 999999}}),
        (200, {"error": {"code": "100"}}),
        (200, {"result": 1, "error": {"code": 147}}),
        (200, {"call_session_history_id": 789, "error": {"code": 147}}),
        (200, {"media_session_access_url": "https://example.test/private", "error": {"code": 147}}),
        (200, {"result": True}),
        (200, {"result": "1"}),
        (200, {"result": 0}),
        (200, {}),
        (200, []),
        (200, None),
        (200, {"result": 1, "call_session_history_id": SECRET}),
        (200, {"result": 1, "call_session_history_id": False}),
        (401, {"message": SECRET}),
        (429, {}),
    ],
)
def test_uncertain_responses_never_retry_or_echo_upstream_data(status, body):
    async def run():
        count = 0

        def respond(request):
            nonlocal count
            count += 1
            return httpx.Response(status, json=body)

        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            with pytest.raises(DispatchUncertain) as caught:
                await VoximplantProvider(settings(), client).start(job())
            assert SECRET not in str(caught.value)
            assert TOKEN not in str(caught.value)
        assert count == 1

    asyncio.run(run())


@pytest.mark.parametrize(
    "exception", [httpx.ConnectError, httpx.ReadTimeout, httpx.WriteTimeout, httpx.RemoteProtocolError]
)
def test_transport_failure_is_uncertain_even_when_timeout_may_follow_acceptance(exception):
    async def run():
        count = 0

        def respond(request):
            nonlocal count
            count += 1
            raise exception(f"{SECRET} {TOKEN}", request=request)

        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            with pytest.raises(DispatchUncertain) as caught:
                await VoximplantProvider(settings(), client).start(job())
            assert SECRET not in str(caught.value)
            assert TOKEN not in str(caught.value)
            assert caught.value.__suppress_context__
        assert count == 1

    asyncio.run(run())


def test_redirect_is_not_followed_even_if_injected_client_follows_redirects():
    async def run():
        requests = []

        def respond(request):
            requests.append(request)
            return httpx.Response(307, headers={"Location": "https://other.example/private"})

        async with httpx.AsyncClient(transport=httpx.MockTransport(respond), follow_redirects=True) as client:
            with pytest.raises(DispatchUncertain):
                await VoximplantProvider(settings(), client).start(job())
        assert len(requests) == 1

    asyncio.run(run())


def test_malformed_body_is_uncertain():
    async def run():
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda request: httpx.Response(200, text=f"not JSON {SECRET}"))
        ) as client:
            with pytest.raises(DispatchUncertain) as caught:
                await VoximplantProvider(settings(), client).start(job())
            assert SECRET not in str(caught.value)

    asyncio.run(run())


@pytest.mark.parametrize(
    "overrides,job_overrides",
    [
        ({"mode": "mock"}, {}),
        ({"voximplant_api_key": ""}, {}),
        ({"voximplant_account_id": "invalid"}, {}),
        ({"voximplant_rule_id": 0}, {}),
        ({}, {"id": None}),
        ({}, {"dispatch_token": ""}),
        ({}, {"dispatch_token": "x" * 200}),
        ({}, {"id": "😀" * 30}),
    ],
)
def test_local_invalid_dispatch_makes_no_network_request(overrides, job_overrides):
    async def run():
        def must_not_call(request):
            pytest.fail("Invalid or mock dispatch must not contact Voximplant")

        async with httpx.AsyncClient(transport=httpx.MockTransport(must_not_call)) as client:
            with pytest.raises(DispatchRejected):
                await VoximplantProvider(settings(**overrides), client).start(job(**job_overrides))

    asyncio.run(run())


def test_provider_closes_owned_client_once_and_rejects_later_dispatch():
    async def run():
        provider = VoximplantProvider(settings())
        await provider.close()
        await provider.close()
        assert provider._client.is_closed
        with pytest.raises(DispatchRejected):
            await provider.start(job())

    asyncio.run(run())
