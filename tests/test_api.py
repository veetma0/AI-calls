"""HTTP boundary tests. All data is synthetic; the dispatcher never runs."""

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json

from fastapi.testclient import TestClient
import httpx
import pytest

from app.config import Settings
from app.main import create_app


PHONE = "+79161234567"
OTHER_PHONE = "+79161234568"
ADMIN = "synthetic-admin-token-" + "a" * 40
INTERNAL = "synthetic-internal-secret-" + "b" * 40
PROVIDER_KEY = "synthetic-provider-key-never-public"
AUTH = {"Authorization": "Bearer " + ADMIN}
INTERNAL_AUTH = {"X-Internal-Secret": INTERNAL}


@pytest.fixture
def app_factory(tmp_path):
    clients = []
    counter = 0

    def make(*, live=False, product=None, **overrides):
        nonlocal counter
        counter += 1
        product_path = tmp_path / f"product-{counter}.json"
        product_path.write_text(
            json.dumps(
                product
                if product is not None
                else {
                    "name": "Тестовый продукт",
                    "call_goal": "Подтвердить время тестовой встречи",
                    "facts": ["Это синтетические данные для теста"],
                    "ready_for_live": True,
                }
            ),
            encoding="utf-8",
        )
        settings = Settings(
            mode="live" if live else "mock",
            admin_token=ADMIN,
            internal_secret=INTERNAL,
            database_path=str(tmp_path / f"calls-{counter}.sqlite3"),
            product_config_path=str(product_path),
            public_base_url="https://calls.example.test",
            voximplant_account_id="12345",
            voximplant_api_key=PROVIDER_KEY,
            voximplant_rule_id="67890",
            caller_id=PHONE,
            elevenlabs_agent_id="agent_synthetic_test",
            live_allowed_numbers=(PHONE,),
            call_window_start=0,
            call_window_end=24,
        )
        settings = replace(settings, **overrides)
        app = create_app(settings, start_worker=False)
        client = TestClient(app, raise_server_exceptions=False)
        client.__enter__()
        clients.append(client)
        return client

    yield make
    for client in reversed(clients):
        client.__exit__(None, None, None)


@pytest.fixture
def client(app_factory):
    return app_factory()


def contact_payload(phone=PHONE, **overrides):
    return {
        "phone": phone,
        "name": "Тестовый собеседник",
        "consent_source": "Заявка TEST-123",
        "consent_at": (datetime.now(timezone.utc) - timedelta(days=1)).isoformat(),
        "timezone": "Europe/Moscow",
        **overrides,
    }


def create_contact(client, phone=PHONE):
    response = client.post("/api/contacts", headers=AUTH, json=contact_payload(phone))
    assert response.status_code == 201, response.text
    return response.json()


def enqueue(client, contact, key="test-request-001"):
    response = client.post("/api/calls", headers=AUTH, json={"contact_id": contact["id"], "idempotency_key": key})
    assert response.status_code == 201, response.text
    return response.json()


def reserve(client):
    contact = create_contact(client)
    job = enqueue(client, contact)
    response = client.post("/api/queue", headers=AUTH, json={"paused": False})
    assert response.status_code == 200, response.text
    store = client.app.state.store
    reserved = store.reserve_next(1, 20, 0, 24, mode=client.app.state.settings.mode)
    assert reserved["id"] == job["id"]
    return reserved


def event_headers(job):
    return {**INTERNAL_AUTH, "X-Dispatch-Token": job["dispatch_token"]}


@pytest.mark.parametrize(
    "method,path,payload",
    [
        ("GET", "/api/config", None),
        ("GET", "/api/contacts", None),
        ("GET", "/api/calls", None),
        ("GET", "/api/calls/missing", None),
        ("GET", "/api/suppressions", None),
        ("POST", "/api/contacts", contact_payload()),
        ("POST", "/api/calls", {"contact_id": "synthetic", "idempotency_key": "request-1"}),
        ("POST", "/api/queue", {"paused": False}),
        ("POST", "/api/calls/missing/stop", {}),
        ("POST", "/api/calls/missing/resolve", {"status": "failed", "note": "Checked provider log"}),
        ("POST", "/api/suppressions", {"phone": PHONE}),
        ("POST", "/api/contacts/import", None),
    ],
)
def test_admin_routes_require_authentication(client, method, path, payload):
    for headers in ({}, {"Authorization": "Bearer invalid"}, {"Authorization": "Basic " + ADMIN}):
        response = client.request(method, path, headers=headers, json=payload)
        assert response.status_code == 401
        assert ADMIN not in response.text and INTERNAL not in response.text
    assert client.app.state.store.list_contacts() == []


def test_health_is_public_and_discloses_no_configuration(client):
    assert client.get("/health").json() == {"ok": True}
    assert client.get("/docs").status_code == 404
    assert client.get("/openapi.json").status_code == 404


def test_configuration_and_job_responses_do_not_expose_credentials(client):
    job = reserve(client)
    paths = ["/api/config", "/api/contacts", "/api/calls", f"/api/calls/{job['id']}", "/api/suppressions"]
    for path in paths:
        response = client.get(path, headers=AUTH)
        assert response.status_code == 200
        for secret in (ADMIN, INTERNAL, PROVIDER_KEY, job["dispatch_token"]):
            assert secret not in response.text
        assert "dispatch_token" not in response.text
        assert response.headers["Cache-Control"] == "no-store"
        assert response.headers["X-Content-Type-Options"] == "nosniff"


@pytest.mark.parametrize(
    "changes",
    [
        {"phone": "+442079460958"},
        {"phone": "+77011234567"},
        {"phone": "12345"},
        {"consent_source": ""},
        {"consent_at": "2020-01-01T12:00:00"},
        {"consent_at": "2099-01-01T12:00:00Z"},
        {"timezone": "Mars/Olympus"},
        {"unexpected": "field"},
    ],
)
def test_invalid_contacts_never_reach_store(client, changes):
    response = client.post("/api/contacts", headers=AUTH, json=contact_payload(**changes))
    assert response.status_code == 422
    assert client.app.state.store.list_contacts() == []


def test_idempotency_and_suppression_work_through_public_api(client):
    contact = create_contact(client)
    job = enqueue(client, contact)
    assert enqueue(client, contact)["id"] == job["id"]
    assert (
        client.post(
            "/api/calls",
            headers=AUTH,
            json={
                "contact_id": contact["id"],
                "idempotency_key": "different-request",
            },
        ).status_code
        == 409
    )
    response = client.post("/api/suppressions", headers=AUTH, json={"phone": PHONE, "reason": "Просьба не звонить"})
    assert response.status_code == 200
    assert client.get(f"/api/calls/{job['id']}", headers=AUTH).json()["status"] == "cancelled"
    assert (
        client.post(
            "/api/calls",
            headers=AUTH,
            json={
                "contact_id": contact["id"],
                "idempotency_key": "after-suppression",
            },
        ).status_code
        == 409
    )


def test_live_allowlist_is_enforced_before_enqueue(app_factory):
    client = app_factory(live=True)
    contact = create_contact(client, OTHER_PHONE)
    response = client.post(
        "/api/calls", headers=AUTH, json={"contact_id": contact["id"], "idempotency_key": "unlisted-number"}
    )
    assert response.status_code == 409
    assert client.app.state.store.list_jobs() == []


def test_live_product_must_be_explicitly_ready(app_factory):
    client = app_factory(
        live=True,
        product={
            "name": "Недописанный продукт",
            "call_goal": "Подтвердить тестовую встречу",
            "facts": [],
            "ready_for_live": False,
        },
    )
    contact = create_contact(client)
    assert client.post("/api/queue", headers=AUTH, json={"paused": False}).status_code == 409
    assert (
        client.post(
            "/api/calls", headers=AUTH, json={"contact_id": contact["id"], "idempotency_key": "not-ready-call"}
        ).status_code
        == 409
    )
    assert client.app.state.store.control()["paused"]


def test_internal_claim_requires_secret_and_is_one_time(app_factory):
    client = app_factory(live=True)
    job = reserve(client)
    path = f"/internal/calls/{job['id']}/claim"
    body = {"dispatch_token": job["dispatch_token"]}
    for headers in ({}, AUTH, {"X-Internal-Secret": "bad"}):
        assert client.post(path, headers=headers, json=body).status_code == 401
    assert client.post(path, headers=INTERNAL_AUTH, json={"dispatch_token": "x" * 43}).status_code == 409
    response = client.post(path, headers=INTERNAL_AUTH, json=body)
    assert response.status_code == 200
    assert response.json()["phone"] == PHONE
    assert response.json()["dynamic_variables"]["contact_name"] == "Тестовый собеседник"
    assert "dispatch_token" not in response.text and PROVIDER_KEY not in response.text
    assert client.post(path, headers=INTERNAL_AUTH, json=body).status_code == 409


def test_mock_mode_cannot_bootstrap_a_real_call(client):
    job = reserve(client)
    response = client.post(
        f"/internal/calls/{job['id']}/claim", headers=INTERNAL_AUTH, json={"dispatch_token": job["dispatch_token"]}
    )
    assert response.status_code == 409
    assert not client.app.state.store.get_job(job["id"])["claimed"]


@pytest.mark.parametrize("action", ["pause", "stop", "suppress"])
def test_claim_rechecks_stop_conditions(app_factory, action):
    client = app_factory(live=True)
    job = reserve(client)
    if action == "pause":
        client.post("/api/queue", headers=AUTH, json={"paused": True})
    elif action == "stop":
        client.post(f"/api/calls/{job['id']}/stop", headers=AUTH, json={})
    else:
        client.post("/api/suppressions", headers=AUTH, json={"phone": PHONE})
    response = client.post(
        f"/internal/calls/{job['id']}/claim", headers=INTERNAL_AUTH, json={"dispatch_token": job["dispatch_token"]}
    )
    assert response.status_code == 409
    assert not client.app.state.store.get_job(job["id"])["claimed"]


def test_internal_events_and_control_require_both_secrets(client):
    job = reserve(client)
    for headers in (
        {},
        AUTH,
        INTERNAL_AUTH,
        {**INTERNAL_AUTH, "X-Dispatch-Token": "bad"},
        {"X-Internal-Secret": "bad", "X-Dispatch-Token": job["dispatch_token"]},
    ):
        assert client.get(f"/internal/calls/{job['id']}/control", headers=headers).status_code == 401
        assert (
            client.post(
                f"/internal/calls/{job['id']}/events",
                headers=headers,
                json={
                    "event_id": "rejected-event",
                    "event_type": "answered",
                },
            ).status_code
            == 401
        )
    assert client.app.state.store.get_job(job["id"])["status"] == "dispatching"


def test_opt_out_persists_dnc_and_stops_active_call(client):
    job = reserve(client)
    path = f"/internal/calls/{job['id']}"
    headers = event_headers(job)
    response = client.post(
        path + "/events",
        headers=headers,
        json={
            "event_id": "opt-out-1",
            "event_type": "opt_out",
            "payload": {"reason": "Не звоните больше"},
        },
    )
    assert response.status_code == 200
    assert client.get(path + "/control", headers=headers).json() == {"stop": True}
    assert client.get("/api/suppressions", headers=AUTH).json()["items"][0]["phone"] == PHONE
    response = client.post(
        path + "/events",
        headers=headers,
        json={
            "event_id": "ended-after-optout",
            "event_type": "ended",
            "payload": {"status": "completed", "outcome": "interested"},
        },
    )
    assert response.json()["result"]["outcome"] == "not_interested"


def test_final_event_is_idempotent_and_cannot_regress(client):
    job = reserve(client)
    path = f"/internal/calls/{job['id']}/events"
    headers = event_headers(job)
    final = {
        "event_id": "ended-1",
        "event_type": "ended",
        "payload": {
            "status": "busy",
            "duration_sec": 0,
            "transcript": [],
        },
    }
    for _ in range(2):
        response = client.post(path, headers=headers, json=final)
        assert response.status_code == 200
        assert response.json()["result"]["status"] == "busy"
        assert "dispatch_token" not in response.text
    changed = {**final, "payload": {"status": "completed"}}
    assert client.post(path, headers=headers, json=changed).status_code == 409
    assert (
        client.post(path, headers=headers, json={"event_id": "late-answer", "event_type": "answered"}).json()["result"][
            "status"
        ]
        == "busy"
    )


def test_ended_event_requires_terminal_status(client):
    job = reserve(client)
    response = client.post(
        f"/internal/calls/{job['id']}/events",
        headers=event_headers(job),
        json={
            "event_id": "missing-status",
            "event_type": "ended",
        },
    )
    assert response.status_code == 422
    assert client.app.state.store.get_job(job["id"])["status"] == "dispatching"


def test_unknown_dispatch_blocks_queue_until_manual_resolution(client):
    job = reserve(client)
    client.app.state.store.mark_dispatch_failed(job["id"], "synthetic_timeout", uncertain=True)
    assert client.post("/api/queue", headers=AUTH, json={"paused": False}).status_code == 409
    path = f"/api/calls/{job['id']}/resolve"
    assert client.post(path, headers=AUTH, json={"status": "failed", "note": "short"}).status_code == 422
    response = client.post(
        path,
        headers=AUTH,
        json={"status": "failed", "note": "Проверено вручную в журнале оператора: вызов не состоялся"},
    )
    assert response.status_code == 200
    assert response.json()["status"] == "failed"
    assert client.app.state.store.control()["paused"]
    assert client.post("/api/queue", headers=AUTH, json={"paused": False}).status_code == 200
    assert len(client.get("/api/calls", headers=AUTH).json()["items"]) == 1


def test_csv_validates_all_rows_before_writing(client):
    csv = (
        "phone,name,consent_source,consent_at,timezone\n"
        + f"{PHONE},Тест,Заявка TEST,2020-01-01T12:00:00Z,Europe/Moscow\n"
        + "12345,Ошибка,Заявка TEST,2020-01-01T12:00:00Z,Europe/Moscow\n"
    )
    response = client.post("/api/contacts/import", headers=AUTH, content=csv.encode())
    assert response.status_code == 422
    assert response.json()["imported"] == 0
    assert response.json()["errors"][0]["row"] == 3
    assert client.app.state.store.list_contacts() == []


def test_csv_utf8_bom_and_duplicate_rows_are_reported(client):
    row = f"{PHONE},Тест,Заявка TEST,2020-01-01T12:00:00Z,Europe/Moscow\n"
    csv = "\ufeffphone,name,consent_source,consent_at,timezone\n" + row + row
    response = client.post("/api/contacts/import", headers=AUTH, content=csv.encode())
    assert response.status_code == 200
    assert response.json()["imported"] == 1
    assert response.json()["errors"][0]["row"] == 3
    assert len(client.app.state.store.list_contacts()) == 1
    assert client.app.state.store.list_jobs() == []


@pytest.mark.parametrize("body,expected", [(b"", 400), (b"\xff\xfe", 400), (b"x" * 262145, 413)])
def test_invalid_or_oversized_csv_is_rejected(client, body, expected):
    assert client.post("/api/contacts/import", headers=AUTH, content=body).status_code == expected
    assert client.app.state.store.list_contacts() == []


def test_unicode_dispatch_token_fails_closed_without_server_error(app_factory):
    client = app_factory(live=True)
    job = reserve(client)
    response = client.post(
        f"/internal/calls/{job['id']}/claim", headers=INTERNAL_AUTH, json={"dispatch_token": "я" * 43}
    )
    assert response.status_code in (401, 409, 422)
    assert not client.app.state.store.get_job(job["id"])["claimed"]


@pytest.mark.parametrize(
    "path,header", [("/api/config", b"authorization"), ("/internal/calls/missing/control", b"x-internal-secret")]
)
def test_non_ascii_auth_header_fails_closed_without_server_error(client, path, header):
    value = (b"Bearer " if header == b"authorization" else b"") + b"\xc3\xa9" * 32
    response = client.get(path, headers=[(header, value)])
    assert response.status_code == 401


def test_application_restart_leaves_queue_paused(app_factory):
    client = app_factory()
    reserve(client)
    assert not client.app.state.store.control()["paused"]
    restarted = create_app(client.app.state.settings, start_worker=False)
    assert restarted.state.store.control()["paused"]


@pytest.mark.parametrize("caller_id", ["89161234567", "+7 (916) 123-45-67"])
def test_live_caller_id_requires_canonical_format(app_factory, caller_id):
    with pytest.raises(ValueError, match="CALLER_ID"):
        app_factory(live=True, caller_id=caller_id)


@pytest.mark.parametrize("length,expected", [("-1", 400), ("invalid", 400), ("262145", 413)])
def test_content_length_is_checked_before_processing(client, length, expected):
    response = client.post("/api/contacts/import", headers={**AUTH, "Content-Length": length}, content=b"x")
    assert response.status_code == expected
    assert client.app.state.store.list_contacts() == []


@pytest.mark.asyncio
async def test_streamed_body_stops_reading_after_limit(client):
    consumed = []

    async def chunks():
        for index in range(20):
            consumed.append(index)
            yield b"x" * 65536

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=client.app), base_url="http://testserver") as remote:
        response = await remote.post("/api/contacts/import", headers=AUTH, content=chunks())
    assert response.status_code == 413
    assert len(consumed) <= 5
    assert client.app.state.store.list_contacts() == []


def test_correct_admin_token_is_not_locked_out_by_failed_attempts(client):
    for _ in range(20):
        assert client.get("/api/config", headers={"Authorization": "Bearer wrong"}).status_code == 401
    assert client.get("/api/config", headers={"Authorization": "Bearer wrong"}).status_code == 429
    assert client.get("/api/config", headers=AUTH).status_code == 200


def test_missing_product_cannot_resume_or_enqueue(client):
    from pathlib import Path

    contact = create_contact(client)
    Path(client.app.state.settings.product_config_path).unlink()
    assert client.get("/api/config", headers=AUTH).json()["config_error"]
    assert client.post("/api/queue", headers=AUTH, json={"paused": False}).status_code == 409
    assert (
        client.post(
            "/api/calls", headers=AUTH, json={"contact_id": contact["id"], "idempotency_key": "missing-product"}
        ).status_code
        == 409
    )
    assert client.app.state.store.control()["paused"]
    assert client.app.state.store.list_jobs() == []


def test_omitted_timezone_uses_configured_default(app_factory):
    client = app_factory(default_timezone="Asia/Vladivostok")
    payload = contact_payload()
    payload.pop("timezone")
    response = client.post("/api/contacts", headers=AUTH, json=payload)
    assert response.status_code == 201
    assert response.json()["timezone"] == "Asia/Vladivostok"


def test_csv_omitted_timezone_uses_configured_default(app_factory):
    client = app_factory(default_timezone="Asia/Vladivostok")
    csv = f"phone,name,consent_source,consent_at\n{PHONE},Тест,Заявка TEST,2020-01-01T12:00:00Z\n"
    response = client.post("/api/contacts/import", headers=AUTH, content=csv.encode())
    assert response.status_code == 200
    assert response.json()["items"][0]["timezone"] == "Asia/Vladivostok"
