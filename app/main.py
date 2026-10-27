"""Authenticated control API + a minimal dashboard. No CRM or messenger dependency."""

import asyncio
from collections import defaultdict, deque
from contextlib import asynccontextmanager
import csv
import hmac
import io
import logging
from pathlib import Path
import time

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from .config import Settings
from .models import CallEvent, Claim, ContactCreate, JobCreate, QueueControl, ResolveUnknown, SuppressionCreate
from .product import load_product
from .providers import VoximplantProvider
from .store import Store
from .worker import Worker

log = logging.getLogger(__name__)
STATIC = Path(__file__).parent / "static"


def create_app(settings: Settings | None = None, *, start_worker: bool = True) -> FastAPI:
    cfg = settings or Settings.load()
    cfg.validate()
    store = Store(cfg.database_path)
    store.initialize()
    # Explicit restart semantics: a crash/redeploy must never silently resume paid dialing.
    store.set_paused(True)
    worker = Worker(store, cfg, VoximplantProvider(cfg) if cfg.mode == "live" else None)

    @asynccontextmanager
    async def lifespan(app):
        task = asyncio.create_task(worker.run()) if start_worker else None
        try:
            yield
        finally:
            store.set_paused(True)
            await worker.stop()
            if task:
                await task
            await worker.close()

    app = FastAPI(title="AI Calls", version="0.1.0", lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
    app.state.store, app.state.settings, app.state.worker = store, cfg, worker
    attempts = defaultdict(deque)

    @app.middleware("http")
    async def limits_and_headers(request: Request, call_next):
        if request.method in {"POST", "PUT", "PATCH"}:
            raw_length = request.headers.get("content-length")
            try:
                length = int(raw_length) if raw_length is not None else 0
                if length < 0:
                    raise ValueError
            except ValueError:
                return JSONResponse({"detail": "Invalid Content-Length"}, status_code=400)
            if length > 262144:
                return JSONResponse({"detail": "Тело запроса превышает 256 КБ"}, status_code=413)
            body = bytearray()
            async for chunk in request.stream():
                if len(body) + len(chunk) > 262144:
                    return JSONResponse({"detail": "Тело запроса превышает 256 КБ"}, status_code=413)
                body.extend(chunk)
            # Starlette's cached request passes this bounded body downstream once.
            request._body = bytes(body)
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; frame-ancestors 'none'; base-uri 'none'; form-action 'self'"
        )
        if request.url.path.startswith(("/api", "/internal")):
            response.headers["Cache-Control"] = "no-store"
        return response

    async def admin(request: Request):
        raw = request.headers.get("authorization", "")
        if raw.startswith("Bearer ") and hmac.compare_digest(raw[7:].encode("utf-8"), cfg.admin_token.encode("utf-8")):
            return
        address = request.client.host if request.client else "unknown"
        now = time.monotonic()
        bucket = attempts[address]
        while bucket and bucket[0] < now - 60:
            bucket.popleft()
        if len(bucket) >= 20:
            raise HTTPException(429, "Слишком много попыток входа. Подождите минуту.")
        bucket.append(now)
        if len(attempts) > 10000:
            attempts.clear()
        raise HTTPException(401, "Требуется ADMIN_TOKEN", headers={"WWW-Authenticate": "Bearer"})

    async def internal(request: Request):
        if not hmac.compare_digest(
            request.headers.get("x-internal-secret", "").encode("utf-8"), cfg.internal_secret.encode("utf-8")
        ):
            raise HTTPException(401, "Unauthorized")

    def call_auth(request: Request, job_id: str):
        if not store.verify_dispatch_token(job_id, request.headers.get("x-dispatch-token", "")):
            raise HTTPException(401, "Invalid dispatch token")

    def ready():
        if cfg.mode == "live":
            cfg.validate_live()
        try:
            return load_product(cfg.product_config_path, live=cfg.mode == "live")
        except OSError:
            raise ValueError("Файл продукта недоступен. Проверьте PRODUCT_CONFIG_PATH и права чтения.") from None

    def allowed(phone):
        if cfg.mode == "live" and not cfg.allow_all_consented_numbers and phone not in cfg.live_allowed_numbers:
            raise ValueError("Номер не входит в LIVE_ALLOWED_NUMBERS. Для пилота добавьте свой номер в .env.")

    @app.exception_handler(KeyError)
    async def missing_handler(request, exc):
        return JSONResponse({"detail": "Запись не найдена"}, status_code=404)

    @app.exception_handler(ValueError)
    async def conflict_handler(request, exc):
        return JSONResponse({"detail": str(exc)}, status_code=409)

    @app.get("/health")
    async def health():
        return {"ok": True}  # Liveness only, no provider/paid-call claim.

    @app.get("/")
    async def index():
        return FileResponse(STATIC / "index.html")

    app.mount("/static", StaticFiles(directory=STATIC), name="static")

    @app.get("/api/config", dependencies=[Depends(admin)])
    async def configuration():
        try:
            product = ready()
            product_data, error = product.model_dump(), None
        except (ValueError, OSError):
            product_data, error = None, "Проверьте config/product.json и настройку live."
        return {
            "settings": cfg.public(),
            "product": product_data,
            "config_error": error,
            "queue": store.control(),
            "stats": store.stats(),
        }

    @app.get("/api/contacts", dependencies=[Depends(admin)])
    async def contacts():
        return {"items": store.list_contacts()}

    @app.post("/api/contacts", dependencies=[Depends(admin)], status_code=201)
    async def add_contact(body: ContactCreate):
        return store.create_contact(
            body.phone,
            body.name,
            body.consent_source,
            body.consent_at.isoformat(),
            body.timezone or cfg.default_timezone,
        )

    @app.post("/api/contacts/import", dependencies=[Depends(admin)])
    async def import_contacts(request: Request):
        """CSV is validated completely before importing. Partial failure is reported explicitly."""
        try:
            text = (await request.body()).decode("utf-8-sig")
            reader = csv.DictReader(io.StringIO(text))
            rows = list(reader)
        except (UnicodeError, csv.Error):
            raise HTTPException(400, "Нужен CSV в UTF-8")
        if not rows or len(rows) > 500:
            raise HTTPException(400, "CSV должен содержать от 1 до 500 строк")
        validated, errors = [], []
        for index, row in enumerate(rows, start=2):
            try:
                item = ContactCreate.model_validate(row)
                validated.append(item)
            except ValueError:
                errors.append({"row": index, "error": "Проверьте номер РФ, имя, источник/дату согласия и timezone"})
        if errors:
            return JSONResponse({"imported": 0, "errors": errors}, status_code=422)
        imported = []
        for index, item in enumerate(validated, start=2):
            try:
                imported.append(
                    store.create_contact(
                        item.phone,
                        item.name,
                        item.consent_source,
                        item.consent_at.isoformat(),
                        item.timezone or cfg.default_timezone,
                    )
                )
            except ValueError as exc:
                errors.append({"row": index, "error": str(exc)})
        return {"imported": len(imported), "items": imported, "errors": errors}

    @app.post("/api/calls", dependencies=[Depends(admin)], status_code=201)
    async def add_call(body: JobCreate):
        ready()
        contact = next((c for c in store.list_contacts() if c["id"] == body.contact_id), None)
        if contact is None:
            raise KeyError("contact")
        allowed(contact["phone"])
        return store.enqueue(
            body.contact_id,
            body.idempotency_key,
            body.not_before.isoformat() if body.not_before else None,
            mode=cfg.mode,
        )

    @app.get("/api/calls", dependencies=[Depends(admin)])
    async def calls():
        return {"items": store.list_jobs(200), "queue": store.control(), "stats": store.stats()}

    @app.get("/api/calls/{job_id}", dependencies=[Depends(admin)])
    async def get_call(job_id: str):
        job = store.get_job(job_id)
        if not job:
            raise KeyError("job")
        return job

    @app.post("/api/queue", dependencies=[Depends(admin)])
    async def queue(body: QueueControl):
        if not body.paused:
            ready()
        return store.set_paused(body.paused)

    @app.post("/api/calls/{job_id}/stop", dependencies=[Depends(admin)])
    async def stop_call(job_id: str):
        return store.request_stop(job_id)

    @app.post("/api/calls/{job_id}/resolve", dependencies=[Depends(admin)])
    async def resolve(job_id: str, body: ResolveUnknown):
        return store.resolve_unknown(job_id, body.status, body.note)

    @app.get("/api/suppressions", dependencies=[Depends(admin)])
    async def suppressions():
        return {"items": store.list_suppressions()}

    @app.post("/api/suppressions", dependencies=[Depends(admin)])
    async def suppress(body: SuppressionCreate):
        return store.suppress(body.phone, body.reason)

    @app.post("/internal/calls/{job_id}/claim", dependencies=[Depends(internal)])
    async def claim(job_id: str, body: Claim):
        if cfg.mode != "live":
            raise HTTPException(409, "Live dialing is disabled")
        product = ready()
        job = store.get_job(job_id)
        if not job:
            raise KeyError("job")
        allowed(job["phone"])
        if job["mode"] != "live":
            raise ValueError("Job mode mismatch")
        job = store.claim(job_id, body.dispatch_token)
        return {
            "job_id": job_id,
            "phone": job["phone"],
            "caller_id": cfg.caller_id,
            "agent_id": cfg.elevenlabs_agent_id,
            "max_duration_sec": cfg.max_duration_sec,
            "ringing_timeout_sec": cfg.ringing_timeout_sec,
            "dynamic_variables": product.variables(job["name"]),
        }

    @app.post("/internal/calls/{job_id}/events", dependencies=[Depends(internal)])
    async def event(job_id: str, body: CallEvent, request: Request):
        call_auth(request, job_id)
        if body.event_type == "ended" and body.payload.status is None:
            raise HTTPException(422, "ended requires payload.status")
        result = store.event(job_id, body.event_id, body.event_type, body.payload.model_dump(exclude_none=True))
        return {"stored": True, "result": result}

    @app.get("/internal/calls/{job_id}/control", dependencies=[Depends(internal)])
    async def control(job_id: str, request: Request):
        call_auth(request, job_id)
        return store.poll_control(job_id)

    return app
