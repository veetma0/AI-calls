"""Transactional call queue. No network access and no automatic redial.

All persisted times and daily attempt budgets use UTC. Calling windows use the
contact's IANA timezone. Every dispatch reservation consumes the day's budget,
including rejected and uncertain attempts.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import re
import secrets
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

ACTIVE = ("dispatching", "starting", "ringing", "answered")
TERMINAL = ("completed", "busy", "no_answer", "failed", "cancelled")
OUTCOMES = ("interested", "not_interested", "callback", "wrong_number", "unknown")
CLAIM_TTL_SECONDS = 90


def utcnow() -> datetime:
    """Clock seam for deterministic tests."""
    return datetime.now(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds")


def _parse(value: str | datetime, label: str) -> datetime:
    try:
        result = value if isinstance(value, datetime) else datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError, TypeError):
        raise ValueError(f"{label} must be an ISO 8601 timestamp") from None
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError(f"{label} must include a timezone")
    return result.astimezone(timezone.utc)


def _text(value: Any, label: str, maximum: int, *, required: bool = True) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{label} must be text")
    value = value.strip()
    if (required and not value) or len(value) > maximum:
        raise ValueError(f"Invalid {label}")
    return value


def _phone(value: str) -> str:
    value = _text(value, "phone", 32)
    if not re.fullmatch(r"\+[1-9][0-9]{7,14}", value):
        raise ValueError("Phone must use international +country format")
    return value


class Store:
    def __init__(self, path: str | Path):
        self.path = str(Path(path).expanduser()) if str(path) != ":memory:" else ":memory:"
        self._uri = self.path == ":memory:"
        self._keeper: sqlite3.Connection | None = None
        if self._uri:
            self.path = f"file:ai_calls_{uuid.uuid4().hex}?mode=memory&cache=shared"
            self._keeper = sqlite3.connect(self.path, uri=True, check_same_thread=False)
        else:
            directory = Path(self.path).resolve().parent
            missing = []
            while not directory.exists():
                missing.append(directory)
                directory = directory.parent
            for directory in reversed(missing):
                # Apply private permissions only to directories we create; never
                # change permissions on an existing workspace or shared parent.
                directory.mkdir(mode=0o700, exist_ok=True)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, uri=self._uri, timeout=10, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=10000")
        return connection

    @contextmanager
    def _tx(self):
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    @contextmanager
    def _read(self):
        connection = self._connect()
        try:
            yield connection
        finally:
            connection.close()

    def initialize(self) -> None:
        with self._read() as connection:
            if not self._uri:
                # Do this before enabling WAL so SQLite sidecar files inherit the
                # private database permissions, irrespective of the process umask.
                Path(self.path).chmod(0o600)
            connection.execute("PRAGMA journal_mode=WAL")
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS contacts (
                    id TEXT PRIMARY KEY, phone TEXT NOT NULL UNIQUE,
                    name TEXT NOT NULL, consent_source TEXT NOT NULL,
                    consent_at TEXT NOT NULL, timezone TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS suppressions (
                    phone TEXT PRIMARY KEY, reason TEXT NOT NULL,
                    created_at TEXT NOT NULL, updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS control (
                    id INTEGER PRIMARY KEY CHECK(id=1), paused INTEGER NOT NULL
                );
                INSERT OR IGNORE INTO control(id,paused) VALUES(1,1);
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY, contact_id TEXT NOT NULL REFERENCES contacts(id),
                    idempotency_key TEXT NOT NULL UNIQUE, mode TEXT NOT NULL,
                    status TEXT NOT NULL, not_before TEXT, created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL, dispatched_at TEXT,
                    dispatch_token TEXT, claimed INTEGER NOT NULL DEFAULT 0,
                    claimed_at TEXT, provider_session_id TEXT, provider_call_id TEXT,
                    last_event_at TEXT, last_event_type TEXT,
                    stop_requested INTEGER NOT NULL DEFAULT 0,
                    outcome TEXT NOT NULL DEFAULT 'unknown', transcript TEXT NOT NULL DEFAULT '[]',
                    conversation_id TEXT, duration_sec INTEGER, error_code TEXT,
                    reason TEXT, resolved_note TEXT, provider_final_status TEXT,
                    transcript_truncated INTEGER NOT NULL DEFAULT 0,
                    reserved_window_start INTEGER, reserved_window_end INTEGER
                );
                CREATE INDEX IF NOT EXISTS jobs_queue ON jobs(status,created_at);
                CREATE INDEX IF NOT EXISTS jobs_attempts ON jobs(dispatched_at);
                CREATE TABLE IF NOT EXISTS events (
                    job_id TEXT NOT NULL REFERENCES jobs(id), event_id TEXT NOT NULL,
                    event_type TEXT NOT NULL, payload TEXT NOT NULL, fingerprint TEXT NOT NULL,
                    received_at TEXT NOT NULL, PRIMARY KEY(job_id,event_id)
                );
            """)
            columns = {row[1] for row in connection.execute("PRAGMA table_info(jobs)")}
            for name, definition in (
                ("transcript_truncated", "INTEGER NOT NULL DEFAULT 0"),
                ("provider_call_id", "TEXT"),
                ("reserved_window_start", "INTEGER"),
                ("reserved_window_end", "INTEGER"),
            ):
                if name not in columns:
                    connection.execute(f"ALTER TABLE jobs ADD COLUMN {name} {definition}")

    @staticmethod
    def _contact(connection: sqlite3.Connection, contact_id: str) -> sqlite3.Row:
        row = connection.execute("SELECT * FROM contacts WHERE id=?", (contact_id,)).fetchone()
        if row is None:
            raise KeyError(contact_id)
        return row

    @staticmethod
    def _job(connection: sqlite3.Connection, job_id: str) -> sqlite3.Row:
        row = connection.execute(
            """SELECT j.*,c.phone,c.name,c.timezone,c.consent_source,c.consent_at
            FROM jobs j JOIN contacts c ON c.id=j.contact_id WHERE j.id=?""",
            (job_id,),
        ).fetchone()
        if row is None:
            raise KeyError(job_id)
        return row

    @staticmethod
    def _public(row: sqlite3.Row | dict, *, internal: bool = False) -> dict:
        result = dict(row)
        if not internal:
            result.pop("dispatch_token", None)
        if "transcript" in result:
            result["transcript"] = json.loads(result["transcript"])
        for flag in ("claimed", "stop_requested", "suppressed", "transcript_truncated"):
            if flag in result:
                result[flag] = bool(result[flag])
        return result

    def create_contact(self, phone, name, consent_source, consent_at, timezone) -> dict:
        phone = _phone(phone)
        name = _text(name, "name", 200, required=False)
        consent_source = _text(consent_source, "consent_source", 1000)
        consent_date = _parse(consent_at, "consent_at")
        now = utcnow()
        if consent_date > now:
            raise ValueError("Consent cannot be dated in the future")
        try:
            ZoneInfo(timezone)
        except (ValueError, TypeError, ZoneInfoNotFoundError):
            raise ValueError("Unknown contact timezone") from None
        contact_id = uuid.uuid4().hex
        with self._tx() as connection:
            try:
                connection.execute(
                    "INSERT INTO contacts VALUES(?,?,?,?,?,?,?)",
                    (contact_id, phone, name, consent_source, _iso(consent_date), timezone, _iso(now)),
                )
            except sqlite3.IntegrityError:
                raise ValueError("A contact with this phone already exists") from None
            return dict(self._contact(connection, contact_id))

    def list_contacts(self) -> list:
        with self._read() as connection:
            rows = connection.execute("""SELECT c.*,s.phone IS NOT NULL AS suppressed
                FROM contacts c LEFT JOIN suppressions s ON s.phone=c.phone
                ORDER BY c.created_at DESC,c.id""").fetchall()
            return [self._public(row) for row in rows]

    @staticmethod
    def _suppress(connection, phone: str, reason: str, now: str) -> dict:
        connection.execute(
            """INSERT INTO suppressions VALUES(?,?,?,?)
            ON CONFLICT(phone) DO UPDATE SET reason=excluded.reason,updated_at=excluded.updated_at""",
            (phone, reason, now, now),
        )
        connection.execute(
            """UPDATE jobs SET status='cancelled',stop_requested=1,
            reason='suppressed',updated_at=? WHERE status='queued'
            AND contact_id IN (SELECT id FROM contacts WHERE phone=?)""",
            (now, phone),
        )
        connection.execute(
            """UPDATE jobs SET stop_requested=1,updated_at=?
            WHERE status IN ('dispatching','starting','ringing','answered','unknown')
            AND contact_id IN (SELECT id FROM contacts WHERE phone=?)""",
            (now, phone),
        )
        return dict(connection.execute("SELECT * FROM suppressions WHERE phone=?", (phone,)).fetchone())

    def suppress(self, phone, reason) -> dict:
        phone, reason = _phone(phone), _text(reason, "reason", 1000)
        with self._tx() as connection:
            return self._suppress(connection, phone, reason, _iso(utcnow()))

    def list_suppressions(self) -> list:
        with self._read() as connection:
            return [dict(row) for row in connection.execute("SELECT * FROM suppressions ORDER BY created_at DESC")]

    def enqueue(self, contact_id, idempotency_key, not_before=None, mode="mock") -> dict:
        key = _text(idempotency_key, "idempotency_key", 160)
        if mode not in ("mock", "live"):
            raise ValueError("Invalid mode")
        scheduled = _iso(_parse(not_before, "not_before")) if not_before is not None else None
        with self._tx() as connection:
            previous = connection.execute(
                "SELECT id,contact_id,not_before,mode FROM jobs WHERE idempotency_key=?", (key,)
            ).fetchone()
            if previous is not None:
                if (previous["contact_id"], previous["not_before"], previous["mode"]) != (contact_id, scheduled, mode):
                    raise ValueError("Idempotency key was already used for a different request")
                return self._public(self._job(connection, previous["id"]))
            contact = self._contact(connection, contact_id)
            if connection.execute("SELECT 1 FROM suppressions WHERE phone=?", (contact["phone"],)).fetchone():
                raise ValueError("This number is in the do-not-call list")
            if connection.execute(
                """SELECT 1 FROM jobs j JOIN contacts c ON c.id=j.contact_id
                WHERE c.phone=? AND j.status IN ('queued','dispatching','starting','ringing','answered','unknown') LIMIT 1""",
                (contact["phone"],),
            ).fetchone():
                raise ValueError("This number already has a pending or unresolved call")
            if not contact["consent_source"] or _parse(contact["consent_at"], "consent_at") > utcnow():
                raise ValueError("Valid recorded consent is required")
            job_id, now = uuid.uuid4().hex, _iso(utcnow())
            connection.execute(
                """INSERT INTO jobs(id,contact_id,idempotency_key,mode,status,not_before,created_at,updated_at)
                VALUES(?,?,?,?,'queued',?,?,?)""",
                (job_id, contact_id, key, mode, scheduled, now, now),
            )
            return self._public(self._job(connection, job_id))

    def list_jobs(self, limit=100) -> list:
        limit = max(1, min(int(limit), 1000))
        with self._read() as connection:
            rows = connection.execute(
                """SELECT j.*,c.phone,c.name,c.timezone,c.consent_source,c.consent_at
                FROM jobs j JOIN contacts c ON c.id=j.contact_id ORDER BY j.created_at DESC,j.rowid DESC LIMIT ?""",
                (limit,),
            ).fetchall()
            return [self._public(row) for row in rows]

    def get_job(self, job_id) -> dict | None:
        with self._read() as connection:
            try:
                return self._public(self._job(connection, job_id))
            except KeyError:
                return None

    def control(self) -> dict:
        with self._read() as connection:
            return {"paused": bool(connection.execute("SELECT paused FROM control WHERE id=1").fetchone()[0])}

    @staticmethod
    def _pause(connection, now: str) -> None:
        connection.execute("UPDATE control SET paused=1 WHERE id=1")
        # A quick pause/resume must not erase the stop instruction before an
        # already-running remote scenario gets to its next control poll.
        connection.execute(
            """UPDATE jobs SET stop_requested=1,updated_at=?
            WHERE status IN ('dispatching','starting','ringing','answered','unknown')""",
            (now,),
        )

    def set_paused(self, paused: bool) -> dict:
        with self._tx() as connection:
            if not paused and connection.execute("SELECT 1 FROM jobs WHERE status='unknown' LIMIT 1").fetchone():
                raise ValueError("Resolve unknown calls after checking the provider before resuming")
            if paused:
                self._pause(connection, _iso(utcnow()))
            else:
                connection.execute("UPDATE control SET paused=0 WHERE id=1")
        return {"paused": bool(paused)}

    def request_stop(self, job_id) -> dict:
        with self._tx() as connection:
            row = self._job(connection, job_id)
            status = "cancelled" if row["status"] == "queued" else row["status"]
            connection.execute(
                "UPDATE jobs SET status=?,stop_requested=1,updated_at=? WHERE id=?", (status, _iso(utcnow()), job_id)
            )
            return self._public(self._job(connection, job_id))

    def reserve_next(self, max_concurrent, daily_limit, window_start, window_end, mode=None) -> dict | None:
        if max_concurrent < 1 or daily_limit < 1:
            raise ValueError("Limits must be positive")
        if not (0 <= window_start < 24 and 0 <= window_end <= 24) or window_start == window_end:
            raise ValueError("Invalid calling window")
        if mode is not None and mode not in ("mock", "live"):
            raise ValueError("Invalid mode")
        now = utcnow()
        stamp = _iso(now)
        midnight = _iso(now.replace(hour=0, minute=0, second=0, microsecond=0))
        with self._tx() as connection:
            if connection.execute("SELECT paused FROM control WHERE id=1").fetchone()[0]:
                return None
            if connection.execute("SELECT 1 FROM jobs WHERE status='unknown' LIMIT 1").fetchone():
                self._pause(connection, stamp)
                return None
            active = connection.execute(
                "SELECT COUNT(*) FROM jobs WHERE status IN ('dispatching','starting','ringing','answered')"
            ).fetchone()[0]
            attempts = connection.execute("SELECT COUNT(*) FROM jobs WHERE dispatched_at>=?", (midnight,)).fetchone()[0]
            if active >= max_concurrent or attempts >= daily_limit:
                return None
            rows = connection.execute(
                """SELECT j.*,c.phone,c.timezone FROM jobs j JOIN contacts c ON c.id=j.contact_id
                WHERE j.status='queued' AND (j.not_before IS NULL OR j.not_before<=?)
                AND (? IS NULL OR j.mode=?) AND j.stop_requested=0
                AND NOT EXISTS(SELECT 1 FROM suppressions s WHERE s.phone=c.phone)
                AND NOT EXISTS(SELECT 1 FROM jobs a JOIN contacts ac ON ac.id=a.contact_id
                    WHERE ac.phone=c.phone AND a.status IN ('dispatching','starting','ringing','answered','unknown'))
                ORDER BY j.created_at,j.rowid""",
                (stamp, mode, mode),
            ).fetchall()
            for row in rows:
                local = now.astimezone(ZoneInfo(row["timezone"]))
                hour = local.hour + local.minute / 60 + local.second / 3600
                inside = (
                    window_start <= hour < window_end
                    if window_start < window_end
                    else hour >= window_start or hour < window_end
                )
                if not inside:
                    continue
                token = secrets.token_urlsafe(32)
                connection.execute(
                    """UPDATE jobs SET status='dispatching',dispatch_token=?,dispatched_at=?,updated_at=?,
                    reserved_window_start=?,reserved_window_end=? WHERE id=? AND status='queued'""",
                    (token, stamp, stamp, window_start, window_end, row["id"]),
                )
                return self._public(self._job(connection, row["id"]), internal=True)
            return None

    def mark_dispatched(self, job_id, provider_session_id=None):
        if provider_session_id is not None:
            provider_session_id = _text(str(provider_session_id), "provider_session_id", 300)
        with self._tx() as connection:
            row = self._job(connection, job_id)
            status = "starting" if row["status"] == "dispatching" else row["status"]
            connection.execute(
                """UPDATE jobs SET status=?,provider_session_id=COALESCE(?,provider_session_id),updated_at=? WHERE id=?""",
                (status, provider_session_id, _iso(utcnow()), job_id),
            )

    def mark_dispatch_failed(self, job_id, reason, uncertain=False):
        reason = _text(reason, "reason", 1000)
        with self._tx() as connection:
            row = self._job(connection, job_id)
            if row["status"] in TERMINAL or row["status"] == "unknown":
                return
            # A late negative dispatch response cannot undo a scenario that has
            # already claimed and advanced. Preserve it until a final event/stale check.
            if not uncertain and (row["status"] in ("ringing", "answered") or row["claimed"]):
                return
            status = "unknown" if uncertain else "failed"
            connection.execute(
                "UPDATE jobs SET status=?,reason=?,updated_at=? WHERE id=?", (status, reason, _iso(utcnow()), job_id)
            )
            if uncertain:
                self._pause(connection, _iso(utcnow()))

    def mark_unknown_stale(self, max_age_seconds):
        if max_age_seconds <= 0:
            raise ValueError("Stale age must be positive")
        now = utcnow()
        cutoff = _iso(now - timedelta(seconds=max_age_seconds))
        with self._tx() as connection:
            # A heartbeat is not proof that a call may run indefinitely. The hard
            # maximum is measured from the reservation, not from the last heartbeat.
            result = connection.execute(
                """UPDATE jobs SET status='unknown',reason='missing_final_event',stop_requested=1,updated_at=?
                WHERE status IN ('dispatching','starting','ringing','answered') AND dispatched_at<=?""",
                (_iso(now), cutoff),
            )
            if result.rowcount:
                self._pause(connection, _iso(now))
            return result.rowcount

    def verify_dispatch_token(self, job_id, token) -> bool:
        if not isinstance(token, str) or not token:
            return False
        with self._read() as connection:
            row = connection.execute("SELECT dispatch_token FROM jobs WHERE id=?", (job_id,)).fetchone()
            return bool(row and row[0] and hmac.compare_digest(row[0].encode("utf-8"), token.encode("utf-8")))

    def claim(self, job_id, dispatch_token) -> dict:
        expired = False
        outside_window = False
        result = None
        with self._tx() as connection:
            row = self._job(connection, job_id)
            if (
                not isinstance(dispatch_token, str)
                or not row["dispatch_token"]
                or not hmac.compare_digest(row["dispatch_token"].encode("utf-8"), dispatch_token.encode("utf-8"))
            ):
                raise ValueError("Invalid dispatch token")
            if row["status"] not in ("dispatching", "starting") or row["claimed"]:
                raise ValueError("Call cannot be claimed again or from its current status")
            if connection.execute("SELECT paused FROM control WHERE id=1").fetchone()[0] or row["stop_requested"]:
                raise ValueError("Call has been stopped or the queue is paused")
            if connection.execute("SELECT 1 FROM suppressions WHERE phone=?", (row["phone"],)).fetchone():
                raise ValueError("Number is suppressed")
            now = utcnow()
            if (
                not row["dispatched_at"]
                or (now - _parse(row["dispatched_at"], "dispatched_at")).total_seconds() >= CLAIM_TTL_SECONDS
            ):
                connection.execute(
                    "UPDATE jobs SET status='unknown',reason='claim_expired',stop_requested=1,updated_at=? WHERE id=?",
                    (_iso(now), job_id),
                )
                self._pause(connection, _iso(now))
                expired = True
            else:
                local = now.astimezone(ZoneInfo(row["timezone"]))
                hour = local.hour + local.minute / 60 + local.second / 3600
                start, end = row["reserved_window_start"], row["reserved_window_end"]
                inside = (
                    start is not None
                    and end is not None
                    and (start <= hour < end if start < end else hour >= start or hour < end)
                )
                if not inside:
                    connection.execute(
                        "UPDATE jobs SET status='cancelled',reason='calling_window_closed',stop_requested=1,updated_at=? WHERE id=?",
                        (_iso(now), job_id),
                    )
                    outside_window = True
                else:
                    connection.execute(
                        "UPDATE jobs SET claimed=1,claimed_at=?,status='starting',updated_at=? WHERE id=?",
                        (_iso(now), _iso(now), job_id),
                    )
                    result = self._public(self._job(connection, job_id), internal=True)
        if expired:
            raise ValueError("Dispatch claim expired; check the provider before resolving")
        if outside_window:
            raise ValueError("The contact's calling window has closed")
        return result

    @staticmethod
    def _validate_payload(event_type, payload):
        if not isinstance(payload, dict):
            raise ValueError("Event payload must be an object")
        if "outcome" in payload and payload["outcome"] not in OUTCOMES:
            raise ValueError("Unknown call outcome")
        if "transcript_truncated" in payload and not isinstance(payload["transcript_truncated"], bool):
            raise ValueError("Invalid transcript truncation flag")
        if event_type == "ended" and payload.get("status") not in TERMINAL:
            raise ValueError("A final event requires a valid terminal status")
        if "duration_sec" in payload:
            value = payload["duration_sec"]
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not 0 <= value <= 86400:
                raise ValueError("Invalid call duration")
        for key in ("conversation_id", "provider_call_id", "error_code", "reason"):
            if key in payload and payload[key] is not None:
                _text(payload[key], key, 1000, required=False)
        if "transcript" in payload:
            transcript = payload["transcript"]
            if not isinstance(transcript, list) or len(transcript) > 200:
                raise ValueError("Invalid transcript")
            for item in transcript:
                if not isinstance(item, dict) or item.get("role") not in ("user", "assistant", "agent", "system"):
                    raise ValueError("Invalid transcript role")
                _text(item.get("text"), "transcript text", 8000, required=False)
        try:
            encoded = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
        except (ValueError, TypeError):
            raise ValueError("Invalid event JSON") from None
        if len(encoded.encode("utf-8")) > 262144:
            raise ValueError("Event payload too large")
        return encoded

    def event(self, job_id, event_id, event_type, payload) -> dict:
        event_id = _text(event_id, "event_id", 200)
        if event_type not in ("ringing", "answered", "ended", "heartbeat", "opt_out"):
            raise ValueError("Unsupported call event")
        encoded = self._validate_payload(event_type, payload)
        fingerprint = hashlib.sha256((event_type + "\n" + encoded).encode()).hexdigest()
        with self._tx() as connection:
            row = self._job(connection, job_id)
            previous = connection.execute(
                "SELECT fingerprint FROM events WHERE job_id=? AND event_id=?", (job_id, event_id)
            ).fetchone()
            if previous:
                if previous[0] != fingerprint:
                    raise ValueError("Event ID was reused with different contents")
                return self._public(row)
            now = _iso(utcnow())
            connection.execute(
                "INSERT INTO events VALUES(?,?,?,?,?,?)", (job_id, event_id, event_type, encoded, fingerprint, now)
            )
            status = row["status"]
            if event_type == "ended" and payload.get("error_code") == "opt_out_persistence_failed":
                # The final authenticated event is fallback evidence that the
                # scenario heard an opt-out but failed to persist its first event.
                self._suppress(connection, row["phone"], "user_request_final_fallback", now)
                self._pause(connection, now)
            if event_type == "opt_out":
                self._suppress(
                    connection, row["phone"], _text(payload.get("reason", "user_request"), "reason", 1000), now
                )
                connection.execute("UPDATE jobs SET outcome='not_interested',stop_requested=1 WHERE id=?", (job_id,))
            if status in ACTIVE:
                if event_type == "ringing" and status in ("dispatching", "starting"):
                    status = "ringing"
                elif event_type == "answered":
                    status = "answered"
                elif event_type == "ended":
                    status = payload["status"]
                fields: dict[str, Any] = {"status": status}
                if event_type == "opt_out":
                    fields["outcome"] = "not_interested"
                elif connection.execute("SELECT 1 FROM suppressions WHERE phone=?", (row["phone"],)).fetchone():
                    fields["outcome"] = "not_interested"
                elif "outcome" in payload:
                    # An unspecified final outcome must not erase a durable tool outcome.
                    if payload["outcome"] != "unknown" or row["outcome"] == "unknown":
                        fields["outcome"] = payload["outcome"]
                for key in ("conversation_id", "provider_call_id", "duration_sec", "error_code"):
                    if key in payload and payload[key] is not None:
                        fields[key] = int(payload[key]) if key == "duration_sec" else payload[key]
                if "transcript" in payload:
                    fields["transcript"] = json.dumps(payload["transcript"], ensure_ascii=False)
                if "transcript_truncated" in payload:
                    fields["transcript_truncated"] = int(payload["transcript_truncated"])
                if event_type == "ended":
                    fields["provider_final_status"] = payload["status"]
                connection.execute(
                    "UPDATE jobs SET " + ",".join(f"{key}=?" for key in fields) + " WHERE id=?",
                    (*fields.values(), job_id),
                )
            elif status == "unknown" and event_type == "ended":
                # Keep the manual review barrier even after a late final message;
                # expose its status as evidence instead of silently resuming calls.
                fields = {"provider_final_status": payload["status"]}
                for key in ("conversation_id", "provider_call_id", "duration_sec", "error_code", "outcome"):
                    if key in payload and payload[key] is not None:
                        fields[key] = int(payload[key]) if key == "duration_sec" else payload[key]
                if connection.execute("SELECT 1 FROM suppressions WHERE phone=?", (row["phone"],)).fetchone():
                    fields["outcome"] = "not_interested"
                if "transcript" in payload:
                    fields["transcript"] = json.dumps(payload["transcript"], ensure_ascii=False)
                if "transcript_truncated" in payload:
                    fields["transcript_truncated"] = int(payload["transcript_truncated"])
                connection.execute(
                    "UPDATE jobs SET " + ",".join(f"{key}=?" for key in fields) + " WHERE id=?",
                    (*fields.values(), job_id),
                )
            connection.execute(
                "UPDATE jobs SET last_event_at=?,last_event_type=?,updated_at=? WHERE id=?",
                (now, event_type, now, job_id),
            )
            return self._public(self._job(connection, job_id))

    def poll_control(self, job_id) -> dict:
        with self._read() as connection:
            row = self._job(connection, job_id)
            paused = connection.execute("SELECT paused FROM control WHERE id=1").fetchone()[0]
            suppressed = connection.execute("SELECT 1 FROM suppressions WHERE phone=?", (row["phone"],)).fetchone()
            return {
                "stop": bool(paused or row["stop_requested"] or suppressed or row["status"] in (*TERMINAL, "unknown"))
            }

    def stats(self) -> dict:
        midnight = _iso(utcnow().replace(hour=0, minute=0, second=0, microsecond=0))
        with self._read() as connection:
            counts = {row[0]: row[1] for row in connection.execute("SELECT status,COUNT(*) FROM jobs GROUP BY status")}
            return {
                "contacts": connection.execute("SELECT COUNT(*) FROM contacts").fetchone()[0],
                "suppressed": connection.execute("SELECT COUNT(*) FROM suppressions").fetchone()[0],
                "jobs": sum(counts.values()),
                "by_status": counts,
                "active": sum(counts.get(status, 0) for status in ACTIVE),
                "unknown": counts.get("unknown", 0),
                "attempts_today": connection.execute(
                    "SELECT COUNT(*) FROM jobs WHERE dispatched_at>=?", (midnight,)
                ).fetchone()[0],
                "budget_timezone": "UTC",
                "paused": bool(connection.execute("SELECT paused FROM control WHERE id=1").fetchone()[0]),
            }

    def resolve_unknown(self, job_id, status, note) -> dict:
        if status not in TERMINAL:
            raise ValueError("Unknown calls may only be resolved to a terminal status")
        note = _text(note, "resolution note", 2000)
        with self._tx() as connection:
            row = self._job(connection, job_id)
            if row["status"] != "unknown":
                raise ValueError("Only an unknown call can be manually resolved")
            connection.execute(
                "UPDATE jobs SET status=?,resolved_note=?,updated_at=? WHERE id=?",
                (status, note, _iso(utcnow()), job_id),
            )
            return self._public(self._job(connection, job_id))
