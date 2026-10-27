from datetime import datetime, timezone
from typing import Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
import phonenumbers
from pydantic import BaseModel, ConfigDict, Field, field_validator


def normalize_phone(value: str) -> str:
    try:
        parsed = phonenumbers.parse(value, "RU")
    except phonenumbers.NumberParseException as exc:
        raise ValueError("Укажите настоящий российский номер в формате +7…") from exc
    if not phonenumbers.is_valid_number(parsed) or phonenumbers.region_code_for_number(parsed) != "RU":
        raise ValueError("В этой версии разрешены только валидные российские номера")
    return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class ContactCreate(StrictModel):
    phone: str = Field(min_length=8, max_length=40)
    name: str = Field(default="", max_length=120)
    consent_source: str = Field(min_length=3, max_length=500)
    consent_at: datetime
    timezone: str | None = None

    _phone = field_validator("phone")(normalize_phone)

    @field_validator("timezone")
    @classmethod
    def valid_timezone(cls, value):
        if value is None:
            return value
        try:
            ZoneInfo(value)
        except (ZoneInfoNotFoundError, ValueError) as exc:
            raise ValueError("Неизвестный часовой пояс") from exc
        return value

    @field_validator("consent_at")
    @classmethod
    def consent_date(cls, value):
        if value.tzinfo is None or value > datetime.now(timezone.utc):
            raise ValueError("Дата согласия должна иметь часовой пояс и не быть в будущем")
        return value.astimezone(timezone.utc)


class JobCreate(StrictModel):
    contact_id: str = Field(min_length=1, max_length=64)
    idempotency_key: str = Field(min_length=8, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")
    not_before: datetime | None = None

    @field_validator("not_before")
    @classmethod
    def aware(cls, value):
        if value is not None and value.tzinfo is None:
            raise ValueError("Укажите часовой пояс времени звонка")
        return value.astimezone(timezone.utc) if value else None


class SuppressionCreate(StrictModel):
    phone: str
    reason: str = Field(default="operator_request", min_length=1, max_length=500)
    _phone = field_validator("phone")(normalize_phone)


class QueueControl(StrictModel):
    paused: bool


class Claim(StrictModel):
    dispatch_token: str = Field(min_length=20, max_length=200)


class TranscriptLine(StrictModel):
    role: Literal["user", "assistant"]
    text: str = Field(max_length=4000)


class EventPayload(StrictModel):
    status: Literal["completed", "busy", "no_answer", "failed", "cancelled"] | None = None
    outcome: Literal["interested", "not_interested", "callback", "wrong_number", "unknown"] | None = None
    transcript: list[TranscriptLine] | None = Field(default=None, max_length=200)
    conversation_id: str | None = Field(default=None, max_length=160)
    duration_sec: int | None = Field(default=None, ge=0, le=7200)
    error_code: str | None = Field(default=None, max_length=160)
    reason: str | None = Field(default=None, max_length=500)
    provider_call_id: str | None = Field(default=None, max_length=160)
    transcript_truncated: bool | None = None


class CallEvent(StrictModel):
    event_id: str = Field(min_length=1, max_length=200)
    event_type: Literal["ringing", "answered", "ended", "heartbeat", "opt_out"]
    payload: EventPayload = Field(default_factory=EventPayload)


class ResolveUnknown(StrictModel):
    status: Literal["completed", "busy", "no_answer", "failed", "cancelled"]
    note: str = Field(min_length=10, max_length=1000)
