"""Configuration. Loading never initiates a network call."""

from dataclasses import dataclass
import os
import re
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    mode: str = "mock"
    admin_token: str = ""
    internal_secret: str = ""
    database_path: str = "data/calls.sqlite3"
    product_config_path: str = "config/product.json"
    public_base_url: str = ""
    voximplant_account_id: str = ""
    voximplant_api_key: str = ""
    voximplant_rule_id: str = ""
    caller_id: str = ""
    elevenlabs_agent_id: str = ""
    max_concurrent: int = 1
    daily_attempt_limit: int = 20
    max_duration_sec: int = 180
    ringing_timeout_sec: int = 30
    call_window_start: int = 9
    call_window_end: int = 20
    default_timezone: str = "Europe/Moscow"
    live_allowed_numbers: tuple[str, ...] = ()
    allow_all_consented_numbers: bool = False
    worker_interval_sec: float = 1.0

    @classmethod
    def load(cls):
        load_dotenv(override=False)
        defaults = cls()
        values = {}
        for name in cls.__dataclass_fields__:
            raw = os.environ.get(name.upper())
            if raw is None:
                continue
            default = getattr(defaults, name)
            if isinstance(default, bool):
                if raw.lower() not in ("true", "false", "1", "0"):
                    raise ValueError(f"{name.upper()} must be true or false")
                values[name] = raw.lower() in ("true", "1")
            elif isinstance(default, int):
                values[name] = int(raw)
            elif isinstance(default, float):
                values[name] = float(raw)
            elif isinstance(default, tuple):
                values[name] = tuple(v.strip() for v in raw.split(",") if v.strip())
            else:
                values[name] = raw.strip()
        settings = cls(**values)
        settings.validate()
        return settings

    def validate(self):
        if self.mode not in {"mock", "live"}:
            raise ValueError("MODE must be mock or live")
        for value, label in ((self.admin_token, "ADMIN_TOKEN"), (self.internal_secret, "INTERNAL_SECRET")):
            if len(value) < 32 or not value.isascii() or any(c.isspace() for c in value):
                raise ValueError(
                    f"Set {label} to a random secret of at least 32 characters (python scripts/init_env.py)"
                )
        if self.admin_token == self.internal_secret:
            raise ValueError("ADMIN_TOKEN and INTERNAL_SECRET must differ")
        if not 1 <= self.max_concurrent <= 20:
            raise ValueError("MAX_CONCURRENT must be 1..20")
        if not 1 <= self.daily_attempt_limit <= 10000:
            raise ValueError("DAILY_ATTEMPT_LIMIT must be 1..10000")
        if not 15 <= self.max_duration_sec <= 1800 or not 10 <= self.ringing_timeout_sec <= 90:
            raise ValueError("Invalid call duration or ringing timeout")
        if not 0 <= self.call_window_start < self.call_window_end <= 24:
            raise ValueError("Call window must be within one local day, start < end")
        if not 0.1 <= self.worker_interval_sec <= 60:
            raise ValueError("Invalid WORKER_INTERVAL_SEC")
        ZoneInfo(self.default_timezone)
        if self.mode == "live":
            self.validate_live()

    def validate_live(self):
        missing = [
            key.upper()
            for key in (
                "public_base_url",
                "voximplant_account_id",
                "voximplant_api_key",
                "voximplant_rule_id",
                "caller_id",
                "elevenlabs_agent_id",
            )
            if not getattr(self, key)
        ]
        if missing:
            raise ValueError("Missing live settings: " + ", ".join(missing))
        url = urlsplit(self.public_base_url)
        if (
            url.scheme != "https"
            or not url.netloc
            or url.username
            or url.password
            or url.query
            or url.fragment
            or url.path not in ("", "/")
        ):
            raise ValueError("PUBLIC_BASE_URL must be an HTTPS origin, without path or credentials")
        if not self.voximplant_account_id.isdigit() or not self.voximplant_rule_id.isdigit():
            raise ValueError("Voximplant account and routing rule IDs must be numeric")
        from .models import normalize_phone

        if normalize_phone(self.caller_id) != self.caller_id:
            raise ValueError("CALLER_ID must be a normalized +7 number")
        if not re.fullmatch(r"agent_[A-Za-z0-9_-]+", self.elevenlabs_agent_id):
            raise ValueError("ELEVENLABS_AGENT_ID must be an agent_ identifier")
        for number in self.live_allowed_numbers:
            if normalize_phone(number) != number:
                raise ValueError("LIVE_ALLOWED_NUMBERS must contain normalized +7 numbers")
        if not self.live_allowed_numbers and not self.allow_all_consented_numbers:
            raise ValueError("Set LIVE_ALLOWED_NUMBERS for pilot, or explicitly enable ALLOW_ALL_CONSENTED_NUMBERS")

    def public(self):
        return {
            "mode": self.mode,
            "max_concurrent": self.max_concurrent,
            "daily_attempt_limit": self.daily_attempt_limit,
            "max_duration_sec": self.max_duration_sec,
            "ringing_timeout_sec": self.ringing_timeout_sec,
            "call_window_start": self.call_window_start,
            "call_window_end": self.call_window_end,
            "default_timezone": self.default_timezone,
            "allow_all_consented_numbers": self.allow_all_consented_numbers,
            "live_allowlist_count": len(self.live_allowed_numbers),
        }
