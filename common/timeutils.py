"""Utilitarios de tempo. Todos os datetimes do projeto sao timezone-aware em UTC."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def parse_ts(value: str | datetime) -> datetime:
    """Converte ISO 8601 (com 'Z', offset ou sem fuso) em datetime UTC aware.

    Strings sem fuso sao interpretadas como UTC (a API documenta 'ta' em UTC).
    """
    if isinstance(value, datetime):
        dt = value
    else:
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def to_iso(dt: datetime) -> str:
    """Serializa em ISO 8601 UTC com sufixo 'Z' e precisao de segundos (ou micro, se houver)."""
    dt = parse_ts(dt)
    spec = "microseconds" if dt.microsecond else "seconds"
    return dt.isoformat(timespec=spec).replace("+00:00", "Z")


def floor_to_window(ts: datetime, minutes: int) -> datetime:
    """Inicio da janela fixa (tumbling) de ``minutes`` minutos que contem ``ts``."""
    ts = parse_ts(ts)
    floored_minute = (ts.minute // minutes) * minutes
    return ts.replace(minute=floored_minute, second=0, microsecond=0)


def window_bounds(ts: datetime, minutes: int) -> tuple[datetime, datetime]:
    start = floor_to_window(ts, minutes)
    return start, start + timedelta(minutes=minutes)
