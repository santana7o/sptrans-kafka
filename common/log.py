"""Logging estruturado em JSON (uma linha por evento) usando apenas a stdlib.

Uso:
    from common.log import setup_logging, get_logger
    setup_logging("metrics", "INFO")
    log = get_logger(__name__)
    log.info("janela emitida", line_code=1234, vehicles=12)
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any

_SERVICE = "app"
_RESERVED = set(logging.LogRecord("", 0, "", 0, "", None, None).__dict__) | {"message", "asctime"}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "service": _SERVICE,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        fields = getattr(record, "fields", None)
        if isinstance(fields, dict):
            payload.update(fields)
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str, ensure_ascii=False)


def setup_logging(service: str, level: str = "INFO") -> None:
    """Configura o root logger para emitir JSON em stdout. Idempotente."""
    global _SERVICE
    _SERVICE = service
    root = logging.getLogger()
    root.handlers.clear()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root.addHandler(handler)
    root.setLevel(level.upper())
    # Bibliotecas ruidosas
    for noisy in ("urllib3", "watchdog", "tornado"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


class StructLogger:
    """Wrapper fino sobre logging.Logger que aceita campos como kwargs."""

    def __init__(self, logger: logging.Logger) -> None:
        self._logger = logger

    def _log(self, level: int, msg: str, exc_info: bool = False, **fields: Any) -> None:
        if self._logger.isEnabledFor(level):
            self._logger.log(level, msg, exc_info=exc_info, extra={"fields": fields}, stacklevel=3)

    def debug(self, msg: str, **fields: Any) -> None:
        self._log(logging.DEBUG, msg, **fields)

    def info(self, msg: str, **fields: Any) -> None:
        self._log(logging.INFO, msg, **fields)

    def warning(self, msg: str, **fields: Any) -> None:
        self._log(logging.WARNING, msg, **fields)

    def error(self, msg: str, **fields: Any) -> None:
        self._log(logging.ERROR, msg, **fields)

    def exception(self, msg: str, **fields: Any) -> None:
        """Loga em nivel ERROR com o traceback da excecao corrente."""
        self._log(logging.ERROR, msg, exc_info=True, **fields)


def get_logger(name: str) -> StructLogger:
    return StructLogger(logging.getLogger(name))
