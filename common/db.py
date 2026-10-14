"""Conexao com o PostgreSQL com retry. Import de psycopg feito sob demanda."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

from common.config import Settings
from common.log import get_logger
from common.runtime import ShutdownFlag, sleep_or_shutdown

if TYPE_CHECKING:  # pragma: no cover
    import psycopg

log = get_logger(__name__)


def connect(
    settings: Settings,
    timeout_s: float = 120.0,
    autocommit: bool = False,
    shutdown: ShutdownFlag | None = None,
) -> "psycopg.Connection":
    """Abre conexao, tentando novamente com backoff exponencial ate ``timeout_s``.

    Com ``shutdown``, a espera e interrompida por SIGTERM (levanta ShutdownRequested).
    """
    import psycopg

    deadline = time.monotonic() + timeout_s
    delay = 1.0
    while True:
        try:
            conn = psycopg.connect(settings.postgres_dsn, autocommit=autocommit, connect_timeout=5)
            log.info("postgres conectado", host=settings.postgres_host, db=settings.postgres_db)
            return conn
        except psycopg.OperationalError as exc:
            if time.monotonic() > deadline:
                raise
            log.warning("aguardando postgres", error=str(exc).strip(), retry_in_s=delay)
            sleep_or_shutdown(delay, shutdown)
            delay = min(delay * 2, 10.0)
