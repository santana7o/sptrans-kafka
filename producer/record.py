"""Gravador de coletas reais: ``python -m producer.record``.

Grava ``RECORD_COLLECTIONS`` respostas de GET /Posicao em
``RECORD_OUTPUT_DIR/recording_<UTC>.jsonl.gz`` (formato da secao 2 de CONTRACTS),
uma linha por coleta, no intervalo de ``POLL_INTERVAL_SECONDS``. Exige SPTRANS_TOKEN.
O arquivo gerado pode ser reproduzido com ``MODE=replay REPLAY_FILE=<arquivo>``.
"""

from __future__ import annotations

import gzip
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from common.config import ConfigError, load_settings
from common.log import get_logger, setup_logging
from common.runtime import ShutdownFlag
from common.timeutils import to_iso, utcnow
from producer.live import next_wait_seconds
from producer.olhovivo import OlhoVivoClient, OlhoVivoError

log = get_logger("producer.record")


def recording_path(output_dir: str | Path, now: datetime) -> Path:
    return Path(output_dir) / f"recording_{now:%Y%m%dT%H%M%SZ}.jsonl.gz"


def collection_line(collected_at: datetime, payload: dict[str, Any]) -> str:
    """Uma linha JSON do arquivo de sample (sem quebra de linha)."""
    record = {"collected_at": to_iso(collected_at), "payload": payload}
    return json.dumps(record, ensure_ascii=False, separators=(",", ":"))


def main() -> int:
    try:
        settings = load_settings()
    except ConfigError as exc:
        setup_logging("recorder", "INFO")
        log.error("configuracao invalida", error=str(exc))
        return 2
    setup_logging("recorder", settings.log_level)
    if not settings.sptrans_token:
        log.error("o gravador exige SPTRANS_TOKEN (defina no .env)")
        return 2
    if settings.record_collections <= 0:
        log.error("RECORD_COLLECTIONS deve ser positivo", value=settings.record_collections)
        return 2

    shutdown = ShutdownFlag()
    client = OlhoVivoClient.from_settings(settings, shutdown)
    path = recording_path(settings.record_output_dir, utcnow())
    path.parent.mkdir(parents=True, exist_ok=True)
    log.info("gravacao iniciada", file=str(path), collections=settings.record_collections,
             interval_s=settings.poll_interval_seconds)

    written = 0
    failures = 0
    try:
        with gzip.open(path, "wt", encoding="utf-8") as fh:
            for attempt in range(settings.record_collections):
                if shutdown.is_set():
                    break
                started = time.monotonic()
                try:
                    payload = client.get_positions()
                    collected_at = utcnow()
                    fh.write(collection_line(collected_at, payload) + "\n")
                    fh.flush()
                    written += 1
                    vehicles = sum(len(line.get("vs") or []) for line in payload["l"] if isinstance(line, dict))
                    log.info("coleta gravada", collection=written, total=settings.record_collections,
                             lines=len(payload["l"]), vehicles=vehicles)
                except OlhoVivoError as exc:
                    failures += 1
                    log.error("falha na coleta", error=str(exc), error_type=type(exc).__name__)
                if attempt < settings.record_collections - 1:
                    shutdown.wait(next_wait_seconds(settings.poll_interval_seconds, time.monotonic() - started))
    finally:
        client.close()

    if written == 0:
        path.unlink(missing_ok=True)
        log.error("nenhuma coleta gravada", failures=failures)
        return 1
    log.info("gravacao concluida", file=str(path), collections=written, failures=failures,
             size_bytes=path.stat().st_size)
    return 0


if __name__ == "__main__":
    sys.exit(main())
