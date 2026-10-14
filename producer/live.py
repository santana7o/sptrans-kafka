"""Modo live: coleta /Posicao a cada POLL_INTERVAL_SECONDS e publica no Kafka."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

from common.config import Settings
from common.log import get_logger
from common.runtime import ShutdownFlag
from common.schemas import SOURCE_LIVE
from common.timeutils import utcnow
from producer.dedup import Deduplicator
from producer.olhovivo import OlhoVivoClient, OlhoVivoError
from producer.pipeline import process_collection
from producer.publisher import Publisher

if TYPE_CHECKING:  # pragma: no cover
    from confluent_kafka import Producer

log = get_logger(__name__)


def next_wait_seconds(interval_s: float, elapsed_s: float) -> float:
    """Tempo ate a proxima coleta, descontando o tempo gasto nesta (nunca negativo)."""
    return max(0.0, interval_s - elapsed_s)


def run_live(
    settings: Settings,
    producer: "Producer",
    shutdown: ShutdownFlag,
    client: OlhoVivoClient | None = None,
) -> None:
    """Loop principal do modo live. Erros de uma coleta sao logados e o loop segue."""
    client = client or OlhoVivoClient.from_settings(settings, shutdown)
    publisher = Publisher(producer, settings.topic_raw)
    dedup = Deduplicator()
    log.info("modo live iniciado", interval_s=settings.poll_interval_seconds, topic=settings.topic_raw)
    collections = 0
    failures = 0
    try:
        while not shutdown.is_set():
            started = time.monotonic()
            try:
                payload = client.get_positions()
                api_ms = round((time.monotonic() - started) * 1000, 1)
                collected_at = utcnow()
                stats = process_collection(payload, collected_at, SOURCE_LIVE, dedup, publisher)
                collections += 1
                log.info(
                    "coleta publicada",
                    collection=collections,
                    reference_time=payload.get("hr"),
                    lines=stats.lines,
                    vehicles=stats.vehicles,
                    published=stats.published,
                    duplicates=stats.duplicates,
                    malformed=stats.malformed,
                    api_latency_ms=api_ms,
                    publish_ms=stats.publish_ms,
                    delivery_failures=publisher.failed,
                )
            except OlhoVivoError as exc:
                failures += 1
                log.error("falha na coleta", error=str(exc), error_type=type(exc).__name__, failures=failures)
            except Exception as exc:  # noqa: BLE001 - uma coleta ruim nao derruba o loop
                failures += 1
                log.exception("erro inesperado na coleta", error_type=type(exc).__name__, failures=failures)
            shutdown.wait(next_wait_seconds(settings.poll_interval_seconds, time.monotonic() - started))
    finally:
        client.close()
        log.info("modo live encerrado", collections=collections, failures=failures)
