"""Fabricas de clientes Kafka com configuracao padrao do projeto.

O import de ``confluent_kafka`` e feito dentro das funcoes para que os modulos de
logica pura possam ser testados sem a biblioteca nativa instalada.
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Iterable

from common.config import Settings
from common.log import get_logger
from common.runtime import ShutdownFlag, sleep_or_shutdown

if TYPE_CHECKING:  # pragma: no cover
    from confluent_kafka import Consumer, Message, Producer

log = get_logger(__name__)

# Logs internos do librdkafka passam pelo logging do Python (saida JSON padronizada).
_RDKAFKA_LOGGER = logging.getLogger("librdkafka")


def make_producer(settings: Settings, client_id: str) -> "Producer":
    """Producer idempotente (acks=all), com compressao e pequeno linger para lotes."""
    from confluent_kafka import Producer

    return Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "client.id": client_id,
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "lz4",
            "linger.ms": 50,
            "batch.num.messages": 10000,
            "queue.buffering.max.messages": 200000,
            "message.timeout.ms": 120000,
            "logger": _RDKAFKA_LOGGER,
        }
    )


def make_consumer(settings: Settings, group_id: str, topics: Iterable[str]) -> "Consumer":
    """Consumer com auto commit DESLIGADO: o chamador commita apos processar."""
    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": group_id,
            "client.id": group_id,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "earliest",
            "session.timeout.ms": 45000,
            "max.poll.interval.ms": 300000,
        },
        logger=_RDKAFKA_LOGGER,
    )
    consumer.subscribe(list(topics))
    return consumer


def wait_for_kafka(settings: Settings, timeout_s: float = 120.0, shutdown: ShutdownFlag | None = None) -> None:
    """Bloqueia ate o broker responder (list_topics) ou estoura TimeoutError.

    Com ``shutdown``, a espera e interrompida por SIGTERM (levanta ShutdownRequested).
    """
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers, "logger": _RDKAFKA_LOGGER})
    deadline = time.monotonic() + timeout_s
    delay = 1.0
    while True:
        try:
            admin.list_topics(timeout=5)
            log.info("kafka disponivel", bootstrap=settings.kafka_bootstrap_servers)
            return
        except Exception as exc:  # noqa: BLE001 - qualquer falha de conexao vira retry
            if time.monotonic() > deadline:
                raise TimeoutError("kafka indisponivel") from exc
            log.warning("aguardando kafka", error=str(exc), retry_in_s=delay)
            sleep_or_shutdown(delay, shutdown)
            delay = min(delay * 2, 10.0)


def delivery_report(err, msg: "Message") -> None:
    """Callback padrao de entrega: loga apenas falhas."""
    if err is not None:
        log.error("falha na entrega ao kafka", topic=msg.topic(), error=str(err))


def commit_offsets(consumer: "Consumer", messages: list["Message"]) -> None:
    """Commit sincrono dos offsets (offset + 1) das mensagens ja processadas."""
    if not messages:
        return
    from confluent_kafka import TopicPartition

    latest: dict[tuple[str, int], int] = {}
    for m in messages:
        if m.error() is not None:
            continue
        key = (m.topic(), m.partition())
        latest[key] = max(latest.get(key, -1), m.offset())
    if latest:
        consumer.commit(
            offsets=[TopicPartition(t, p, o + 1) for (t, p), o in latest.items()],
            asynchronous=False,
        )
