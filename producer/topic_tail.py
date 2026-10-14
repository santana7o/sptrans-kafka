"""Leitura do fim do topico bus-positions-raw (ancora do replay).

Le a ultima mensagem de cada particao com um Consumer temporario (sem subscribe, sem
commit de offsets) e devolve o maior instante de evento ja publicado. Isolado da logica
pura de ``producer.replay`` para que a decisao da ancora seja testavel sem Kafka.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime
from typing import Any

from common.config import Settings
from common.log import get_logger
from common.timeutils import parse_ts

log = get_logger(__name__)

_RDKAFKA_LOGGER = logging.getLogger("librdkafka")


def message_time_bound(raw: bytes | str | None) -> datetime | None:
    """Maior instante de evento de uma mensagem de BusPosition: max(event_ts, collected_at).

    ``collected_at`` e o instante da coleta, nunca anterior a nenhum ``ta`` da mesma coleta;
    usa-lo como limite cobre leituras da ultima coleta que caem em outras posicoes da
    particao. Mensagem ilegivel devolve None.
    """
    if raw is None:
        return None
    try:
        data = json.loads(raw)
    except (TypeError, ValueError):
        return None
    if not isinstance(data, dict):
        return None
    stamps = []
    for field in ("event_ts", "collected_at"):
        try:
            stamps.append(parse_ts(data[field]))
        except (KeyError, TypeError, ValueError, AttributeError):
            continue
    return max(stamps) if stamps else None


def read_last_event_ts(settings: Settings, timeout_s: float = 5.0) -> datetime | None:
    """Maior instante de evento entre as ultimas mensagens de cada particao do topico raw.

    Topico vazio ou inexistente devolve None. Erros de conexao sao propagados (o chamador
    decide o que fazer).
    """
    from confluent_kafka import Consumer, KafkaException, TopicPartition

    topic = settings.topic_raw
    deadline = time.monotonic() + timeout_s
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": "producer-replay-anchor",
            "client.id": "producer-replay-anchor",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        },
        logger=_RDKAFKA_LOGGER,
    )
    try:
        metadata = consumer.list_topics(topic, timeout=timeout_s)
        topic_meta = metadata.topics.get(topic)
        if topic_meta is None or topic_meta.error is not None or not topic_meta.partitions:
            return None
        targets: list[Any] = []
        for partition in sorted(topic_meta.partitions):
            remaining = max(0.5, deadline - time.monotonic())
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=remaining)
            if high > low:
                targets.append(TopicPartition(topic, partition, high - 1))
        if not targets:
            return None
        consumer.assign(targets)
        pending = {tp.partition for tp in targets}
        latest: datetime | None = None
        while pending and time.monotonic() < deadline:
            msg = consumer.poll(min(1.0, max(0.1, deadline - time.monotonic())))
            if msg is None:
                continue
            if msg.error() is not None:
                if msg.error().fatal():
                    raise KafkaException(msg.error())
                continue
            pending.discard(msg.partition())
            bound = message_time_bound(msg.value())
            if bound is not None and (latest is None or bound > latest):
                latest = bound
        if pending:
            log.warning("particoes sem ultima mensagem lida no prazo", topic=topic, partitions=sorted(pending))
        return latest
    finally:
        consumer.close()
