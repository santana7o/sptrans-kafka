"""Publicacao de BusPosition no topico bus-positions-raw (chave = vehicle_id)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Sequence

from common.kafka_utils import delivery_report
from common.log import get_logger
from common.schemas import BusPosition

if TYPE_CHECKING:  # pragma: no cover
    from confluent_kafka import Producer

log = get_logger(__name__)


class Publisher:
    """Envia lotes de BusPosition. ``flush`` ao fim de cada coleta.

    O producer e injetado (``common.kafka_utils.make_producer`` em producao, um falso nos
    testes). Falhas de entrega sao contadas e logadas pelo callback.
    """

    def __init__(
        self,
        producer: "Producer",
        topic: str,
        poll_every: int = 500,
        flush_timeout_s: float = 30.0,
    ) -> None:
        self.producer = producer
        self.topic = topic
        self.poll_every = max(1, poll_every)
        self.flush_timeout_s = flush_timeout_s
        self.delivered = 0
        self.failed = 0

    def _on_delivery(self, err: Any, msg: Any) -> None:
        if err is not None:
            self.failed += 1
            delivery_report(err, msg)
        else:
            self.delivered += 1

    def publish(self, events: Sequence[BusPosition]) -> int:
        """Enfileira todos os eventos e faz flush. Retorna quantos foram enfileirados."""
        queued = 0
        for i, event in enumerate(events):
            key, value = event.key(), event.to_json_bytes()
            while True:
                try:
                    self.producer.produce(self.topic, key=key, value=value, on_delivery=self._on_delivery)
                    break
                except BufferError:
                    # Fila local cheia: deixa o librdkafka drenar e tenta de novo.
                    self.producer.poll(0.5)
            queued += 1
            if (i + 1) % self.poll_every == 0:
                self.producer.poll(0)
        remaining = self.producer.flush(self.flush_timeout_s)
        if remaining:
            log.warning("flush incompleto", topic=self.topic, pending=remaining)
        return queued
