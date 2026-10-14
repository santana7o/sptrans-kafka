"""Publicacao de BusPosition no topico bus-positions-raw (chave = vehicle_id)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Sequence

from common.log import get_logger
from common.schemas import BusPosition

if TYPE_CHECKING:  # pragma: no cover
    from confluent_kafka import Producer

log = get_logger(__name__)


class Publisher:
    """Envia lotes de BusPosition. ``flush`` ao fim de cada coleta.

    O producer e injetado (``common.kafka_utils.make_producer`` em producao, um falso nos
    testes). Falhas de entrega sao contadas no callback e logadas em um unico resumo por
    chamada de ``publish`` (quantidade + primeiro erro), e nao uma linha por mensagem
    (com o broker fora seriam milhares de linhas por coleta).
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
        self._batch_failed = 0
        self._first_error: str | None = None

    def _on_delivery(self, err: Any, msg: Any) -> None:  # noqa: ARG002 - assinatura do librdkafka
        if err is not None:
            self.failed += 1
            self._batch_failed += 1
            if self._first_error is None:
                self._first_error = str(err)
        else:
            self.delivered += 1

    def _log_failures(self) -> None:
        """Loga o resumo das falhas de entrega acumuladas desde o ultimo resumo e zera."""
        if self._batch_failed:
            log.error(
                "falhas na entrega ao kafka",
                topic=self.topic,
                failed=self._batch_failed,
                first_error=self._first_error,
            )
        self._batch_failed = 0
        self._first_error = None

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
        self._log_failures()
        return queued
