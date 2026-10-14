"""Loop de processamento compartilhado pelos consumers de metricas e de alertas.

Ciclo de cada lote:
1. ``consume`` de ate ``batch_size`` mensagens (ou ``poll_timeout_s``);
2. desserializa em BusPosition (malformada: log e ignora; o offset dela e commitado);
3. aplica o lote ao estado em memoria via ``processor.process`` -> lista de saidas;
4. entrega as saidas: publica no Kafka (com flush) e grava no Postgres;
5. SO ENTAO commita os offsets.

Reprocessamento sem contar duas vezes: o lote e aplicado ao estado UMA vez (passo 3) e
as saidas ficam em ``pending`` ate a entrega dar certo. Se publicar ou gravar falhar,
apenas as saidas pendentes sao reenviadas (com backoff), sem reaplicar mensagens ao
estado. Publicacao e gravacao sao idempotentes (chaves deterministicas, upsert/ON
CONFLICT), entao reenviar depois de uma falha parcial nao duplica dados.
"""

from __future__ import annotations

import time
from typing import Any, Callable, Generic, Protocol, Sequence, TypeVar

from common.kafka_utils import commit_offsets
from common.log import get_logger
from common.runtime import ShutdownFlag
from common.schemas import BusPosition, SchemaError

log = get_logger(__name__)

Out = TypeVar("Out")


class Processor(Protocol[Out]):
    """Logica de negocio com estado em memoria."""

    def process(self, positions: Sequence[BusPosition]) -> list[Out]: ...

    def on_idle(self) -> list[Out]: ...

    def stats(self) -> dict[str, Any]: ...


class Sink(Protocol[Out]):
    """Destino das saidas (Kafka + Postgres). Deve ser idempotente."""

    def deliver(self, outputs: Sequence[Out]) -> None: ...

    def reset(self) -> None: ...


def decode_positions(messages: Sequence[Any]) -> tuple[list[BusPosition], int]:
    """Desserializa mensagens Kafka. Retorna (posicoes, malformadas)."""
    positions: list[BusPosition] = []
    malformed = 0
    for msg in messages:
        if msg.error() is not None:
            continue
        try:
            positions.append(BusPosition.from_json_bytes(msg.value()))
        except SchemaError as exc:
            malformed += 1
            log.warning("mensagem malformada ignorada", topic=msg.topic(),
                        partition=msg.partition(), offset=msg.offset(), error=str(exc))
    return positions, malformed


class StreamLoop(Generic[Out]):
    def __init__(
        self,
        consumer: Any,
        processor: Processor[Out],
        sink: Sink[Out],
        shutdown: ShutdownFlag,
        batch_size: int = 500,
        poll_timeout_s: float = 1.0,
        idle_flush_s: float = 120.0,
        stats_every_s: float = 60.0,
        commit_fn: Callable[[Any, list[Any]], None] = commit_offsets,
        clock: Callable[[], float] = time.monotonic,
        retry_initial_s: float = 1.0,
        retry_max_s: float = 30.0,
    ) -> None:
        self._consumer = consumer
        self._processor = processor
        self._sink = sink
        self._shutdown = shutdown
        self._batch_size = batch_size
        self._poll_timeout_s = poll_timeout_s
        self._idle_flush_s = idle_flush_s
        self._stats_every_s = stats_every_s
        self._commit_fn = commit_fn
        self._clock = clock
        self._retry_initial_s = retry_initial_s
        self._retry_max_s = retry_max_s
        self._last_message_at = clock()
        self._last_stats_at = clock()
        self._idle_flushed = True
        self.totals = {"messages": 0, "malformed": 0, "outputs": 0, "batches": 0}

    def deliver(self, outputs: list[Out]) -> bool:
        """Entrega com retry. Retorna False se o shutdown interromper as tentativas."""
        if not outputs:
            return True
        delay = self._retry_initial_s
        attempt = 0
        while True:
            attempt += 1
            try:
                self._sink.deliver(outputs)
                self.totals["outputs"] += len(outputs)
                return True
            except Exception as exc:  # noqa: BLE001 - falha de infraestrutura: retry
                log.error("falha ao entregar saidas, offsets nao commitados",
                          attempt=attempt, outputs=len(outputs), error=str(exc).strip(), retry_in_s=delay)
                try:
                    self._sink.reset()
                except Exception as reset_exc:  # noqa: BLE001
                    log.warning("falha ao reiniciar destino", error=str(reset_exc).strip())
                if self._shutdown.wait(delay):
                    return False
                delay = min(delay * 2, self._retry_max_s)

    def step(self) -> bool:
        """Processa um lote. Retorna False se o shutdown interrompeu uma entrega."""
        messages = self._consumer.consume(num_messages=self._batch_size, timeout=self._poll_timeout_s) or []
        now = self._clock()
        if not messages:
            if not self._idle_flushed and now - self._last_message_at >= self._idle_flush_s:
                outputs = self._processor.on_idle()
                self._idle_flushed = True
                log.info("ociosidade: emitindo estado aberto", outputs=len(outputs))
                return self.deliver(outputs)
            self._maybe_log_stats(now)
            return True

        self._last_message_at = now
        self._idle_flushed = False
        for msg in messages:
            err = msg.error()
            if err is not None and not _is_partition_eof(err):
                log.error("erro do kafka na mensagem", error=str(err))
        positions, malformed = decode_positions(messages)
        try:
            outputs = self._processor.process(positions)
        except Exception:  # noqa: BLE001 - bug de logica nao derruba o servico
            log.exception("erro inesperado ao processar lote; lote ignorado", messages=len(messages))
            outputs = []
        if not self.deliver(outputs):
            return False
        try:
            self._commit_fn(self._consumer, messages)
        except Exception as exc:  # noqa: BLE001 - saidas ja entregues; reprocessar e idempotente
            log.error("falha no commit de offsets", error=str(exc).strip())
        self.totals["messages"] += len(messages)
        self.totals["malformed"] += malformed
        self.totals["batches"] += 1
        self._maybe_log_stats(now)
        return True

    def _maybe_log_stats(self, now: float) -> None:
        if now - self._last_stats_at >= self._stats_every_s:
            self._last_stats_at = now
            log.info("estatisticas", **self.totals, **self._processor.stats())

    def run(self) -> None:
        try:
            while not self._shutdown.is_set():
                if not self.step():
                    log.warning("encerrando com saidas nao entregues; mensagens serao reprocessadas")
                    break
        finally:
            log.info("encerrando consumer", **self.totals, **self._processor.stats())
            try:
                self._consumer.close()
            except Exception as exc:  # noqa: BLE001
                log.warning("erro ao fechar consumer", error=str(exc))


def _is_partition_eof(error: Any) -> bool:
    try:
        from confluent_kafka import KafkaError

        return error.code() == KafkaError._PARTITION_EOF
    except Exception:  # noqa: BLE001
        return False


class KafkaPublisher:
    """Publica eventos (com ``key()`` e ``to_json_bytes()``) e falha se alguma entrega falhar."""

    def __init__(self, producer: Any, topic: str, flush_timeout_s: float = 30.0) -> None:
        self._producer = producer
        self._topic = topic
        self._flush_timeout_s = flush_timeout_s

    def publish(self, events: Sequence[Any]) -> None:
        errors: list[str] = []

        def on_delivery(err, msg) -> None:  # noqa: ARG001
            if err is not None:
                errors.append(str(err))

        for event in events:
            while True:
                try:
                    self._producer.produce(self._topic, key=event.key(), value=event.to_json_bytes(),
                                           on_delivery=on_delivery)
                    break
                except BufferError:
                    self._producer.poll(0.5)
            self._producer.poll(0)
        remaining = self._producer.flush(self._flush_timeout_s)
        if remaining:
            raise RuntimeError(f"{remaining} mensagens nao entregues ao kafka apos flush")
        if errors:
            raise RuntimeError(f"{len(errors)} falhas de entrega ao kafka: {errors[0]}")
