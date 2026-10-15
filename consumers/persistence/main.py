"""Consumer de persistencia: bus-positions-raw -> tabela bus_positions, em lote.

Execucao: ``python -m consumers.persistence.main``.

Ciclo de cada lote:
1. ``consume`` ate ``PERSISTENCE_BATCH_SIZE`` mensagens ou ``PERSISTENCE_FLUSH_SECONDS``;
2. desserializa com ``BusPosition.from_json_bytes`` (malformada: log e ignora, mas o
   offset dela tambem e commitado no fim do lote);
3. grava no Postgres e faz commit da transacao;
4. SO ENTAO commita os offsets no Kafka (``common.kafka_utils.commit_offsets``).

Falha de banco: rollback, reconexao com backoff e nova tentativa do MESMO lote, sem
commitar offsets. Como a gravacao e idempotente (ON CONFLICT DO NOTHING), reprocessar
apos uma queda nao duplica linhas (at-least-once, DECISIONS D07).

``BatchPersister`` recebe consumer, writer e funcao de commit injetados, para que a
montagem do lote e o caminho de commit sejam testaveis sem Kafka e sem Postgres.
"""

from __future__ import annotations

import time
from typing import Any, Callable, Protocol, Sequence

from common.config import Settings, load_settings
from common.kafka_utils import commit_offsets, make_consumer, wait_for_kafka
from common.log import get_logger, setup_logging
from common.runtime import ShutdownFlag, ShutdownRequested
from common.schemas import BusPosition
from common.stream_loop import decode_positions, is_partition_eof

GROUP_ID = "persistence-consumer"

log = get_logger(__name__)


class PositionWriter(Protocol):
    """O que o loop precisa de um writer (ver ``writer.PostgresPositionWriter``)."""

    def write(self, positions: Sequence[BusPosition]) -> int: ...

    def reset(self) -> None: ...


class MessageConsumer(Protocol):
    """Subconjunto de ``confluent_kafka.Consumer`` usado aqui."""

    def consume(self, num_messages: int = 1, timeout: float = -1) -> list[Any]: ...

    def close(self) -> None: ...



class BatchPersister:
    """Loop de lote: consume -> decode -> grava/commit no banco -> commit de offsets."""

    def __init__(
        self,
        consumer: MessageConsumer,
        writer: PositionWriter,
        shutdown: ShutdownFlag,
        batch_size: int,
        flush_seconds: float,
        commit_fn: Callable[[Any, list[Any]], None] = commit_offsets,
        clock: Callable[[], float] = time.monotonic,
        retry_initial_s: float = 1.0,
        retry_max_s: float = 30.0,
        poll_timeout_s: float = 1.0,
    ) -> None:
        self._consumer = consumer
        self._writer = writer
        self._shutdown = shutdown
        self._batch_size = batch_size
        self._flush_seconds = flush_seconds
        self._commit_fn = commit_fn
        self._clock = clock
        self._retry_initial_s = retry_initial_s
        self._retry_max_s = retry_max_s
        self._poll_timeout_s = poll_timeout_s
        self.totals = {"messages": 0, "malformed": 0, "inserted": 0, "duplicates": 0, "batches": 0}

    def collect_batch(self) -> list[Any]:
        """Le mensagens ate encher o lote, estourar o tempo de flush ou haver shutdown."""
        batch: list[Any] = []
        deadline = self._clock() + self._flush_seconds
        while len(batch) < self._batch_size and not self._shutdown.is_set():
            remaining = deadline - self._clock()
            if remaining <= 0:
                break
            msgs = self._consumer.consume(
                num_messages=self._batch_size - len(batch),
                timeout=min(remaining, self._poll_timeout_s),
            )
            for msg in msgs or []:
                err = msg.error()
                if err is not None and not is_partition_eof(err):
                    log.error("erro do kafka na mensagem", error=str(err))
            batch.extend(msgs or [])
        return batch

    def process_batch(self, batch: list[Any]) -> bool:
        """Grava o lote e commita offsets. Retorna False se interrompido por shutdown
        antes de conseguir gravar (offsets nao commitados; o lote sera reprocessado)."""
        if not batch:
            return True
        started = self._clock()
        positions, malformed = decode_positions(batch)

        delay = self._retry_initial_s
        attempts = 0
        while True:
            attempts += 1
            write_started = self._clock()
            try:
                inserted = self._writer.write(positions)
                break
            except Exception as exc:  # noqa: BLE001 - falha de infraestrutura: retry
                log.error(
                    "falha ao gravar lote, offsets nao commitados",
                    attempt=attempts,
                    positions=len(positions),
                    error=str(exc).strip(),
                    retry_in_s=delay,
                )
                try:
                    self._writer.reset()
                except Exception as reset_exc:  # noqa: BLE001
                    log.warning("falha ao reiniciar writer", error=str(reset_exc).strip())
                if self._shutdown.wait(delay):
                    log.warning("encerrando com lote nao gravado; sera reprocessado", messages=len(batch))
                    return False
                delay = min(delay * 2, self._retry_max_s)
        write_ms = (self._clock() - write_started) * 1000.0

        try:
            self._commit_fn(self._consumer, batch)
        except Exception as exc:  # noqa: BLE001 - dados ja gravados; reprocessar e idempotente
            log.error("falha no commit de offsets (dados ja gravados)", error=str(exc).strip())

        duplicates = len(positions) - inserted
        self.totals["messages"] += len(batch)
        self.totals["malformed"] += malformed
        self.totals["inserted"] += inserted
        self.totals["duplicates"] += duplicates
        self.totals["batches"] += 1
        log.info(
            "lote persistido",
            messages=len(batch),
            positions=len(positions),
            malformed=malformed,
            inserted=inserted,
            duplicates=duplicates,
            attempts=attempts,
            write_ms=round(write_ms, 1),
            batch_ms=round((self._clock() - started) * 1000.0, 1),
            total_inserted=self.totals["inserted"],
        )
        return True

    def run(self) -> None:
        """Loop principal ate o shutdown; o lote em montagem e gravado antes de sair."""
        try:
            while not self._shutdown.is_set():
                batch = self.collect_batch()
                # Mesmo com shutdown, o lote parcial ja lido e gravado (flush final).
                self.process_batch(batch)
        finally:
            log.info("encerrando consumer de persistencia", **self.totals)
            try:
                self._consumer.close()
            except Exception as exc:  # noqa: BLE001
                log.warning("erro ao fechar consumer", error=str(exc))


def build_persister(settings: Settings, shutdown: ShutdownFlag) -> tuple[BatchPersister, Any]:
    """Cria writer (ja conectado) e consumer reais. Retorna (persister, writer).

    As esperas por Postgres e Kafka sao interrompidas por SIGTERM (``ShutdownRequested``);
    nesse caso o writer ja aberto e fechado antes de propagar.
    """
    from consumers.persistence.writer import PostgresPositionWriter

    writer = PostgresPositionWriter(settings, shutdown=shutdown)
    try:
        writer.connect(timeout_s=120.0)
        wait_for_kafka(settings, shutdown=shutdown)
    except BaseException:
        writer.close()
        raise
    consumer = make_consumer(settings, GROUP_ID, [settings.topic_raw])
    persister = BatchPersister(
        consumer=consumer,
        writer=writer,
        shutdown=shutdown,
        batch_size=settings.persistence_batch_size,
        flush_seconds=settings.persistence_flush_seconds,
    )
    return persister, writer


def main() -> None:
    settings = load_settings()
    setup_logging("persistence", settings.log_level)
    log.info(
        "iniciando consumer de persistencia",
        topic=settings.topic_raw,
        group=GROUP_ID,
        batch_size=settings.persistence_batch_size,
        flush_seconds=settings.persistence_flush_seconds,
    )
    shutdown = ShutdownFlag()
    try:
        persister, writer = build_persister(settings, shutdown)
    except ShutdownRequested:
        log.info("encerramento solicitado durante a inicializacao; nada foi consumido")
        return
    try:
        persister.run()
    finally:
        writer.close()


if __name__ == "__main__":
    main()
