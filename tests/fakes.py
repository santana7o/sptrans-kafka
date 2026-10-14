"""Duplas de teste (consumer, mensagem, producer Kafka, relogio) usadas em varios arquivos."""

from __future__ import annotations

from typing import Any


class FakeMsg:
    """Imita ``confluent_kafka.Message``."""

    def __init__(self, value: Any, offset: int, partition: int = 0, err: Any = None,
                 topic: str = "bus-positions-raw") -> None:
        self._value, self._offset, self._partition, self._err, self._topic = value, offset, partition, err, topic

    def value(self) -> Any:
        return self._value

    def error(self) -> Any:
        return self._err

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset


class FakeKafkaError:
    def __init__(self, code: int = -999, text: str = "boom") -> None:
        self._code, self._text = code, text

    def code(self) -> int:
        return self._code

    def __str__(self) -> str:
        return self._text


class FakeConsumer:
    """Devolve os lotes pre-definidos, um por chamada de ``consume``; depois, lista vazia."""

    def __init__(self, chunks: list[list[FakeMsg]] | None = None) -> None:
        self.chunks = list(chunks or [])
        self.calls: list[tuple[int, float]] = []
        self.closed = False
        self.committed: list[list[tuple[str, int, int]]] = []

    def consume(self, num_messages: int = 1, timeout: float = -1) -> list[FakeMsg]:
        self.calls.append((num_messages, timeout))
        return self.chunks.pop(0) if self.chunks else []

    def commit(self, offsets=None, asynchronous: bool = True) -> None:  # noqa: ARG002
        self.committed.append([(o.topic, o.partition, o.offset) for o in offsets])

    def close(self) -> None:
        self.closed = True


class FakeProducer:
    """Imita ``confluent_kafka.Producer``: guarda o que foi produzido e chama os callbacks no flush."""

    def __init__(self, buffer_errors: int = 0, delivery_error: Any = None, remaining_after_flush: int = 0) -> None:
        self.produced: list[tuple[str, bytes, bytes]] = []
        self.polls: list[float] = []
        self.flushes: list[float] = []
        self._buffer_errors = buffer_errors
        self._delivery_error = delivery_error
        self._remaining = remaining_after_flush
        self._callbacks: list[Any] = []

    def produce(self, topic: str, key: bytes = b"", value: bytes = b"", on_delivery=None) -> None:
        if self._buffer_errors > 0:
            self._buffer_errors -= 1
            raise BufferError("fila local cheia")
        self.produced.append((topic, key, value))
        if on_delivery is not None:
            self._callbacks.append(on_delivery)

    def poll(self, timeout: float = 0) -> int:
        self.polls.append(timeout)
        return 0

    def flush(self, timeout: float = -1) -> int:
        self.flushes.append(timeout)
        for cb in self._callbacks:
            cb(self._delivery_error, FakeMsg(None, 0))
        self._callbacks.clear()
        return self._remaining


class StepClock:
    """Relogio monotono falso: cada chamada avanca ``step`` segundos (ou fica parado com step=0)."""

    def __init__(self, start: float = 0.0, step: float = 0.0) -> None:
        self.t = start
        self.step = step

    def __call__(self) -> float:
        self.t += self.step
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds
