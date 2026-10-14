"""common.stream_loop (StreamLoop e KafkaPublisher) e o sink de metricas, com fakes."""

from __future__ import annotations

import re

import pytest

from common.runtime import ShutdownFlag
from common.stream_loop import KafkaPublisher, StreamLoop, decode_positions
from consumers.metrics.main import UPSERT_SQL, metric_params
from consumers.metrics.processor import MetricsProcessor
from tests.conftest import make_position
from tests.fakes import FakeConsumer, FakeKafkaError, FakeMsg, FakeProducer, StepClock


class RecordingProcessor:
    """Devolve uma saida por posicao e registra as chamadas."""

    def __init__(self, fail: bool = False) -> None:
        self.batches: list[list] = []
        self.idle_calls = 0
        self.fail = fail

    def process(self, positions):
        self.batches.append(list(positions))
        if self.fail:
            raise ValueError("bug de logica")
        return [p.vehicle_id for p in positions]

    def on_idle(self):
        self.idle_calls += 1
        return ["idle"]

    def stats(self):
        return {"batches_seen": len(self.batches)}


class FlakySink:
    def __init__(self, failures: int = 0) -> None:
        self.failures = failures
        self.attempts: list[list] = []
        self.delivered: list[list] = []
        self.resets = 0

    def deliver(self, outputs):
        self.attempts.append(list(outputs))
        if self.failures > 0:
            self.failures -= 1
            raise ConnectionError("kafka/postgres fora")
        self.delivered.append(list(outputs))

    def reset(self):
        self.resets += 1


def _msgs(*vehicles, start_offset: int = 0):
    return [FakeMsg(make_position(v, t=i).to_json_bytes(), start_offset + i) for i, v in enumerate(vehicles)]


def _loop(consumer, processor, sink, shutdown=None, events=None, clock=None, **kw):
    events = events if events is not None else []
    loop = StreamLoop(
        consumer=consumer, processor=processor, sink=sink,
        shutdown=shutdown or ShutdownFlag(install_handlers=False),
        commit_fn=lambda c, msgs: events.append(("commit", [m.offset() for m in msgs])),
        clock=clock or StepClock(), retry_initial_s=0.0, retry_max_s=0.0, **kw,
    )
    return loop, events


def test_step_processes_delivers_then_commits():
    events: list = []
    sink = FlakySink()
    original = sink.deliver
    sink.deliver = lambda outs: (events.append(("deliver", list(outs))), original(outs))
    loop, _ = _loop(FakeConsumer([_msgs("a", "b")]), RecordingProcessor(), sink, events=events)
    assert loop.step() is True
    assert events == [("deliver", ["a", "b"]), ("commit", [0, 1])]
    assert loop.totals == {"messages": 2, "malformed": 0, "outputs": 2, "batches": 1}


def test_delivery_failure_retries_outputs_without_reprocessing():
    proc, sink = RecordingProcessor(), FlakySink(failures=2)
    loop, events = _loop(FakeConsumer([_msgs("a")]), proc, sink)
    assert loop.step() is True
    assert len(proc.batches) == 1                     # lote aplicado ao estado uma unica vez
    assert sink.attempts == [["a"], ["a"], ["a"]] and sink.resets == 2
    assert events == [("commit", [0])]


def test_shutdown_during_retry_does_not_commit():
    shutdown = ShutdownFlag(install_handlers=False)
    shutdown.set()
    loop, events = _loop(FakeConsumer([_msgs("a")]), RecordingProcessor(), FlakySink(failures=5), shutdown=shutdown)
    assert loop.step() is False
    assert events == [] and loop.totals["messages"] == 0


def test_malformed_and_kafka_error_messages_are_skipped_but_committed():
    msgs = _msgs("a") + [FakeMsg(b"{x", 1), FakeMsg(b'{"vehicle_id":"1"}', 2), FakeMsg(None, 3, err=FakeKafkaError())]
    proc = RecordingProcessor()
    loop, events = _loop(FakeConsumer([msgs]), proc, FlakySink())
    assert loop.step() is True
    assert [p.vehicle_id for p in proc.batches[0]] == ["a"]
    assert events == [("commit", [0, 1, 2, 3])]
    assert loop.totals["malformed"] == 2 and loop.totals["messages"] == 4


def test_processor_exception_skips_batch_and_commits():
    loop, events = _loop(FakeConsumer([_msgs("a")]), RecordingProcessor(fail=True), FlakySink())
    assert loop.step() is True
    assert events == [("commit", [0])] and loop.totals["outputs"] == 0


def test_commit_failure_is_tolerated():
    def bad_commit(consumer, messages):
        raise RuntimeError("commit falhou")

    loop = StreamLoop(FakeConsumer([_msgs("a")]), RecordingProcessor(), FlakySink(),
                      ShutdownFlag(install_handlers=False), commit_fn=bad_commit, clock=StepClock())
    assert loop.step() is True and loop.totals["batches"] == 1


def test_idle_flush_happens_once_after_idle_period():
    clock = StepClock()
    proc, sink = RecordingProcessor(), FlakySink()
    loop, _ = _loop(FakeConsumer([_msgs("a")]), proc, sink, clock=clock, idle_flush_s=120.0)
    assert loop.step() is True
    clock.advance(119)
    loop.step()
    assert proc.idle_calls == 0
    clock.advance(1)
    loop.step()
    assert proc.idle_calls == 1 and sink.delivered[-1] == ["idle"]
    clock.advance(500)
    loop.step()
    assert proc.idle_calls == 1


def test_no_idle_flush_before_any_message():
    clock = StepClock()
    proc = RecordingProcessor()
    loop, _ = _loop(FakeConsumer([]), proc, FlakySink(), clock=clock, idle_flush_s=1.0)
    clock.advance(1000)
    loop.step()
    assert proc.idle_calls == 0


def test_run_stops_on_shutdown_and_closes_consumer():
    shutdown = ShutdownFlag(install_handlers=False)

    class StoppingConsumer(FakeConsumer):
        def consume(self, num_messages=1, timeout=-1):
            batch = super().consume(num_messages, timeout)
            if not self.chunks:
                shutdown.set()
            return batch

    consumer = StoppingConsumer([_msgs("a"), _msgs("b", start_offset=1)])
    loop, events = _loop(consumer, RecordingProcessor(), FlakySink(), shutdown=shutdown, batch_size=7)
    loop.run()
    assert consumer.closed and events == [("commit", [0]), ("commit", [1])]
    assert consumer.calls[0][0] == 7


def test_real_commit_offsets_commits_next_offset_per_partition():
    from common.kafka_utils import commit_offsets

    consumer = FakeConsumer()
    msgs = [FakeMsg(b"", 5, 0), FakeMsg(b"", 7, 0), FakeMsg(b"", 3, 2), FakeMsg(None, 99, 1, err=FakeKafkaError())]
    commit_offsets(consumer, msgs)
    assert sorted(consumer.committed[0]) == [("bus-positions-raw", 0, 8), ("bus-positions-raw", 2, 4)]
    commit_offsets(consumer, [])
    assert len(consumer.committed) == 1


def test_decode_positions_counts_malformed():
    positions, malformed = decode_positions(_msgs("a", "b") + [FakeMsg(b"[]", 9)])
    assert [p.vehicle_id for p in positions] == ["a", "b"] and malformed == 1


@pytest.mark.xfail(strict=True, reason="BUG: decode_positions so captura SchemaError; mensagem com event_ts "
                                       "numerico levanta AttributeError e derruba StreamLoop.step")
def test_poison_message_does_not_crash_stream_loop():
    poison = FakeMsg(b'{"vehicle_id":"1","line_code":1,"direction":1,"lat":-23.5,"lon":-46.6,'
                     b'"event_ts":1760443200,"collected_at":"2026-10-14T12:00:00Z"}', 1)
    loop, events = _loop(FakeConsumer([_msgs("a") + [poison]]), RecordingProcessor(), FlakySink())
    assert loop.step() is True
    assert events == [("commit", [0, 1])] and loop.totals["malformed"] == 1


def test_stream_loop_with_metrics_processor_end_to_end():
    from consumers.metrics.windows import WindowAggregator
    from common.tracker import SpeedTracker

    proc = MetricsProcessor(SpeedTracker(), WindowAggregator(5, 60))
    sink = FlakySink(failures=1)
    msgs = [FakeMsg(make_position("a", t=t).to_json_bytes(), i) for i, t in enumerate(range(0, 421, 30))]
    loop, events = _loop(FakeConsumer([msgs]), proc, sink)
    loop.step()
    assert len(sink.delivered) == 1 and len(sink.delivered[0]) == 1      # janela 12:00 fechada
    assert proc.aggregator.counters["added"] == len(msgs)                  # sem contagem dupla no retry


# --------------------------------------------------------------------------- KafkaPublisher


def test_kafka_publisher_produces_key_and_value():
    producer = FakeProducer()
    events = [make_position("a"), make_position("b")]
    KafkaPublisher(producer, "bus-alerts", flush_timeout_s=3).publish(events)
    assert [(t, k) for t, k, _ in producer.produced] == [("bus-alerts", b"a"), ("bus-alerts", b"b")]
    assert producer.produced[0][2] == events[0].to_json_bytes() and producer.flushes == [3]


def test_kafka_publisher_retries_on_buffer_error():
    producer = FakeProducer(buffer_errors=2)
    KafkaPublisher(producer, "t").publish([make_position()])
    assert len(producer.produced) == 1 and producer.polls.count(0.5) == 2


def test_kafka_publisher_raises_on_delivery_error():
    with pytest.raises(RuntimeError, match="falhas de entrega"):
        KafkaPublisher(FakeProducer(delivery_error="timeout"), "t").publish([make_position()])


def test_kafka_publisher_raises_when_flush_leaves_messages():
    with pytest.raises(RuntimeError, match="nao entregues"):
        KafkaPublisher(FakeProducer(remaining_after_flush=1), "t").publish([make_position()])


def test_metric_params_match_upsert_placeholders():
    from consumers.metrics.windows import WindowAggregator

    agg = WindowAggregator()
    from common.tracker import TrackedReading

    agg.add(TrackedReading(make_position(), None, None))
    [metric] = agg.flush_all()
    assert set(re.findall(r"%\((\w+)\)s", UPSERT_SQL)) == set(metric_params(metric))
    assert "ON CONFLICT (line_code, direction, window_start) DO UPDATE" in UPSERT_SQL
