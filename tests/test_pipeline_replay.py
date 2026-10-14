"""Ponta a ponta em processo (sem broker): sample -> caminho do producer -> logica dos consumers.

Reproduz o que a stack faz em modo replay: rebase (D04), flatten, dedup e serializacao
como no topico bus-positions-raw; depois alimenta os consumers de metricas, alertas e
persistencia pelos mesmos loops de producao (StreamLoop / BatchPersister) com Kafka,
Postgres e commit falsos, em lotes de 500 mensagens.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import timedelta

import pytest

from common.runtime import ShutdownFlag
from common.schemas import SOURCE_REPLAY, BusPosition, flatten_posicao
from common.stream_loop import StreamLoop
from common.terminals import is_near_terminal
from common.tracker import SpeedTracker
from consumers.alerts.detector import StoppedVehicleDetector
from consumers.alerts.main import AlertsProcessor
from consumers.metrics.processor import MetricsProcessor
from consumers.metrics.windows import WindowAggregator
from consumers.persistence.main import BatchPersister
from producer.dedup import Deduplicator
from producer.replay import rebase_payload, scan_file
from tests.fakes import FakeConsumer, FakeMsg, StepClock

BATCH = 500
# Deslocamento fixo (multiplo de 5 min + 137 s) para o alinhamento das janelas ser deterministico.
REBASE = timedelta(hours=4, seconds=137)

ALERT_VEHICLES = {"86775", "53291", "78285", "85920"}
TERMINAL_WAITERS = {"22908", "46920", "58928", "61050"}


@dataclass
class ReplayResult:
    readings: int
    duplicates: int
    malformed: int
    published: list[BusPosition]
    messages: list[FakeMsg]


class CollectingSink:
    def __init__(self) -> None:
        self.outputs: list = []

    def deliver(self, outputs) -> None:
        self.outputs.extend(outputs)

    def reset(self) -> None:  # pragma: no cover - sink nao falha
        pass


def _run_stream_loop(processor, messages: list[FakeMsg]):
    """Consome todas as mensagens em lotes e, no fim, dispara a ociosidade (flush do replay)."""
    chunks = [messages[i:i + BATCH] for i in range(0, len(messages), BATCH)]
    clock = StepClock()
    committed: list[int] = []
    sink = CollectingSink()
    loop = StreamLoop(FakeConsumer(chunks), processor, sink, ShutdownFlag(install_handlers=False),
                      batch_size=BATCH, idle_flush_s=120.0, clock=clock,
                      commit_fn=lambda c, msgs: committed.extend(m.offset() for m in msgs))
    for _ in chunks:
        assert loop.step() is True
    clock.advance(121)
    assert loop.step() is True
    return sink.outputs, committed, loop


@pytest.fixture(scope="module")
def replay(sample_collections) -> ReplayResult:
    dedup = Deduplicator()
    readings = duplicates = malformed = 0
    published: list[BusPosition] = []
    for collected_at, payload in sample_collections:
        events, bad = flatten_posicao(rebase_payload(payload, REBASE), collected_at + REBASE, source=SOURCE_REPLAY)
        kept, dups = dedup.filter(events)
        readings += len(events)
        duplicates += dups
        malformed += bad
        published.extend(kept)
    messages = [FakeMsg(p.to_json_bytes(), i, partition=int(p.vehicle_id) % 3) for i, p in enumerate(published)]
    return ReplayResult(readings, duplicates, malformed, published, messages)


@pytest.fixture(scope="module")
def metrics_run(replay):
    processor = MetricsProcessor(SpeedTracker(), WindowAggregator(5, 60))
    outputs, committed, loop = _run_stream_loop(processor, replay.messages)
    return outputs, committed, loop, processor


@pytest.fixture(scope="module")
def alerts_run(replay):
    processor = AlertsProcessor(SpeedTracker(), StoppedVehicleDetector())
    outputs, committed, loop = _run_stream_loop(processor, replay.messages)
    return outputs, committed, loop, processor


# --------------------------------------------------------------------------- producer


def test_producer_path_counts(replay, sample_path):
    assert scan_file(sample_path).collections == 81
    assert replay.readings == 40_095
    assert replay.duplicates == 562
    assert replay.malformed == 0
    assert len(replay.published) == 40_095 - 562
    assert len({(p.vehicle_id, p.event_ts) for p in replay.published}) == len(replay.published)
    assert len({p.vehicle_id for p in replay.published}) == 495
    zero = [p for p in replay.published if p.lat == 0.0 or p.lon == 0.0]
    assert len(zero) == 12                          # coordenadas zeradas sao publicadas (dado bruto)


def test_rebase_moves_event_time(replay, sample_collections):
    first_collected = sample_collections[0][0]
    assert min(p.collected_at for p in replay.published) == first_collected + REBASE


# --------------------------------------------------------------------------- metricas


def test_metrics_windows(metrics_run, replay):
    metrics, committed, loop, processor = metrics_run
    keys = [(m.line_code, m.direction, m.window_start) for m in metrics]
    assert len(metrics) == 450
    assert len(set(keys)) == len(keys)                       # cada janela emitida uma unica vez
    assert len({(m.line_code, m.direction) for m in metrics}) == 50
    assert processor.aggregator.open_windows == 0
    assert committed == list(range(len(replay.messages)))   # todos os offsets commitados
    assert loop.totals["messages"] == len(replay.messages) and loop.totals["malformed"] == 0


def test_metrics_speed_status_matches_sample(metrics_run):
    _, _, _, processor = metrics_run
    status = processor.speed_status
    assert status["first_reading"] == 495
    assert status["invalid_coordinate"] == 12
    assert status["speed_too_high"] == 4                     # 2 saltos de GPS (ida e volta)
    assert status["ok"] == 39_022
    counters = processor.aggregator.counters
    assert counters["invalid"] == 12 and counters["late"] == 0 and counters["future"] == 0


def test_metrics_values_are_plausible(metrics_run):
    metrics, *_ = metrics_run
    for m in metrics:
        assert m.window_end - m.window_start == timedelta(minutes=5)
        assert m.window_start.minute % 5 == 0 and m.window_start.second == 0
        assert m.active_vehicles >= 1
        assert 0 <= m.accessible_vehicles <= m.active_vehicles
        assert 0.0 <= m.accessible_pct <= 100.0
        if m.avg_speed_kmh is not None:
            assert 0.0 <= m.avg_speed_kmh <= 100.0 and m.speed_samples > 0
    with_speed = [m for m in metrics if m.avg_speed_kmh is not None]
    assert len(with_speed) >= 0.95 * len(metrics)
    city = sum(m.avg_speed_kmh * m.speed_samples for m in with_speed) / sum(m.speed_samples for m in with_speed)
    assert 10.0 < city < 25.0
    assert sum(m.speed_samples for m in metrics) == 39_022


# --------------------------------------------------------------------------- alertas


def test_exactly_the_stopped_vehicles_outside_terminals_alert(alerts_run):
    alerts, committed, loop, processor = alerts_run
    assert {a.vehicle_id for a in alerts} == ALERT_VEHICLES
    assert len(alerts) == 4 and len({a.alert_id for a in alerts}) == 4
    assert not ({a.vehicle_id for a in alerts} & TERMINAL_WAITERS)
    for a in alerts:
        assert a.stopped_minutes >= 10.0 and a.last_speed_kmh < 2.0
        assert a.detected_at - a.stopped_since >= timedelta(minutes=10)
        assert not is_near_terminal(a.lat, a.lon)
        assert a.alert_id == f"{a.vehicle_id}-{a.stopped_since:%Y%m%dT%H%M%SZ}"
    assert processor.alerts_emitted == 4 and loop.totals["outputs"] == 4


def test_terminal_waiters_would_alert_without_terminal_rule(replay):
    """Prova que a ausencia de alerta nos 4 veiculos do terminal vem da regra de terminal."""
    processor = AlertsProcessor(SpeedTracker(), StoppedVehicleDetector(near_terminal=lambda lat, lon, r: False))
    alerts = []
    for i in range(0, len(replay.published), BATCH):
        alerts += processor.process(replay.published[i:i + BATCH])
    assert TERMINAL_WAITERS <= {a.vehicle_id for a in alerts}
    assert ALERT_VEHICLES <= {a.vehicle_id for a in alerts}


def test_alerts_are_deterministic_on_reprocessing(alerts_run, replay):
    alerts, *_ = alerts_run
    again, _, _ = _run_stream_loop(AlertsProcessor(SpeedTracker(), StoppedVehicleDetector()), replay.messages)
    assert [a.to_dict() for a in again] == [a.to_dict() for a in alerts]


# --------------------------------------------------------------------------- persistencia


def test_persistence_writes_every_published_reading_once(replay):
    class MemoryWriter:
        def __init__(self) -> None:
            self.rows: set = set()

        def write(self, positions) -> int:
            before = len(self.rows)
            self.rows.update((p.vehicle_id, p.event_ts) for p in positions)
            return len(self.rows) - before

        def reset(self) -> None:  # pragma: no cover
            pass

    writer = MemoryWriter()
    chunks = [replay.messages[i:i + BATCH] for i in range(0, len(replay.messages), BATCH)]
    persister = BatchPersister(FakeConsumer(chunks), writer, ShutdownFlag(install_handlers=False),
                               batch_size=BATCH, flush_seconds=2.0, commit_fn=lambda c, m: None,
                               clock=StepClock(step=0.01))
    for _ in chunks:
        assert persister.process_batch(persister.collect_batch())
    assert persister.totals["inserted"] == len(replay.published) and persister.totals["duplicates"] == 0
    # reprocessar tudo (at-least-once) nao duplica linhas
    persister2 = BatchPersister(FakeConsumer([replay.messages]), writer, ShutdownFlag(install_handlers=False),
                                batch_size=len(replay.messages), flush_seconds=2.0, commit_fn=lambda c, m: None,
                                clock=StepClock(step=0.01))
    persister2.process_batch(persister2.collect_batch())
    assert persister2.totals["inserted"] == 0 and len(writer.rows) == len(replay.published)


def test_speed_counts_are_independent_of_batching(replay):
    """Mesma entrada em lotes de 37 ou de uma vez: mesmas velocidades e alertas."""
    def run(batch: int):
        mp = MetricsProcessor(SpeedTracker(), WindowAggregator(5, 60))
        ap = AlertsProcessor(SpeedTracker(), StoppedVehicleDetector())
        out, alerts = [], []
        for i in range(0, len(replay.published), batch):
            out += mp.process(replay.published[i:i + batch])
            alerts += ap.process(replay.published[i:i + batch])
        out += mp.on_idle()
        return Counter(mp.speed_status), sorted(a.alert_id for a in alerts), len(out)

    assert run(37) == run(len(replay.published))
