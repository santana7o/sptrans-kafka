"""Regressao das correcoes feitas apos a revisao de codigo (DECISIONS D12)."""

from __future__ import annotations

import logging
from datetime import timedelta

import pytest

from common.runtime import ShutdownFlag, ShutdownRequested, sleep_or_shutdown
from common.tracker import SpeedTracker
from consumers.metrics.processor import MetricsProcessor
from consumers.metrics.windows import WindowAggregator
from producer.publisher import Publisher
from producer.replay import replay_anchor
from tests.conftest import BASE_TS, make_position


# watermark por particao -------------------------------------------------

def test_lagging_partition_holds_watermark_and_window_keeps_both_vehicles():
    """Particao A entrega 10 min antes da particao B (catch-up de backlog)."""
    proc = MetricsProcessor(SpeedTracker(),
                            WindowAggregator(window_minutes=5, lateness_seconds=60, expected_partitions=2))
    batch_a = [make_position("A1", t=t) for t in range(0, 601, 30)]
    batch_b = [make_position("B1", t=t) for t in range(0, 601, 30)]

    emitted = proc.process(batch_a, [0] * len(batch_a))
    assert emitted == [], "sem dados da particao B o watermark nao pode avancar so pela A"

    emitted += proc.process(batch_b, [1] * len(batch_b))
    first = [m for m in emitted if m.window_start == BASE_TS.replace(minute=0, second=0, microsecond=0)]
    assert first and first[0].active_vehicles == 2
    assert proc.aggregator.counters["late"] == 0


def test_watermark_is_minimum_over_partitions():
    agg = WindowAggregator(window_minutes=5, lateness_seconds=60, expected_partitions=2)
    tracker = SpeedTracker()
    agg.add(tracker.update(make_position("A1", t=0)), partition=0)
    agg.add(tracker.update(make_position("B1", t=600)), partition=1)
    assert agg.watermark == BASE_TS - timedelta(seconds=60)


# ancora do replay ---------------------------------------------------------

def test_replay_anchor_without_published_data_is_now():
    assert replay_anchor(BASE_TS, None, timedelta(seconds=30)) == BASE_TS


def test_replay_anchor_never_goes_back_in_event_time():
    ahead = BASE_TS + timedelta(hours=4)
    anchor = replay_anchor(BASE_TS, ahead, timedelta(seconds=30), lead=timedelta(seconds=30))
    assert anchor == ahead + timedelta(seconds=60)


def test_replay_anchor_uses_now_when_published_data_is_old():
    old = BASE_TS - timedelta(hours=1)
    assert replay_anchor(BASE_TS, old, timedelta(seconds=30)) == BASE_TS


# segredos fora do repr ---------------------------------------------------

def test_settings_repr_hides_token_and_password(clean_env):
    clean_env.setenv("SPTRANS_TOKEN", "segredo-token-123")
    clean_env.setenv("POSTGRES_PASSWORD", "senha-forte-456")
    from common.config import load_settings

    text = repr(load_settings())
    assert "segredo-token-123" not in text and "senha-forte-456" not in text


# esperas de inicializacao respeitam SIGTERM --------------------------------

def test_sleep_or_shutdown_raises_when_flag_is_set():
    flag = ShutdownFlag(install_handlers=False)
    flag.set()
    with pytest.raises(ShutdownRequested):
        sleep_or_shutdown(5, flag)


def test_sleep_or_shutdown_returns_after_delay_without_signal():
    sleep_or_shutdown(0.01, ShutdownFlag(install_handlers=False))


def test_wait_for_kafka_stops_on_shutdown(monkeypatch, test_settings):
    import confluent_kafka.admin as admin_mod

    class _Admin:
        def __init__(self, conf):
            pass

        def list_topics(self, timeout):
            raise RuntimeError("broker fora")

    monkeypatch.setattr(admin_mod, "AdminClient", _Admin)
    from common.kafka_utils import wait_for_kafka

    flag = ShutdownFlag(install_handlers=False)
    flag.set()
    with pytest.raises(ShutdownRequested):
        wait_for_kafka(test_settings, timeout_s=60, shutdown=flag)


# resumo de falhas de entrega -----------------------------------------------

class _FailingProducer:
    def __init__(self):
        self.callbacks = []

    def produce(self, topic, key, value, on_delivery):
        self.callbacks.append(on_delivery)

    def poll(self, timeout):
        return 0

    def flush(self, timeout=None):
        for cb in self.callbacks:
            cb(RuntimeError("broker fora"), None)
        self.callbacks.clear()
        return 0


def test_publisher_logs_single_summary_per_collection(caplog):
    pub = Publisher(_FailingProducer(), "bus-positions-raw")
    events = [make_position(str(i), t=0) for i in range(50)]
    with caplog.at_level(logging.ERROR):
        pub.publish(events)
    summaries = [r for r in caplog.records if r.getMessage() == "falhas na entrega ao kafka"]
    assert len(summaries) == 1
    assert summaries[0].fields["failed"] == 50
    assert pub.failed == 50


def test_partition_without_data_stops_holding_after_stall_limit():
    agg = WindowAggregator(window_minutes=5, lateness_seconds=60, expected_partitions=3)
    tracker = SpeedTracker()
    agg.add(tracker.update(make_position("A1", t=0)), partition=0)
    agg.add(tracker.update(make_position("A1", t=600)), partition=0)
    assert agg.watermark is None  # 10 min < 3 janelas: particoes 1 e 2 ainda seguram
    agg.add(tracker.update(make_position("A1", t=960)), partition=0)
    assert agg.watermark == BASE_TS + timedelta(seconds=900)


def test_stalled_partition_stops_holding_others():
    agg = WindowAggregator(window_minutes=5, lateness_seconds=60, expected_partitions=2)
    tracker = SpeedTracker()
    agg.add(tracker.update(make_position("B1", t=0)), partition=1)
    agg.add(tracker.update(make_position("A1", t=0)), partition=0)
    agg.add(tracker.update(make_position("A1", t=1200)), partition=0)
    assert agg.watermark == BASE_TS + timedelta(seconds=1140)
