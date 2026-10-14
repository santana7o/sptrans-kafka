"""consumers.metrics: janelas tumbling, watermark e MetricsProcessor (contrato 4.2)."""

from __future__ import annotations

import math
from datetime import timedelta

import pytest

from common.timeutils import parse_ts
from common.tracker import SpeedTracker, TrackedReading
from consumers.metrics.processor import MetricsProcessor
from consumers.metrics.windows import MAX_FUTURE_SKEW, WindowAggregator
from tests.conftest import BASE_TS, FREE_LAT, make_position

ONE_KM_LAT = 1000.0 / (math.pi / 180 * 6_371_008.8)
NOW = parse_ts("2030-01-01T00:00:00Z")


def _agg(**kw) -> WindowAggregator:
    return WindowAggregator(window_minutes=kw.pop("window_minutes", 5),
                            lateness_seconds=kw.pop("lateness_seconds", 60), clock=lambda: NOW)


def _reading(pos, previous=None) -> TrackedReading:
    """Leitura sem velocidade (primeira do veiculo)."""
    return TrackedReading(pos, previous, None)


def _feed(agg: WindowAggregator, tracker: SpeedTracker, *positions) -> list[bool]:
    return [agg.add(tracker.update(p)) for p in positions]


def test_window_is_aligned_to_utc_clock():
    agg = _agg()
    agg.add(_reading(make_position(t=3 * 60 + 10)))              # 12:03:10
    [metric] = agg.flush_all()
    assert metric.window_start == parse_ts("2026-10-14T12:00:00Z")
    assert metric.window_end == parse_ts("2026-10-14T12:05:00Z")
    assert metric.computed_at == NOW


def test_aggregation_average_distinct_vehicles_and_accessible_pct():
    agg, tracker = _agg(), SpeedTracker()
    _feed(agg, tracker,
          make_position("A", t=0, accessible=True),
          make_position("B", t=0, accessible=False),
          make_position("C", t=10, accessible=True),
          make_position("A", t=60, lat=FREE_LAT + ONE_KM_LAT, accessible=True),       # 60 km/h
          make_position("B", t=60, lat=FREE_LAT + ONE_KM_LAT / 2, accessible=False))  # 30 km/h
    [m] = agg.flush_all()
    assert m.active_vehicles == 3 and m.accessible_vehicles == 2
    assert m.accessible_pct == pytest.approx(66.67)
    assert m.speed_samples == 2 and m.avg_speed_kmh == pytest.approx(45.0, abs=0.01)
    assert (m.line_code, m.direction, m.line_sign) == (1273, 1, "8000-10")


def test_line_and_direction_are_separate_windows():
    agg = _agg()
    agg.add(_reading(make_position("A", t=0, direction=1)))
    agg.add(_reading(make_position("B", t=0, direction=2)))
    agg.add(_reading(make_position("C", t=0, line_code=999)))
    metrics = agg.flush_all()
    assert sorted((m.line_code, m.direction) for m in metrics) == [(999, 1), (1273, 1), (1273, 2)]
    assert all(m.active_vehicles == 1 for m in metrics)


def test_speed_goes_to_window_of_current_reading_and_none_without_samples():
    agg, tracker = _agg(), SpeedTracker()
    _feed(agg, tracker, make_position("A", t=4 * 60 + 30), make_position("A", t=5 * 60 + 30, lat=FREE_LAT + ONE_KM_LAT))
    first, second = agg.flush_all()
    assert first.window_start == BASE_TS and first.avg_speed_kmh is None and first.speed_samples == 0
    assert first.active_vehicles == 1
    assert second.window_start == BASE_TS + timedelta(minutes=5) and second.speed_samples == 1
    assert second.avg_speed_kmh == pytest.approx(60.0, abs=0.01)


def test_window_without_vehicles_never_exists_and_zero_pct_guard():
    agg = _agg()
    assert agg.flush_all() == [] and agg.close_ready() == [] and agg.watermark is None


def test_watermark_closes_window_only_after_lateness():
    agg = _agg()
    agg.add(_reading(make_position("A", t=60)))
    agg.add(_reading(make_position("B", t=5 * 60 + 59)))          # 12:05:59 -> wm 12:04:59
    assert agg.watermark == parse_ts("2026-10-14T12:04:59Z")
    assert agg.close_ready() == []
    agg.add(_reading(make_position("C", t=6 * 60)))               # 12:06:00 -> wm 12:05:00
    [closed] = agg.close_ready()
    assert closed.window_start == BASE_TS and closed.active_vehicles == 1
    assert agg.close_ready() == []                                 # emitida uma unica vez
    assert agg.open_windows == 1 and agg.counters["emitted"] == 1


def test_late_reading_for_closed_window_is_dropped():
    agg = _agg()
    agg.add(_reading(make_position("A", t=60)))
    agg.add(_reading(make_position("B", t=7 * 60)))
    assert len(agg.close_ready()) == 1
    # outra linha, janela 12:00 ja fechada: descartada mesmo nunca tendo sido aberta
    assert agg.add(_reading(make_position("Z", t=4 * 60, line_code=42))) is False
    assert agg.counters["late"] == 1
    # janela ainda aberta continua aceitando leituras atrasadas dentro da tolerancia
    assert agg.add(_reading(make_position("Y", t=5 * 60 + 1, line_code=42))) is True


def test_invalid_coordinate_is_counted_and_not_active():
    agg = _agg()
    assert agg.add(_reading(make_position("A", t=0, lat=0.0, lon=0.0))) is False
    agg.add(_reading(make_position("B", t=0)))
    [m] = agg.flush_all()
    assert m.active_vehicles == 1 and agg.counters["invalid"] == 1


def test_future_reading_is_ignored_and_does_not_move_watermark():
    agg = _agg()
    agg.add(_reading(make_position("A", t=0)))
    wm = agg.watermark
    future = make_position("B", t=3600, collected_at=BASE_TS + timedelta(seconds=30))
    assert agg.add(_reading(future)) is False
    assert agg.counters["future"] == 1 and agg.watermark == wm
    # exatamente no limite de 5 min ainda entra
    edge = make_position("C", t=30 + MAX_FUTURE_SKEW.total_seconds(), collected_at=BASE_TS + timedelta(seconds=30))
    assert agg.add(_reading(edge)) is True


def test_flush_all_emits_everything_and_closes_them():
    agg = _agg()
    for i in range(3):
        agg.add(_reading(make_position(str(i), t=i * 5 * 60)))
    metrics = agg.flush_all()
    assert [m.window_start for m in metrics] == [BASE_TS + timedelta(minutes=5 * i) for i in range(3)]
    assert agg.open_windows == 0 and agg.flush_all() == []
    assert agg.add(_reading(make_position("X", t=14 * 60))) is False   # janela 12:10 ja emitida
    assert agg.add(_reading(make_position("X", t=15 * 60))) is True


def test_other_window_sizes():
    agg = _agg(window_minutes=15, lateness_seconds=0)
    agg.add(_reading(make_position(t=14 * 60)))
    agg.add(_reading(make_position("B", t=15 * 60)))
    [m] = agg.close_ready()
    assert m.window_end - m.window_start == timedelta(minutes=15)


# --------------------------------------------------------------------------- processor


def test_processor_sorts_batch_by_event_time_and_closes_windows():
    proc = MetricsProcessor(SpeedTracker(), _agg())
    batch = [
        make_position("A", t=60, lat=FREE_LAT + ONE_KM_LAT),
        make_position("B", t=7 * 60),
        make_position("A", t=0),
    ]
    metrics = proc.process(batch)
    assert len(metrics) == 1
    assert metrics[0].speed_samples == 1 and metrics[0].avg_speed_kmh == pytest.approx(60.0, abs=0.01)
    assert proc.speed_status == {"first_reading": 2, "ok": 1}
    rest = proc.on_idle()
    assert [m.window_start for m in rest] == [BASE_TS + timedelta(minutes=5)]
    stats = proc.stats()
    assert stats["vehicles_tracked"] == 2 and stats["open_windows"] == 0
    assert stats["windows"]["emitted"] == 2


def test_processor_from_settings_uses_configuration(clean_env):
    clean_env.setenv("WINDOW_MINUTES", "10")
    clean_env.setenv("WINDOW_LATENESS_SECONDS", "30")
    clean_env.setenv("MAX_SPEED_KMH", "80")
    from common.config import load_settings

    proc = MetricsProcessor.from_settings(load_settings())
    assert proc.aggregator.window == timedelta(minutes=10)
    assert proc.aggregator.lateness == timedelta(seconds=30)
    assert proc.tracker.max_speed_kmh == 80
