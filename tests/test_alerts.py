"""consumers.alerts: regra de onibus parado (contrato 4.3), AlertsProcessor e SQL do sink."""

from __future__ import annotations

import math
import re
from datetime import timedelta

import pytest

from common.schemas import ALERT_TYPE_STOPPED, BusAlert
from common.tracker import SpeedTracker
from consumers.alerts.detector import StoppedVehicleDetector
from common.stream_loop import PostgresKafkaSink
from consumers.alerts.main import INSERT_SQL, AlertsProcessor, alert_params
from tests.conftest import BASE_TS, FREE_LAT, FREE_LON, LAPA_LAT, LAPA_LON, make_position

ONE_KM_LAT = 1000.0 / (math.pi / 180 * 6_371_008.8)
STEP = 30  # segundos entre leituras


class Harness:
    """SpeedTracker + StoppedVehicleDetector com os parametros padrao do contrato."""

    def __init__(self, **kw) -> None:
        self.tracker = SpeedTracker()
        self.detector = StoppedVehicleDetector(**kw)
        self.alerts: list[BusAlert] = []

    def feed(self, pos) -> BusAlert | None:
        alert = self.detector.process(self.tracker.update(pos))
        if alert is not None:
            self.alerts.append(alert)
        return alert

    def stopped(self, t_from: int, t_to: int, vehicle: str = "11433", lat: float = FREE_LAT, lon: float = FREE_LON):
        """Leituras paradas a cada STEP segundos em [t_from, t_to]."""
        for t in range(t_from, t_to + 1, STEP):
            self.feed(make_position(vehicle, t=t, lat=lat, lon=lon))

    def moving(self, t_from: int, t_to: int, vehicle: str = "11433"):
        """~30 km/h para o norte."""
        for t in range(t_from, t_to + 1, STEP):
            self.feed(make_position(vehicle, t=t, lat=FREE_LAT + ONE_KM_LAT * 0.25 * (t - t_from) / STEP))


def test_alert_fires_after_ten_minutes_stopped_outside_terminal():
    h = Harness()
    h.stopped(0, 570)
    assert h.alerts == []                        # 9,5 min parado
    alert = h.feed(make_position(t=600))         # 10 min desde stopped_since (= 1a leitura)
    assert alert is not None
    assert alert.alert_type == ALERT_TYPE_STOPPED and alert.vehicle_id == "11433"
    assert alert.stopped_since == BASE_TS        # inicio do intervalo lento (leitura anterior)
    assert alert.detected_at == BASE_TS + timedelta(minutes=10)
    assert alert.stopped_minutes == 10.0 and alert.last_speed_kmh == 0.0
    assert alert.alert_id == "11433-20261014T120000Z"
    assert (alert.lat, alert.lon, alert.line_code, alert.direction) == (FREE_LAT, FREE_LON, 1273, 1)


def test_slow_but_not_stopped_counts_as_slow():
    h = Harness()
    # ~1,2 km/h: 10 m a cada 30 s
    for i in range(0, 22):
        h.feed(make_position(t=i * STEP, lat=FREE_LAT + ONE_KM_LAT * 0.01 * i))
    assert len(h.alerts) == 1 and 1.0 < h.alerts[0].last_speed_kmh < 2.0


def test_only_one_alert_per_sequence():
    h = Harness()
    h.stopped(0, 1800)
    assert len(h.alerts) == 1


def test_never_alerts_inside_terminal():
    h = Harness()
    h.stopped(0, 1800, lat=LAPA_LAT, lon=LAPA_LON)
    assert h.alerts == [] and h.detector.stopped_vehicles == 0


def test_terminal_reading_resets_sequence():
    in_terminal = {"value": False}
    h = Harness(near_terminal=lambda lat, lon, radius: in_terminal["value"])
    h.stopped(0, 300)
    assert h.detector.stopped_vehicles == 1
    in_terminal["value"] = True
    h.stopped(330, 330)                     # uma leitura dentro de terminal zera a sequencia
    assert h.detector.stopped_vehicles == 0
    in_terminal["value"] = False
    h.stopped(360, 900)                     # nova sequencia comeca em 330 (leitura anterior)
    assert h.alerts == []
    alert = h.feed(make_position(t=930))
    assert alert is not None and alert.stopped_since == BASE_TS + timedelta(seconds=330)


def test_terminal_check_uses_configured_radius():
    seen = []
    h = Harness(terminal_radius_m=123.0, near_terminal=lambda lat, lon, radius: seen.append(radius) or False)
    h.stopped(0, 30)
    assert seen == [123.0]


def test_moving_resets_and_new_sequence_can_alert_again():
    h = Harness()
    h.stopped(0, 600)
    assert len(h.alerts) == 1
    h.moving(630, 900)
    assert h.detector.stopped_vehicles == 0
    last = h.tracker.last("11433")
    for t in range(930, 930 + 630, STEP):
        h.feed(make_position(t=t, lat=last.lat, lon=last.lon))
    assert len(h.alerts) == 2
    first, second = h.alerts
    assert first.alert_id != second.alert_id
    assert second.stopped_since == BASE_TS + timedelta(seconds=900)


def test_short_stop_does_not_alert():
    h = Harness()
    h.stopped(0, 540)
    h.moving(570, 900)
    h.stopped(930, 1400)
    assert h.alerts == []


def test_invalid_reading_does_not_reset_sequence():
    h = Harness()
    h.stopped(0, 300)
    h.feed(make_position(t=330, lat=0.0, lon=0.0))   # coordenada zerada
    assert h.detector.stopped_vehicles == 1
    h.stopped(360, 600)
    assert len(h.alerts) == 1 and h.alerts[0].stopped_since == BASE_TS


def test_discarded_speed_does_not_reset_sequence():
    h = Harness()
    h.stopped(0, 300)
    h.feed(make_position(t=300, lat=FREE_LAT + 0.01))     # intervalo zero: descartada, estado mantido
    h.feed(make_position(t=300.5))                        # intervalo < 1 s: descartada
    assert h.detector.stopped_vehicles == 1
    h.stopped(330, 600)
    assert len(h.alerts) == 1 and h.alerts[0].stopped_since == BASE_TS


def test_gps_jump_does_not_reset_sequence():
    h = Harness()
    h.stopped(0, 300)
    h.feed(make_position(t=330, lat=FREE_LAT + 0.05))     # salto de ~5,5 km em 30 s: SPEED_TOO_HIGH
    h.feed(make_position(t=360))                           # volta: outro salto
    assert h.detector.stopped_vehicles == 1
    h.stopped(390, 600)
    assert len(h.alerts) == 1 and h.alerts[0].stopped_since == BASE_TS


def test_first_reading_does_not_start_sequence():
    h = Harness()
    assert h.feed(make_position(t=0)) is None and h.detector.stopped_vehicles == 0


def test_vehicles_are_independent():
    h = Harness()
    for t in range(0, 601, STEP):
        h.feed(make_position("A", t=t))
        h.feed(make_position("B", t=t, lat=FREE_LAT + ONE_KM_LAT * 0.25 * t / STEP))
    assert [a.vehicle_id for a in h.alerts] == ["A"]


def test_custom_thresholds():
    h = Harness(alert_speed_kmh=40.0, alert_min_minutes=1.0)
    h.moving(0, 60)                                   # 30 km/h < 40: lento
    assert len(h.alerts) == 1 and h.alerts[0].stopped_minutes == 1.0


def test_alert_id_is_deterministic_on_reprocessing():
    run1, run2 = Harness(), Harness()
    run1.stopped(0, 700)
    run2.stopped(0, 700)
    assert [a.alert_id for a in run1.alerts] == [a.alert_id for a in run2.alerts]
    assert run1.alerts == run2.alerts


# --------------------------------------------------------------------------- processor e sink


def test_alerts_processor_sorts_batch_and_counts():
    proc = AlertsProcessor(SpeedTracker(), StoppedVehicleDetector())
    batch = [make_position(t=t) for t in range(0, 601, STEP)]
    alerts = proc.process(list(reversed(batch)))
    assert len(alerts) == 1 and alerts[0].stopped_since == BASE_TS
    assert proc.on_idle() == []
    stats = proc.stats()
    assert stats["alerts_emitted"] == 1 and stats["vehicles_tracked"] == 1 and stats["vehicles_stopped"] == 1
    assert stats["speed_status"] == {"first_reading": 1, "ok": 20}


def test_alerts_processor_from_settings(test_settings):
    proc = AlertsProcessor.from_settings(test_settings)
    assert proc.detector.alert_speed_kmh == 2.0
    assert proc.detector.min_stopped == timedelta(minutes=10)
    assert proc.detector.terminal_radius_m == 400.0


def _alert() -> BusAlert:
    h = Harness()
    h.stopped(0, 600)
    return h.alerts[0]


def test_alert_params_match_insert_placeholders():
    placeholders = set(re.findall(r"%\((\w+)\)s", INSERT_SQL))
    params = alert_params(_alert())
    assert placeholders == set(params)
    assert "ON CONFLICT (alert_id) DO NOTHING" in INSERT_SQL


class _Cursor:
    def __init__(self, log):
        self.log = log

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def executemany(self, sql, rows):
        self.log.append((sql, list(rows)))


class _Conn:
    def __init__(self, fail: bool = False):
        self.closed = False
        self.log: list = []
        self.commits = self.rollbacks = 0
        self.fail = fail

    def cursor(self):
        if self.fail:
            raise RuntimeError("conexao perdida")
        return _Cursor(self.log)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


class _Publisher:
    def __init__(self, events_log: list | None = None, fail: bool = False):
        self.published = []
        self.events_log = events_log if events_log is not None else []
        self.fail = fail

    def publish(self, events):
        self.events_log.append("kafka")
        if self.fail:
            raise RuntimeError("kafka fora")
        self.published.extend(events)


def _sink(test_settings, conn, pub):
    return PostgresKafkaSink(test_settings, pub, INSERT_SQL, alert_params, name="alerts",
                             connect_fn=lambda settings, shutdown=None: conn)


def test_sink_writes_and_commits_before_publishing(test_settings):
    order: list[str] = []
    conn = _Conn()
    original_commit = conn.commit

    def commit():
        order.append("db")
        original_commit()

    conn.commit = commit
    pub = _Publisher(order)
    alert = _alert()
    _sink(test_settings, conn, pub).deliver([alert])
    assert order == ["db", "kafka"]
    assert pub.published == [alert]
    assert conn.log[0][0] == INSERT_SQL
    assert conn.log[0][1][0]["alert_id"] == alert.alert_id and conn.commits == 1


def test_sink_db_failure_rolls_back_and_does_not_publish(test_settings):
    conn, pub = _Conn(fail=True), _Publisher()
    sink = _sink(test_settings, conn, pub)
    with pytest.raises(RuntimeError):
        sink.deliver([_alert()])
    assert conn.rollbacks == 1 and pub.published == []
    sink.reset()
    assert conn.closed


def test_sink_kafka_failure_after_db_commit_propagates(test_settings):
    conn, pub = _Conn(), _Publisher(fail=True)
    with pytest.raises(RuntimeError, match="kafka fora"):
        _sink(test_settings, conn, pub).deliver([_alert()])
    assert conn.commits == 1


def test_sink_rollback_failure_does_not_mask_original_error(test_settings):
    conn = _Conn(fail=True)

    def broken_rollback():
        raise OSError("conexao quebrada")

    conn.rollback = broken_rollback
    with pytest.raises(RuntimeError, match="conexao perdida"):
        _sink(test_settings, conn, _Publisher()).deliver([_alert()])


def test_sink_ignores_empty_outputs(test_settings):
    conn, pub = _Conn(), _Publisher()
    _sink(test_settings, conn, pub).deliver([])
    assert conn.log == [] and pub.events_log == []
