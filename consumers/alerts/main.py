"""Consumer de alertas: bus-positions-raw -> bus-alerts + tabela bus_alerts.

Execucao: ``python -m consumers.alerts.main``. O ciclo de lote, retry e commit de
offsets esta em ``common.stream_loop.StreamLoop``.
"""

from __future__ import annotations

from collections import Counter
from typing import Any, Sequence

from common.config import Settings, load_settings
from common.db import connect
from common.kafka_utils import make_consumer, make_producer, wait_for_kafka
from common.log import get_logger, setup_logging
from common.runtime import ShutdownFlag
from common.schemas import BusAlert, BusPosition
from common.stream_loop import KafkaPublisher, StreamLoop
from common.tracker import SpeedTracker
from consumers.alerts.detector import StoppedVehicleDetector

GROUP_ID = "alerts-consumer"

log = get_logger(__name__)

INSERT_SQL = """
INSERT INTO bus_alerts (
  alert_id, alert_type, vehicle_id, line_code, line_sign, direction, lat, lon,
  stopped_since, detected_at, stopped_minutes, last_speed_kmh
) VALUES (
  %(alert_id)s, %(alert_type)s, %(vehicle_id)s, %(line_code)s, %(line_sign)s, %(direction)s, %(lat)s, %(lon)s,
  %(stopped_since)s, %(detected_at)s, %(stopped_minutes)s, %(last_speed_kmh)s
)
ON CONFLICT (alert_id) DO NOTHING
"""


class AlertsProcessor:
    def __init__(self, tracker: SpeedTracker, detector: StoppedVehicleDetector) -> None:
        self.tracker = tracker
        self.detector = detector
        self.speed_status: Counter[str] = Counter()
        self.alerts_emitted = 0

    @classmethod
    def from_settings(cls, settings: Settings) -> "AlertsProcessor":
        return cls(
            SpeedTracker(settings.max_speed_kmh, settings.min_interval_seconds, settings.max_interval_seconds),
            StoppedVehicleDetector(settings.alert_speed_kmh, settings.alert_min_minutes, settings.terminal_radius_m),
        )

    def process(self, positions: Sequence[BusPosition]) -> list[BusAlert]:
        alerts: list[BusAlert] = []
        for pos in sorted(positions, key=lambda p: p.event_ts):
            reading = self.tracker.update(pos)
            self.speed_status[reading.speed.status.value if reading.speed else "first_reading"] += 1
            alert = self.detector.process(reading)
            if alert is not None:
                alerts.append(alert)
        self.alerts_emitted += len(alerts)
        return alerts

    def on_idle(self) -> list[BusAlert]:
        return []

    def stats(self) -> dict[str, Any]:
        return {
            "vehicles_tracked": len(self.tracker),
            "vehicles_stopped": self.detector.stopped_vehicles,
            "alerts_emitted": self.alerts_emitted,
            "speed_status": dict(self.speed_status),
        }


def alert_params(alert: BusAlert) -> dict[str, Any]:
    return {
        "alert_id": alert.alert_id,
        "alert_type": alert.alert_type,
        "vehicle_id": alert.vehicle_id,
        "line_code": alert.line_code,
        "line_sign": alert.line_sign,
        "direction": alert.direction,
        "lat": alert.lat,
        "lon": alert.lon,
        "stopped_since": alert.stopped_since,
        "detected_at": alert.detected_at,
        "stopped_minutes": alert.stopped_minutes,
        "last_speed_kmh": alert.last_speed_kmh,
    }


class AlertsSink:
    """Publica em bus-alerts e grava em bus_alerts (idempotente por alert_id)."""

    def __init__(self, settings: Settings, publisher: KafkaPublisher) -> None:
        self._settings = settings
        self._publisher = publisher
        self._conn = None

    def _connection(self):
        if self._conn is None or self._conn.closed:
            self._conn = connect(self._settings)
        return self._conn

    def deliver(self, outputs: Sequence[BusAlert]) -> None:
        self._publisher.publish(outputs)
        conn = self._connection()
        try:
            with conn.cursor() as cur:
                cur.executemany(INSERT_SQL, [alert_params(a) for a in outputs])
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        for alert in outputs:
            log.info("alerta emitido", alert_id=alert.alert_id, vehicle_id=alert.vehicle_id,
                     line_sign=alert.line_sign, stopped_minutes=alert.stopped_minutes)

    def reset(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            finally:
                self._conn = None

    def close(self) -> None:
        self.reset()


def main() -> None:
    settings = load_settings()
    setup_logging("alerts", settings.log_level)
    log.info("iniciando consumer de alertas", topic_in=settings.topic_raw, topic_out=settings.topic_alerts,
             group=GROUP_ID, alert_speed_kmh=settings.alert_speed_kmh, alert_min_minutes=settings.alert_min_minutes)
    shutdown = ShutdownFlag()
    wait_for_kafka(settings)
    sink = AlertsSink(settings, KafkaPublisher(make_producer(settings, "alerts-producer"), settings.topic_alerts))
    sink._connection()
    loop = StreamLoop(
        consumer=make_consumer(settings, GROUP_ID, [settings.topic_raw]),
        processor=AlertsProcessor.from_settings(settings),
        sink=sink,
        shutdown=shutdown,
        idle_flush_s=settings.idle_flush_seconds,
    )
    try:
        loop.run()
    finally:
        sink.close()


if __name__ == "__main__":
    main()
