"""Consumer de metricas: bus-positions-raw -> bus-metrics + tabela line_metrics.

Execucao: ``python -m consumers.metrics.main``. O ciclo de lote, retry e commit de
offsets esta em ``common.stream_loop.StreamLoop``.
"""

from __future__ import annotations

from typing import Any

from common.config import load_settings
from common.kafka_utils import make_consumer, make_producer, wait_for_kafka
from common.log import get_logger, setup_logging
from common.runtime import ShutdownFlag, ShutdownRequested
from common.schemas import LineMetric
from common.stream_loop import KafkaPublisher, PostgresKafkaSink, StreamLoop
from consumers.metrics.processor import MetricsProcessor

GROUP_ID = "metrics-consumer"

log = get_logger(__name__)

UPSERT_SQL = """
INSERT INTO line_metrics (
  window_start, window_end, line_code, line_sign, direction, avg_speed_kmh, speed_samples,
  active_vehicles, accessible_vehicles, accessible_pct, computed_at
) VALUES (
  %(window_start)s, %(window_end)s, %(line_code)s, %(line_sign)s, %(direction)s, %(avg_speed_kmh)s,
  %(speed_samples)s, %(active_vehicles)s, %(accessible_vehicles)s, %(accessible_pct)s, %(computed_at)s
)
ON CONFLICT (line_code, direction, window_start) DO UPDATE SET
  window_end = EXCLUDED.window_end,
  line_sign = EXCLUDED.line_sign,
  avg_speed_kmh = EXCLUDED.avg_speed_kmh,
  speed_samples = EXCLUDED.speed_samples,
  active_vehicles = EXCLUDED.active_vehicles,
  accessible_vehicles = EXCLUDED.accessible_vehicles,
  accessible_pct = EXCLUDED.accessible_pct,
  computed_at = EXCLUDED.computed_at
"""


def metric_params(metric: LineMetric) -> dict[str, Any]:
    """Parametros SQL (datetimes como objetos, nao strings)."""
    return {
        "window_start": metric.window_start,
        "window_end": metric.window_end,
        "line_code": metric.line_code,
        "line_sign": metric.line_sign,
        "direction": metric.direction,
        "avg_speed_kmh": metric.avg_speed_kmh,
        "speed_samples": metric.speed_samples,
        "active_vehicles": metric.active_vehicles,
        "accessible_vehicles": metric.accessible_vehicles,
        "accessible_pct": metric.accessible_pct,
        "computed_at": metric.computed_at,
    }


def main() -> None:
    settings = load_settings()
    setup_logging("metrics", settings.log_level)
    log.info("iniciando consumer de metricas", topic_in=settings.topic_raw, topic_out=settings.topic_metrics,
             group=GROUP_ID, window_minutes=settings.window_minutes)
    shutdown = ShutdownFlag()
    sink = PostgresKafkaSink(
        settings,
        KafkaPublisher(make_producer(settings, "metrics-producer"), settings.topic_metrics),
        UPSERT_SQL,
        metric_params,
        name="metrics",
    )
    try:
        sink.connect(shutdown)
        wait_for_kafka(settings, shutdown=shutdown)
    except ShutdownRequested:
        log.info("encerrado durante a inicializacao")
        return
    loop = StreamLoop(
        consumer=make_consumer(settings, GROUP_ID, [settings.topic_raw]),
        processor=MetricsProcessor.from_settings(settings),
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
