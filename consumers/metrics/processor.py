"""Processador de metricas: SpeedTracker + WindowAggregator. Logica pura, testavel sem Kafka."""

from __future__ import annotations

from collections import Counter
from typing import Any, Sequence

from common.config import Settings
from common.schemas import BusPosition, LineMetric
from common.tracker import SpeedTracker
from consumers.metrics.windows import WindowAggregator


class MetricsProcessor:
    def __init__(self, tracker: SpeedTracker, aggregator: WindowAggregator) -> None:
        self.tracker = tracker
        self.aggregator = aggregator
        self.speed_status: Counter[str] = Counter()

    @classmethod
    def from_settings(cls, settings: Settings) -> "MetricsProcessor":
        return cls(
            SpeedTracker(settings.max_speed_kmh, settings.min_interval_seconds, settings.max_interval_seconds),
            WindowAggregator(settings.window_minutes, settings.window_lateness_seconds),
        )

    def process(self, positions: Sequence[BusPosition], partitions: Sequence[int] | None = None) -> list[LineMetric]:
        if partitions is None:
            partitions = [0] * len(positions)
        # Ordena por tempo de evento: dentro de um lote, mensagens de particoes
        # diferentes chegam intercaladas.
        for pos, partition in sorted(zip(positions, partitions), key=lambda item: item[0].event_ts):
            reading = self.tracker.update(pos)
            self.speed_status[reading.speed.status.value if reading.speed else "first_reading"] += 1
            self.aggregator.add(reading, partition)
        return self.aggregator.close_ready()

    def on_idle(self) -> list[LineMetric]:
        return self.aggregator.flush_all()

    def stats(self) -> dict[str, Any]:
        return {
            "vehicles_tracked": len(self.tracker),
            "open_windows": self.aggregator.open_windows,
            "windows": dict(self.aggregator.counters),
            "speed_status": dict(self.speed_status),
        }
