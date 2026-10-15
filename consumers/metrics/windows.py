"""Janelas fixas de metricas por linha e sentido (contrato 4.2). Logica pura, sem Kafka."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Callable

from common.schemas import LineMetric
from common.timeutils import floor_to_window, utcnow
from common.tracker import TrackedReading

WindowKey = tuple[int, int, datetime]  # (line_code, direction, window_start)

# Leitura com event_ts muito a frente da coleta indica relogio de GPS errado; ela nao
# entra nas janelas para nao empurrar o watermark e fechar janelas cedo demais.
MAX_FUTURE_SKEW = timedelta(minutes=5)


@dataclass
class _WindowState:
    line_sign: str
    vehicles: set[str] = field(default_factory=set)
    accessible: set[str] = field(default_factory=set)
    speed_sum: float = 0.0
    speed_count: int = 0


class WindowAggregator:
    """Agrega leituras em janelas tumbling e emite LineMetric quando o watermark passa."""

    def __init__(
        self,
        window_minutes: int = 5,
        lateness_seconds: int = 60,
        clock: Callable[[], datetime] = utcnow,
        expected_partitions: int = 1,
        stall_windows: int = 3,
    ) -> None:
        self.window = timedelta(minutes=window_minutes)
        self.window_minutes = window_minutes
        self.lateness = timedelta(seconds=lateness_seconds)
        self._clock = clock
        self._open: dict[WindowKey, _WindowState] = {}
        # Maior event_ts visto em cada particao Kafka. O watermark usa a particao mais
        # atrasada: em catch-up de backlog o librdkafka entrega blocos de uma particao
        # antes da outra, e um watermark global descartaria as leituras da atrasada.
        # Particao parada segura o watermark; o flush por ociosidade cobre esse caso.
        # Particao que ainda nao entregou nada tambem segura o watermark, ate o tempo de
        # evento avancar ``stall_windows`` janelas; o mesmo limite faz uma particao parada
        # (sem dados novos) deixar de segurar as demais.
        self._max_by_partition: dict[int, datetime] = {}
        self.expected_partitions = max(1, expected_partitions)
        self.stall = self.window * max(1, stall_windows)
        self._first_event_ts: datetime | None = None
        self._closed_until: datetime | None = None  # janelas com fim <= isto ja foram emitidas
        self.counters = {"added": 0, "late": 0, "future": 0, "invalid": 0, "emitted": 0}

    @property
    def watermark(self) -> datetime | None:
        if not self._max_by_partition or self._first_event_ts is None:
            return None
        global_max = max(self._max_by_partition.values())
        progressed = global_max - self._first_event_ts
        if len(self._max_by_partition) < self.expected_partitions and progressed < self.stall:
            return None
        active = [ts for ts in self._max_by_partition.values() if ts >= global_max - self.stall]
        return min(active) - self.lateness

    @property
    def open_windows(self) -> int:
        return len(self._open)

    def add(self, reading: TrackedReading, partition: int = 0) -> bool:
        """Inclui a leitura na janela do seu event_ts. Retorna False se descartada."""
        pos = reading.position
        if not reading.valid_coordinate:
            self.counters["invalid"] += 1
            return False
        if pos.event_ts > pos.collected_at + MAX_FUTURE_SKEW:
            self.counters["future"] += 1
            return False
        start = floor_to_window(pos.event_ts, self.window_minutes)
        end = start + self.window
        if self._closed_until is not None and end <= self._closed_until:
            self.counters["late"] += 1
            return False
        key = (pos.line_code, pos.direction, start)
        state = self._open.get(key)
        if state is None:
            state = self._open[key] = _WindowState(line_sign=pos.line_sign)
        state.vehicles.add(pos.vehicle_id)
        if pos.accessible:
            state.accessible.add(pos.vehicle_id)
        speed = reading.speed_kmh
        if speed is not None:
            state.speed_sum += speed
            state.speed_count += 1
        if self._first_event_ts is None or pos.event_ts < self._first_event_ts:
            self._first_event_ts = pos.event_ts
        current = self._max_by_partition.get(partition)
        if current is None or pos.event_ts > current:
            self._max_by_partition[partition] = pos.event_ts
        self.counters["added"] += 1
        return True

    def close_ready(self) -> list[LineMetric]:
        """Emite e remove as janelas com window_end <= watermark."""
        wm = self.watermark
        if wm is None:
            return []
        return self._emit(lambda key: key[2] + self.window <= wm, closed_until=floor_to_window(wm, self.window_minutes))

    def flush_all(self) -> list[LineMetric]:
        """Emite todas as janelas abertas (ociosidade ou fim do replay)."""
        if not self._open:
            return []
        last_end = max(key[2] for key in self._open) + self.window
        return self._emit(lambda key: True, closed_until=last_end)

    def _emit(self, predicate: Callable[[WindowKey], bool], closed_until: datetime) -> list[LineMetric]:
        ready = sorted(key for key in self._open if predicate(key))
        now = self._clock()
        metrics = [self._to_metric(key, self._open.pop(key), now) for key in ready]
        if self._closed_until is None or closed_until > self._closed_until:
            self._closed_until = closed_until
        self.counters["emitted"] += len(metrics)
        return metrics

    def _to_metric(self, key: WindowKey, state: _WindowState, now: datetime) -> LineMetric:
        line_code, direction, start = key
        active = len(state.vehicles)
        accessible = len(state.accessible)
        avg = round(state.speed_sum / state.speed_count, 2) if state.speed_count else None
        return LineMetric(
            window_start=start,
            window_end=start + self.window,
            line_code=line_code,
            line_sign=state.line_sign,
            direction=direction,
            avg_speed_kmh=avg,
            speed_samples=state.speed_count,
            active_vehicles=active,
            accessible_vehicles=accessible,
            accessible_pct=round(100.0 * accessible / active, 2) if active else 0.0,
            computed_at=now,
        )
