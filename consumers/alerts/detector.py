"""Regra de alerta de onibus parado fora de terminal (contrato 4.3). Logica pura."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable

from common.schemas import ALERT_TYPE_STOPPED, BusAlert
from common.terminals import is_near_terminal
from common.tracker import TrackedReading


@dataclass
class _StopState:
    stopped_since: datetime
    alerted: bool = False


class StoppedVehicleDetector:
    """Mantem, por veiculo, a sequencia atual de leituras lentas fora de terminal."""

    def __init__(
        self,
        alert_speed_kmh: float = 2.0,
        alert_min_minutes: float = 10.0,
        terminal_radius_m: float = 400.0,
        near_terminal: Callable[[float, float, float], bool] = is_near_terminal,
    ) -> None:
        self.alert_speed_kmh = alert_speed_kmh
        self.min_stopped = timedelta(minutes=alert_min_minutes)
        self.terminal_radius_m = terminal_radius_m
        self._near_terminal = near_terminal
        self._stops: dict[str, _StopState] = {}

    @property
    def stopped_vehicles(self) -> int:
        return len(self._stops)

    def process(self, reading: TrackedReading) -> BusAlert | None:
        """Atualiza o estado do veiculo e devolve um BusAlert quando a regra dispara."""
        speed = reading.speed_kmh
        if speed is None or reading.previous is None:
            # Primeira leitura ou velocidade descartada: nao altera a sequencia.
            return None
        pos = reading.position
        vehicle = pos.vehicle_id
        if speed >= self.alert_speed_kmh or self._near_terminal(pos.lat, pos.lon, self.terminal_radius_m):
            self._stops.pop(vehicle, None)
            return None
        state = self._stops.get(vehicle)
        if state is None:
            # A parada comeca no inicio do intervalo lento (leitura anterior).
            state = self._stops[vehicle] = _StopState(stopped_since=reading.previous.event_ts)
        stopped_for = pos.event_ts - state.stopped_since
        if state.alerted or stopped_for < self.min_stopped:
            return None
        state.alerted = True
        return BusAlert(
            alert_id=BusAlert.make_id(vehicle, state.stopped_since),
            alert_type=ALERT_TYPE_STOPPED,
            vehicle_id=vehicle,
            line_code=pos.line_code,
            line_sign=pos.line_sign,
            direction=pos.direction,
            lat=pos.lat,
            lon=pos.lon,
            stopped_since=state.stopped_since,
            detected_at=pos.event_ts,
            stopped_minutes=round(stopped_for.total_seconds() / 60.0, 2),
            last_speed_kmh=round(speed, 2),
        )
