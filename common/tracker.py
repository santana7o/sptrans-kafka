"""Estado do ultimo ponto por veiculo e calculo de velocidade (contrato 4.1).

Usado pelos consumers de metricas e de alertas, para que ambos apliquem exatamente
as mesmas regras de descarte.
"""

from __future__ import annotations

from dataclasses import dataclass

from common.geo import SpeedResult, SpeedStatus, compute_speed_kmh, is_valid_coordinate
from common.schemas import BusPosition


@dataclass(frozen=True)
class TrackedReading:
    """Resultado de alimentar uma leitura no tracker."""

    position: BusPosition
    previous: BusPosition | None      # leitura valida anterior do mesmo veiculo (None = primeira)
    speed: SpeedResult | None          # None quando nao ha leitura anterior

    @property
    def valid_coordinate(self) -> bool:
        return is_valid_coordinate(self.position.lat, self.position.lon)

    @property
    def speed_kmh(self) -> float | None:
        """Velocidade valida em km/h, ou None se descartada/indisponivel."""
        return self.speed.speed_kmh if self.speed is not None and self.speed.ok else None


class SpeedTracker:
    """Guarda a ultima leitura valida de cada veiculo e calcula velocidades."""

    def __init__(
        self,
        max_speed_kmh: float = 100.0,
        min_interval_s: float = 1.0,
        max_interval_s: float = 600.0,
    ) -> None:
        self.max_speed_kmh = max_speed_kmh
        self.min_interval_s = min_interval_s
        self.max_interval_s = max_interval_s
        self._last: dict[str, BusPosition] = {}

    def __len__(self) -> int:
        return len(self._last)

    def last(self, vehicle_id: str) -> BusPosition | None:
        return self._last.get(vehicle_id)

    def update(self, pos: BusPosition) -> TrackedReading:
        """Processa uma leitura e devolve a velocidade (se houver) e o status.

        Regras:
        - coordenada invalida: status INVALID_COORDINATE, estado NAO e atualizado;
        - sem leitura anterior: speed=None, estado passa a ser esta leitura;
        - intervalo <= 0 (repetida ou fora de ordem): descartada, estado NAO e atualizado;
        - demais descartes (intervalo curto/longo, velocidade alta): velocidade descartada,
          estado passa a ser esta leitura.
        """
        if not is_valid_coordinate(pos.lat, pos.lon):
            return TrackedReading(pos, self._last.get(pos.vehicle_id),
                                  SpeedResult(SpeedStatus.INVALID_COORDINATE, None, None, None))
        prev = self._last.get(pos.vehicle_id)
        if prev is None:
            self._last[pos.vehicle_id] = pos
            return TrackedReading(pos, None, None)
        result = compute_speed_kmh(
            prev.lat, prev.lon, prev.event_ts,
            pos.lat, pos.lon, pos.event_ts,
            max_speed_kmh=self.max_speed_kmh,
            min_interval_s=self.min_interval_s,
            max_interval_s=self.max_interval_s,
        )
        interval = (pos.event_ts - prev.event_ts).total_seconds()
        if interval > 0:
            self._last[pos.vehicle_id] = pos
        return TrackedReading(pos, prev, result)
