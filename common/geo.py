"""Calculos geograficos e regra de velocidade compartilhados pelos consumers."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

EARTH_RADIUS_M = 6_371_008.8

# Caixa aproximada da Regiao Metropolitana de Sao Paulo. Pontos fora dela sao
# tratados como leitura invalida de GPS.
SP_BBOX = {"lat_min": -24.10, "lat_max": -23.30, "lon_min": -47.10, "lon_max": -46.20}


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distancia em metros entre dois pontos (lat/lon em graus) pela formula de haversine."""
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlmb / 2) ** 2
    return 2 * EARTH_RADIUS_M * math.asin(min(1.0, math.sqrt(a)))


def is_valid_coordinate(lat: float | None, lon: float | None) -> bool:
    """Coordenada valida: numerica, nao zerada e dentro da Grande Sao Paulo."""
    if lat is None or lon is None:
        return False
    try:
        lat, lon = float(lat), float(lon)
    except (TypeError, ValueError):
        return False
    if math.isnan(lat) or math.isnan(lon):
        return False
    if lat == 0.0 or lon == 0.0:
        return False
    return SP_BBOX["lat_min"] <= lat <= SP_BBOX["lat_max"] and SP_BBOX["lon_min"] <= lon <= SP_BBOX["lon_max"]


class SpeedStatus(str, Enum):
    OK = "ok"
    INVALID_COORDINATE = "invalid_coordinate"
    INTERVAL_TOO_SHORT = "interval_too_short"  # inclui intervalo zero ou negativo
    INTERVAL_TOO_LONG = "interval_too_long"
    SPEED_TOO_HIGH = "speed_too_high"


@dataclass(frozen=True)
class SpeedResult:
    status: SpeedStatus
    speed_kmh: float | None
    distance_m: float | None
    interval_s: float | None

    @property
    def ok(self) -> bool:
        return self.status is SpeedStatus.OK


def compute_speed_kmh(
    lat1: float,
    lon1: float,
    ts1: datetime,
    lat2: float,
    lon2: float,
    ts2: datetime,
    *,
    max_speed_kmh: float = 100.0,
    min_interval_s: float = 1.0,
    max_interval_s: float = 600.0,
) -> SpeedResult:
    """Velocidade media entre duas leituras consecutivas do mesmo veiculo.

    Regras de descarte (contrato em docs/CONTRACTS.md):
    - qualquer coordenada invalida (zerada, fora da Grande SP)
    - intervalo < min_interval_s (inclui igual a zero e negativo: leitura fora de ordem)
    - intervalo > max_interval_s (veiculo sumiu por muito tempo; velocidade nao representativa)
    - velocidade > max_speed_kmh (salto de GPS)
    """
    if not (is_valid_coordinate(lat1, lon1) and is_valid_coordinate(lat2, lon2)):
        return SpeedResult(SpeedStatus.INVALID_COORDINATE, None, None, None)
    interval = (ts2 - ts1).total_seconds()
    if interval < min_interval_s:
        return SpeedResult(SpeedStatus.INTERVAL_TOO_SHORT, None, None, interval)
    if interval > max_interval_s:
        return SpeedResult(SpeedStatus.INTERVAL_TOO_LONG, None, None, interval)
    distance = haversine_m(lat1, lon1, lat2, lon2)
    speed = distance / interval * 3.6
    if speed > max_speed_kmh:
        return SpeedResult(SpeedStatus.SPEED_TOO_HIGH, speed, distance, interval)
    return SpeedResult(SpeedStatus.OK, speed, distance, interval)
