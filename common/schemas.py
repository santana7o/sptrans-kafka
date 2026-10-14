"""Schemas dos eventos Kafka. Fonte unica da verdade (ver docs/CONTRACTS.md).

Todos os eventos sao JSON UTF-8. Datas sao strings ISO 8601 em UTC com sufixo 'Z'.
Cada evento carrega ``schema_version`` para permitir evolucao compativel.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, fields
from datetime import datetime
from typing import Any, Iterable

from common.timeutils import parse_ts, to_iso

SCHEMA_VERSION = 1

TOPIC_RAW = "bus-positions-raw"
TOPIC_METRICS = "bus-metrics"
TOPIC_ALERTS = "bus-alerts"

SOURCE_LIVE = "live"
SOURCE_REPLAY = "replay"

ALERT_TYPE_STOPPED = "STOPPED_VEHICLE"


class SchemaError(ValueError):
    """Evento malformado (campo ausente, tipo errado, JSON invalido)."""


def _serialize(obj: Any) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in asdict(obj).items():
        out[key] = to_iso(value) if isinstance(value, datetime) else value
    return out


def _require(data: dict[str, Any], name: str) -> Any:
    if name not in data or data[name] is None:
        raise SchemaError(f"campo obrigatorio ausente: {name}")
    return data[name]


def _loads(raw: bytes | str) -> dict[str, Any]:
    try:
        data = json.loads(raw)
    except (TypeError, ValueError, AttributeError) as exc:
        raise SchemaError(f"JSON invalido: {exc}") from exc
    if not isinstance(data, dict):
        raise SchemaError("evento deve ser um objeto JSON")
    return data


class _Event:
    """Metodos comuns de serializacao."""

    def to_dict(self) -> dict[str, Any]:
        return _serialize(self)

    def to_json_bytes(self) -> bytes:
        return json.dumps(self.to_dict(), ensure_ascii=False, separators=(",", ":")).encode("utf-8")

    @classmethod
    def from_json_bytes(cls, raw: bytes | str):
        return cls.from_dict(_loads(raw))  # type: ignore[attr-defined]


@dataclass(frozen=True)
class BusPosition(_Event):
    """Uma leitura de GPS de um veiculo. Topico: bus-positions-raw. Chave: vehicle_id."""

    vehicle_id: str          # vs[].p (prefixo do veiculo)
    line_code: int           # l[].cl (codigo identificador da linha)
    line_sign: str           # l[].c  (letreiro completo, ex. "8000-10")
    direction: int           # l[].sl (1 = principal->secundario, 2 = secundario->principal)
    destination_sign: str    # l[].lt0 (letreiro de destino)
    origin_sign: str         # l[].lt1 (letreiro de origem)
    accessible: bool         # vs[].a
    lat: float               # vs[].py
    lon: float               # vs[].px
    event_ts: datetime       # vs[].ta (UTC, momento da captura do GPS)
    collected_at: datetime   # momento em que o producer obteve a resposta da API
    reference_time: str      # hr (horario de referencia da API, "HH:MM", hora local)
    source: str              # "live" ou "replay"
    schema_version: int = SCHEMA_VERSION

    def key(self) -> bytes:
        return self.vehicle_id.encode("utf-8")

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "BusPosition":
        try:
            return cls(
                vehicle_id=str(_require(data, "vehicle_id")),
                line_code=int(_require(data, "line_code")),
                line_sign=str(data.get("line_sign") or ""),
                direction=int(_require(data, "direction")),
                destination_sign=str(data.get("destination_sign") or ""),
                origin_sign=str(data.get("origin_sign") or ""),
                accessible=bool(data.get("accessible", False)),
                lat=float(_require(data, "lat")),
                lon=float(_require(data, "lon")),
                event_ts=parse_ts(_require(data, "event_ts")),
                collected_at=parse_ts(_require(data, "collected_at")),
                reference_time=str(data.get("reference_time") or ""),
                source=str(data.get("source") or SOURCE_LIVE),
                schema_version=int(data.get("schema_version", SCHEMA_VERSION)),
            )
        except SchemaError:
            raise
        except (TypeError, ValueError, AttributeError) as exc:
            raise SchemaError(f"BusPosition invalido: {exc}") from exc


@dataclass(frozen=True)
class LineMetric(_Event):
    """Metricas de uma linha+sentido em uma janela fixa. Topico: bus-metrics.

    Chave: f"{line_code}-{direction}".
    """

    window_start: datetime
    window_end: datetime
    line_code: int
    line_sign: str
    direction: int
    avg_speed_kmh: float | None   # media das velocidades validas; None se nao houve amostra
    speed_samples: int            # quantidade de velocidades validas usadas na media
    active_vehicles: int          # veiculos distintos vistos na janela
    accessible_vehicles: int      # veiculos acessiveis distintos vistos na janela
    accessible_pct: float         # 100 * accessible_vehicles / active_vehicles
    computed_at: datetime
    schema_version: int = SCHEMA_VERSION

    def key(self) -> bytes:
        return f"{self.line_code}-{self.direction}".encode("utf-8")

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "LineMetric":
        try:
            avg = data.get("avg_speed_kmh")
            return cls(
                window_start=parse_ts(_require(data, "window_start")),
                window_end=parse_ts(_require(data, "window_end")),
                line_code=int(_require(data, "line_code")),
                line_sign=str(data.get("line_sign") or ""),
                direction=int(_require(data, "direction")),
                avg_speed_kmh=None if avg is None else float(avg),
                speed_samples=int(data.get("speed_samples", 0)),
                active_vehicles=int(_require(data, "active_vehicles")),
                accessible_vehicles=int(data.get("accessible_vehicles", 0)),
                accessible_pct=float(data.get("accessible_pct", 0.0)),
                computed_at=parse_ts(_require(data, "computed_at")),
                schema_version=int(data.get("schema_version", SCHEMA_VERSION)),
            )
        except SchemaError:
            raise
        except (TypeError, ValueError, AttributeError) as exc:
            raise SchemaError(f"LineMetric invalido: {exc}") from exc


@dataclass(frozen=True)
class BusAlert(_Event):
    """Alerta de veiculo parado fora de terminal. Topico: bus-alerts. Chave: vehicle_id."""

    alert_id: str               # deterministico: f"{vehicle_id}-{stopped_since:%Y%m%dT%H%M%SZ}"
    alert_type: str             # "STOPPED_VEHICLE"
    vehicle_id: str
    line_code: int
    line_sign: str
    direction: int
    lat: float
    lon: float
    stopped_since: datetime     # event_ts da primeira leitura lenta da sequencia
    detected_at: datetime       # event_ts da leitura que disparou o alerta
    stopped_minutes: float      # (detected_at - stopped_since) em minutos
    last_speed_kmh: float       # velocidade da leitura que disparou o alerta
    schema_version: int = SCHEMA_VERSION

    def key(self) -> bytes:
        return self.vehicle_id.encode("utf-8")

    @staticmethod
    def make_id(vehicle_id: str, stopped_since: datetime) -> str:
        return f"{vehicle_id}-{parse_ts(stopped_since):%Y%m%dT%H%M%SZ}"

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "BusAlert":
        try:
            return cls(
                alert_id=str(_require(data, "alert_id")),
                alert_type=str(data.get("alert_type") or ALERT_TYPE_STOPPED),
                vehicle_id=str(_require(data, "vehicle_id")),
                line_code=int(_require(data, "line_code")),
                line_sign=str(data.get("line_sign") or ""),
                direction=int(_require(data, "direction")),
                lat=float(_require(data, "lat")),
                lon=float(_require(data, "lon")),
                stopped_since=parse_ts(_require(data, "stopped_since")),
                detected_at=parse_ts(_require(data, "detected_at")),
                stopped_minutes=float(_require(data, "stopped_minutes")),
                last_speed_kmh=float(data.get("last_speed_kmh", 0.0)),
                schema_version=int(data.get("schema_version", SCHEMA_VERSION)),
            )
        except SchemaError:
            raise
        except (TypeError, ValueError, AttributeError) as exc:
            raise SchemaError(f"BusAlert invalido: {exc}") from exc


def flatten_posicao(
    payload: dict[str, Any],
    collected_at: datetime,
    source: str = SOURCE_LIVE,
) -> tuple[list[BusPosition], int]:
    """Achata a resposta de GET /Posicao em um BusPosition por veiculo.

    Formato da API (v2.1): {"hr": "HH:MM", "l": [{"c", "cl", "sl", "lt0", "lt1", "qv",
    "vs": [{"p", "a", "ta", "py", "px", ...}]}]}. Campos extras sao ignorados.

    Retorna (eventos, quantidade_de_veiculos_descartados_por_estar_malformados).
    Coordenadas zeradas NAO sao descartadas aqui: o evento bruto e preservado e a
    validacao acontece nos consumers de processamento.
    """
    events: list[BusPosition] = []
    skipped = 0
    reference_time = str(payload.get("hr") or "")
    lines: Iterable[dict[str, Any]] = payload.get("l") or []
    if not isinstance(lines, list):
        return events, 1
    for line in lines:
        if not isinstance(line, dict):
            skipped += 1
            continue
        vehicles = line.get("vs") or []
        if not isinstance(vehicles, list):
            skipped += 1
            continue
        try:
            line_code = int(line["cl"])
            direction = int(line["sl"])
        except (KeyError, TypeError, ValueError):
            skipped += len(vehicles)
            continue
        line_sign = str(line.get("c") or "")
        for vehicle in vehicles:
            try:
                events.append(
                    BusPosition(
                        vehicle_id=str(vehicle["p"]),
                        line_code=line_code,
                        line_sign=line_sign,
                        direction=direction,
                        destination_sign=str(line.get("lt0") or ""),
                        origin_sign=str(line.get("lt1") or ""),
                        accessible=bool(vehicle.get("a", False)),
                        lat=float(vehicle["py"]),
                        lon=float(vehicle["px"]),
                        event_ts=parse_ts(vehicle["ta"]),
                        collected_at=parse_ts(collected_at),
                        reference_time=reference_time,
                        source=source,
                    )
                )
            except (KeyError, TypeError, ValueError, AttributeError):
                skipped += 1
    return events, skipped


EVENT_FIELDS = {
    "BusPosition": [f.name for f in fields(BusPosition)],
    "LineMetric": [f.name for f in fields(LineMetric)],
    "BusAlert": [f.name for f in fields(BusAlert)],
}
