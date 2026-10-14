"""Gerador do sample sintetico versionado: ``python -m producer.synthetic``.

Produz ``data/samples/sample_synthetic.jsonl.gz`` no formato EXATO da resposta de
GET /Posicao (CONTRACTS secoes 1 e 2), de forma deterministica (seed fixa, gzip com
mtime zerado: o arquivo sai identico byte a byte a cada execucao).

Cenario simulado:
- 25 linhas, 2 sentidos cada, trajetos entre pares de terminais de ``common.terminals``
  (com pontos intermediarios para o trajeto nao ser uma reta);
- 8 a 12 veiculos por linha/sentido, velocidade de cruzeiro 12-26 km/h com variacao
  por coleta (limites 8-35 km/h) e paradas curtas em pontos de onibus;
- ao chegar ao terminal o veiculo espera alguns minutos e volta no sentido oposto
  (aparece sob o ``cl`` do outro sentido, como na API real);
- coletas a cada 30 s durante 40 minutos; ``ta`` de cada veiculo tem fase propria;
- ruido de GPS de poucos metros; ~75% dos veiculos acessiveis.

Casos de borda (ver ``Manifest``):
(a) veiculos parados 14-16 min FORA de terminal (> 1 km de qualquer terminal): alertas;
(b) veiculos parados 14-17 min DENTRO do terminal (< 150 m do centro): sem alerta;
(c) leituras com py/px = 0;
(d) leituras repetidas (mesmo ``ta`` em coletas seguidas): duplicatas;
(e) saltos de GPS (> 100 km/h).
"""

from __future__ import annotations

import argparse
import gzip
import json
import math
import random
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from common.geo import haversine_m
from common.log import get_logger, setup_logging
from common.terminals import TERMINALS, Terminal, nearest_terminal
from common.timeutils import to_iso

log = get_logger("producer.synthetic")

SEED = 20261014
START = datetime(2026, 10, 14, 11, 0, 0, tzinfo=timezone.utc)   # 08:00 em Sao Paulo
LOCAL_OFFSET = timedelta(hours=-3)
INTERVAL_S = 30
COLLECTIONS = 81                 # 0..80 -> 40 minutos
COLLECT_DELAY_S = 4              # collected_at = instante da coleta + 4 s
N_LINES = 25
VEHICLES_PER_DIRECTION = (8, 12)
PAIR_DISTANCE_M = (5_000, 16_000)
CRUISE_KMH = (12.0, 26.0)
SPEED_LIMITS_KMH = (8.0, 35.0)
BUS_STOP_PROB = 0.06             # chance de uma coleta "parado no ponto" (30 s)
ACCESSIBLE_PROB = 0.75
GPS_NOISE_M = 3.0
DWELL_NORMAL_S = (120, 420)
DWELL_OFFSET_M = 40.0            # vaga no terminal: ate 40 m do centro
DIRECTION2_OFFSET = 32768        # na API real o cl do sentido 2 = cl do sentido 1 + 32768

N_STOP_OUTSIDE = 4               # caso (a)
STOP_OUTSIDE_S = (840, 960)
STOP_MIN_TERMINAL_DIST_M = 1_200
N_TERMINAL_WAIT = 4              # caso (b)
TERMINAL_WAIT_S = (840, 1020)
N_ZERO = 12                      # caso (c)
DUPLICATE_PROB = 0.015           # caso (d)
N_JUMPS = 2                      # caso (e)
JUMP_M = 2_500

DEFAULT_OUTPUT = Path(__file__).resolve().parents[1] / "data" / "samples" / "sample_synthetic.jsonl.gz"

M_PER_DEG_LAT = 111_195.0


# --------------------------------------------------------------------------- geometria


def _m_per_deg_lon(lat: float) -> float:
    return M_PER_DEG_LAT * math.cos(math.radians(lat))


def offset_point(lat: float, lon: float, dx_m: float, dy_m: float) -> tuple[float, float]:
    """Desloca um ponto em metros (dx para leste, dy para norte)."""
    return lat + dy_m / M_PER_DEG_LAT, lon + dx_m / _m_per_deg_lon(lat)


@dataclass
class Route:
    """Polilinha entre dois terminais, com interpolacao por distancia percorrida."""

    points: list[tuple[float, float]]
    cumulative: list[float] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.cumulative = [0.0]
        for (a_lat, a_lon), (b_lat, b_lon) in zip(self.points, self.points[1:]):
            self.cumulative.append(self.cumulative[-1] + haversine_m(a_lat, a_lon, b_lat, b_lon))

    @property
    def length(self) -> float:
        return self.cumulative[-1]

    def reversed(self) -> "Route":
        return Route(list(reversed(self.points)))

    def position(self, s: float) -> tuple[float, float]:
        s = min(max(s, 0.0), self.length)
        for i in range(1, len(self.cumulative)):
            if s <= self.cumulative[i] or i == len(self.cumulative) - 1:
                seg = self.cumulative[i] - self.cumulative[i - 1]
                f = 0.0 if seg == 0 else (s - self.cumulative[i - 1]) / seg
                (a_lat, a_lon), (b_lat, b_lon) = self.points[i - 1], self.points[i]
                return a_lat + f * (b_lat - a_lat), a_lon + f * (b_lon - a_lon)
        return self.points[-1]


def build_route(rng: random.Random, a: Terminal, b: Terminal) -> Route:
    """Trajeto A -> B com 3 pontos intermediarios deslocados perpendicularmente."""
    dx = (b.lon - a.lon) * _m_per_deg_lon(a.lat)
    dy = (b.lat - a.lat) * M_PER_DEG_LAT
    length = math.hypot(dx, dy)
    px, py = -dy / length, dx / length          # vetor unitario perpendicular
    points = [(a.lat, a.lon)]
    for frac in (0.25, 0.5, 0.75):
        side = rng.uniform(-0.09, 0.09) * length
        base_lat, base_lon = a.lat + frac * (b.lat - a.lat), a.lon + frac * (b.lon - a.lon)
        points.append(offset_point(base_lat, base_lon, px * side, py * side))
    points.append((b.lat, b.lon))
    return Route(points)


# --------------------------------------------------------------------------- modelo


@dataclass
class Line:
    sign: str
    code: int                    # cl do sentido 1
    principal: Terminal
    secondary: Terminal
    routes: dict[int, Route]     # sentido -> trajeto

    def cl(self, direction: int) -> int:
        return self.code if direction == 1 else self.code + DIRECTION2_OFFSET

    def signs(self, direction: int) -> tuple[str, str]:
        """(lt0 destino, lt1 origem) do sentido."""
        p, s = _terminal_sign(self.principal), _terminal_sign(self.secondary)
        return (s, p) if direction == 1 else (p, s)


def _terminal_sign(t: Terminal) -> str:
    return f"TERM. {t.name.upper()}"


@dataclass
class Bus:
    prefix: int
    accessible: bool
    line: Line
    direction: int
    s: float
    cruise_ms: float
    phase_s: int
    kind: str = "normal"                  # normal | stop_outside | terminal_wait
    state: str = "moving"                 # moving | dwell | stopped
    remaining_s: float = 0.0              # tempo restante em dwell/stopped
    stop_at_s: float | None = None
    stop_duration_s: float = 0.0
    dwell_offset: tuple[float, float] = (0.0, 0.0)
    last_ta: datetime | None = None
    last_reading: dict[str, Any] | None = None
    last_was_normal: bool = False
    bus_stop_last_tick: bool = False

    @property
    def route(self) -> Route:
        return self.line.routes[self.direction]


@dataclass
class Manifest:
    """O que foi plantado no sample (para validacao e documentacao)."""

    stop_outside: list[str] = field(default_factory=list)
    stop_outside_min_terminal_m: dict[str, float] = field(default_factory=dict)
    terminal_wait: list[str] = field(default_factory=list)
    zero_readings: int = 0
    duplicate_readings: int = 0
    gps_jumps: list[str] = field(default_factory=list)


def _cruise_ms(rng: random.Random) -> float:
    return rng.uniform(*CRUISE_KMH) / 3.6


def _tick_speed_ms(rng: random.Random, bus: Bus) -> float:
    """Velocidade da coleta: cruzeiro com variacao; as vezes parado no ponto (30 s)."""
    if bus.kind == "normal" and not bus.bus_stop_last_tick and rng.random() < BUS_STOP_PROB:
        bus.bus_stop_last_tick = True
        return rng.uniform(0.0, 0.4)
    bus.bus_stop_last_tick = False
    kmh = bus.cruise_ms * 3.6 * (1.0 + rng.gauss(0.0, 0.2))
    return min(max(kmh, SPEED_LIMITS_KMH[0]), SPEED_LIMITS_KMH[1]) / 3.6


def _start_dwell(rng: random.Random, bus: Bus, seconds: float) -> None:
    bus.state = "dwell"
    bus.remaining_s = seconds
    bus.s = bus.route.length
    angle = rng.uniform(0, 2 * math.pi)
    radius = rng.uniform(0, DWELL_OFFSET_M)
    bus.dwell_offset = (radius * math.cos(angle), radius * math.sin(angle))


def advance(rng: random.Random, bus: Bus, dt: float) -> None:
    """Avanca a simulacao do veiculo por ``dt`` segundos."""
    speed = _tick_speed_ms(rng, bus)
    remaining = dt
    while remaining > 1e-9:
        if bus.state in ("dwell", "stopped"):
            used = min(remaining, bus.remaining_s)
            bus.remaining_s -= used
            remaining -= used
            if bus.remaining_s <= 1e-9:
                if bus.state == "dwell":     # parte no sentido oposto
                    bus.direction = 3 - bus.direction
                    bus.s = 0.0
                    bus.dwell_offset = (0.0, 0.0)
                bus.state = "moving"
            continue
        if speed <= 0:
            return
        distance = speed * remaining
        if bus.stop_at_s is not None and bus.s < bus.stop_at_s <= bus.s + distance:
            remaining -= (bus.stop_at_s - bus.s) / speed
            bus.s = bus.stop_at_s
            bus.stop_at_s = None
            bus.state = "stopped"
            bus.remaining_s = bus.stop_duration_s
        elif bus.s + distance >= bus.route.length:
            remaining -= (bus.route.length - bus.s) / speed
            _start_dwell(rng, bus, rng.uniform(*DWELL_NORMAL_S))
        else:
            bus.s += distance
            remaining = 0.0


def bus_position(rng: random.Random, bus: Bus) -> tuple[float, float]:
    lat, lon = bus.route.position(bus.s)
    dx, dy = bus.dwell_offset if bus.state == "dwell" else (0.0, 0.0)
    return offset_point(lat, lon, dx + rng.gauss(0, GPS_NOISE_M), dy + rng.gauss(0, GPS_NOISE_M))


# --------------------------------------------------------------------------- montagem


def _make_lines(rng: random.Random) -> list[Line]:
    pairs = []
    for i, a in enumerate(TERMINALS):
        for b in TERMINALS[i + 1:]:
            if PAIR_DISTANCE_M[0] <= haversine_m(a.lat, a.lon, b.lat, b.lon) <= PAIR_DISTANCE_M[1]:
                pairs.append((a, b))
    chosen = rng.sample(pairs, N_LINES)
    codes = rng.sample(range(300, 2900), N_LINES)
    numbers = rng.sample(range(1000, 9000), N_LINES)
    lines = []
    for (a, b), code, number in zip(chosen, codes, numbers):
        if rng.random() < 0.15:
            base = f"{number // 10:03d}{rng.choice('ACHJKLMNP')}"
        else:
            base = f"{number:04d}"
        sign = f"{base}-{rng.choice(['10', '10', '10', '10', '21', '31', '41', '51'])}"
        if rng.random() < 0.5:
            a, b = b, a
        route = build_route(rng, a, b)
        lines.append(Line(sign, code, a, b, {1: route, 2: route.reversed()}))
    return sorted(lines, key=lambda line: line.code)


def _stop_point(rng: random.Random, route: Route) -> float | None:
    """Distancia ao longo do trajeto com o terminal mais proximo a mais de 1,2 km."""
    candidates = [route.length * f / 100 for f in range(25, 76)]
    rng.shuffle(candidates)
    for s in candidates:
        lat, lon = route.position(s)
        if nearest_terminal(lat, lon)[1] > STOP_MIN_TERMINAL_DIST_M:
            return s
    return None


def _make_buses(rng: random.Random, lines: list[Line], manifest: Manifest) -> list[Bus]:
    slots = [(line, d) for line in lines for d in (1, 2)]
    counts = [rng.randint(*VEHICLES_PER_DIRECTION) for _ in slots]
    prefixes = iter(rng.sample(range(10000, 90000), sum(counts)))
    buses: list[Bus] = []
    for (line, direction), count in zip(slots, counts):
        for _ in range(count):
            bus = Bus(
                prefix=next(prefixes),
                accessible=rng.random() < ACCESSIBLE_PROB,
                line=line,
                direction=direction,
                s=rng.uniform(0, line.routes[direction].length),
                cruise_ms=_cruise_ms(rng),
                phase_s=rng.randint(0, 24),
            )
            if rng.random() < 0.08:
                _start_dwell(rng, bus, rng.uniform(0, DWELL_NORMAL_S[1]))
            buses.append(bus)

    # (a) parados fora de terminal: um por linha distinta, parada 1,5-5 min apos o inicio
    special_lines: set[int] = set()
    for bus in rng.sample(buses, len(buses)):
        if len(manifest.stop_outside) >= N_STOP_OUTSIDE:
            break
        if bus.line.code in special_lines:
            continue
        stop_s = _stop_point(rng, bus.route)
        start_s = None if stop_s is None else stop_s - bus.cruise_ms * rng.uniform(90, 300)
        if start_s is None or start_s < 0:
            continue
        bus.kind, bus.state, bus.s = "stop_outside", "moving", start_s
        bus.stop_at_s = stop_s
        bus.stop_duration_s = rng.uniform(*STOP_OUTSIDE_S)
        special_lines.add(bus.line.code)
        lat, lon = bus.route.position(stop_s)
        manifest.stop_outside.append(str(bus.prefix))
        manifest.stop_outside_min_terminal_m[str(bus.prefix)] = round(nearest_terminal(lat, lon)[1], 1)

    # (b) longa espera dentro do terminal, ja no inicio do arquivo
    for bus in rng.sample(buses, len(buses)):
        if len(manifest.terminal_wait) >= N_TERMINAL_WAIT:
            break
        if bus.kind != "normal" or bus.line.code in special_lines:
            continue
        bus.kind = "terminal_wait"
        _start_dwell(rng, bus, rng.uniform(*TERMINAL_WAIT_S))
        special_lines.add(bus.line.code)
        manifest.terminal_wait.append(str(bus.prefix))
    return sorted(buses, key=lambda b: b.prefix)


def generate(seed: int = SEED) -> tuple[list[dict[str, Any]], Manifest]:
    """Gera as coletas (``{"collected_at", "payload"}``) e o manifesto dos casos de borda."""
    rng = random.Random(seed)
    manifest = Manifest()
    lines = _make_lines(rng)
    buses = _make_buses(rng, lines, manifest)

    normal = [b for b in buses if b.kind == "normal"]
    zero_plan = {(b.prefix, rng.randint(2, COLLECTIONS - 3)) for b in rng.sample(normal, N_ZERO)}
    zero_prefixes = {prefix for prefix, _ in zero_plan}
    jump_buses = rng.sample([b for b in normal if b.prefix not in zero_prefixes], N_JUMPS)
    jump_plan = {(b.prefix, rng.randint(10, COLLECTIONS - 10)) for b in jump_buses}
    manifest.gps_jumps = sorted(str(b.prefix) for b in jump_buses)

    records: list[dict[str, Any]] = []
    for tick in range(COLLECTIONS):
        tick_time = START + timedelta(seconds=tick * INTERVAL_S)
        groups: dict[tuple[int, int], list[dict[str, Any]]] = {}
        for bus in buses:
            ta = tick_time - timedelta(seconds=bus.phase_s + rng.randint(0, 2))
            if bus.last_ta is not None:
                advance(rng, bus, (ta - bus.last_ta).total_seconds())
            bus.last_ta = ta
            lat, lon = bus_position(rng, bus)
            key = (bus.prefix, tick)
            if key in zero_plan:
                reading = {"p": bus.prefix, "a": bus.accessible, "ta": to_iso(ta), "py": 0.0, "px": 0.0}
                manifest.zero_readings += 1
                bus.last_was_normal = False
            elif key in jump_plan:
                angle = rng.uniform(0, 2 * math.pi)
                jlat, jlon = offset_point(lat, lon, JUMP_M * math.cos(angle), JUMP_M * math.sin(angle))
                reading = {"p": bus.prefix, "a": bus.accessible, "ta": to_iso(ta),
                           "py": round(jlat, 6), "px": round(jlon, 6)}
                bus.last_was_normal = False
            elif (bus.kind == "normal" and bus.last_was_normal and bus.last_reading is not None
                  and rng.random() < DUPLICATE_PROB):
                reading = dict(bus.last_reading)       # mesma leitura (mesmo ta) repetida
                manifest.duplicate_readings += 1
                bus.last_was_normal = False
            else:
                reading = {"p": bus.prefix, "a": bus.accessible, "ta": to_iso(ta),
                           "py": round(lat, 6), "px": round(lon, 6)}
                bus.last_was_normal = True
            bus.last_reading = reading
            groups.setdefault((bus.line.code, bus.direction), []).append(reading)

        payload_lines = []
        for line in lines:
            for direction in (1, 2):
                vehicles = groups.get((line.code, direction))
                if not vehicles:
                    continue
                lt0, lt1 = line.signs(direction)
                payload_lines.append({
                    "c": line.sign, "cl": line.cl(direction), "sl": direction,
                    "lt0": lt0, "lt1": lt1, "qv": len(vehicles), "vs": vehicles,
                })
        records.append({
            "collected_at": to_iso(tick_time + timedelta(seconds=COLLECT_DELAY_S)),
            "payload": {"hr": f"{tick_time + LOCAL_OFFSET:%H:%M}", "l": payload_lines},
        })
    return records, manifest


def write_sample(records: list[dict[str, Any]], path: Path) -> int:
    """Grava JSON Lines em gzip deterministico (mtime 0, sem nome no cabecalho)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as raw, gzip.GzipFile(filename="", mode="wb", fileobj=raw,
                                                 mtime=0, compresslevel=9) as gz:
        for record in records:
            line = json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n"
            gz.write(line.encode("utf-8"))
    return path.stat().st_size


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="producer.synthetic", description="Gera o sample sintetico")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--seed", type=int, default=SEED)
    args = parser.parse_args(argv)
    setup_logging("synthetic", "INFO")

    records, manifest = generate(args.seed)
    size = write_sample(records, args.output)
    readings = sum(len(line["vs"]) for r in records for line in r["payload"]["l"])
    vehicles = {v["p"] for r in records for line in r["payload"]["l"] for v in line["vs"]}
    line_codes = {line["cl"] for r in records for line in r["payload"]["l"]}
    log.info(
        "sample sintetico gerado",
        file=str(args.output),
        size_bytes=size,
        collections=len(records),
        first=records[0]["collected_at"],
        last=records[-1]["collected_at"],
        line_directions=len(line_codes),
        vehicles=len(vehicles),
        readings=readings,
        stop_outside=manifest.stop_outside,
        stop_outside_min_terminal_m=manifest.stop_outside_min_terminal_m,
        terminal_wait=manifest.terminal_wait,
        zero_readings=manifest.zero_readings,
        duplicate_readings=manifest.duplicate_readings,
        gps_jumps=manifest.gps_jumps,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
