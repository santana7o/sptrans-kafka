"""Modo replay: reproduz um arquivo de coletas (CONTRACTS secao 2) no Kafka.

Rebase de timestamps (DECISIONS D04): todos os ``ta`` e ``collected_at`` sao deslocados
para que a primeira coleta corresponda ao inicio da reproducao. A cada volta do loop
soma-se a duracao do arquivo mais um intervalo tipico entre coletas, para que nenhuma
leitura repita um ``event_ts`` ja publicado. ``REPLAY_SPEED`` divide as esperas.

Ancora: com ``REPLAY_SPEED`` > 1 o tempo de evento anda a frente do relogio.
Ao reiniciar, o inicio da reproducao e ``max(agora, ultimo instante ja publicado +
intervalo + defasagem da primeira coleta)`` (``replay_anchor``), para o tempo de evento
nunca voltar. O ultimo instante publicado vem de ``producer.topic_tail``.
"""

from __future__ import annotations

import gzip
import json
import math
import statistics
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterator

from common.config import Settings
from common.log import get_logger
from common.runtime import ShutdownFlag
from common.schemas import SOURCE_REPLAY
from common.timeutils import parse_ts, to_iso, utcnow
from producer.dedup import Deduplicator
from producer.pipeline import process_collection
from producer.publisher import Publisher

if TYPE_CHECKING:  # pragma: no cover
    from confluent_kafka import Producer

log = get_logger(__name__)

DEFAULT_GAP_S = 30.0


class ReplayFileError(ValueError):
    """Arquivo de replay ausente, vazio ou sem nenhuma coleta valida."""


# --------------------------------------------------------------------------- logica pura


def shift_iso(value: Any, delta: timedelta) -> Any:
    """Desloca um timestamp ISO 8601 por ``delta``. Valor invalido e devolvido intacto."""
    try:
        return to_iso(parse_ts(value) + delta)
    except (TypeError, ValueError, AttributeError):
        return value


def shift_hhmm(value: Any, delta: timedelta) -> Any:
    """Desloca o ``hr`` ("HH:MM", hora local) por ``delta``, modulo 24 h."""
    try:
        hours, minutes = str(value).split(":")
        total = (int(hours) * 60 + int(minutes) + int(delta.total_seconds() // 60)) % (24 * 60)
    except (TypeError, ValueError):
        return value
    return f"{total // 60:02d}:{total % 60:02d}"


def rebase_payload(payload: dict[str, Any], delta: timedelta) -> dict[str, Any]:
    """Nova copia do payload de /Posicao com ``vs[].ta`` e ``hr`` deslocados por ``delta``.

    Funcao pura: o payload original nao e alterado. Campos desconhecidos sao preservados.
    """
    new_lines = []
    for line in payload.get("l") or []:
        if not isinstance(line, dict):
            new_lines.append(line)
            continue
        new_line = dict(line)
        vehicles = line.get("vs")
        if isinstance(vehicles, list):
            new_vehicles = []
            for vehicle in vehicles:
                if isinstance(vehicle, dict) and "ta" in vehicle:
                    vehicle = {**vehicle, "ta": shift_iso(vehicle["ta"], delta)}
                new_vehicles.append(vehicle)
            new_line["vs"] = new_vehicles
        new_lines.append(new_line)
    out = dict(payload)
    out["l"] = new_lines
    if "hr" in payload:
        out["hr"] = shift_hhmm(payload["hr"], delta)
    return out


def replay_anchor(
    now: datetime,
    last_event_ts: datetime | None,
    interval: timedelta,
    lead: timedelta = timedelta(0),
) -> datetime:
    """Instante em que a primeira coleta do arquivo sera reproduzida.

    Sem nada publicado (``last_event_ts`` None), e ``now``. Caso contrario,
    ``max(now, last_event_ts + interval + lead)``: ``interval`` e o intervalo entre coletas e
    ``lead`` a defasagem entre ``collected_at`` e o ``ta`` mais antigo da primeira coleta,
    de modo que nenhuma leitura nova fique antes ou no mesmo instante do que ja foi publicado.
    """
    if last_event_ts is None:
        return now
    return max(now, last_event_ts + interval + max(lead, timedelta(0)))


def lap_delta(base_delta: timedelta, lap: int, file_duration: timedelta, gap: timedelta) -> timedelta:
    """Deslocamento da volta ``lap`` (0 = primeira): base + lap * (duracao + intervalo)."""
    return base_delta + lap * (file_duration + gap)


def replay_wait_seconds(original_offset_s: float, speed: float, elapsed_s: float) -> float:
    """Espera ate a coleta cujo deslocamento original desde o inicio e ``original_offset_s``.

    O alvo e ``original_offset_s / speed`` segundos depois do inicio da reproducao;
    ``elapsed_s`` e quanto ja passou. Agendar pelo inicio evita acumular atraso.
    """
    return max(0.0, original_offset_s / speed - elapsed_s)


# --------------------------------------------------------------------------- leitura do arquivo


@dataclass(frozen=True)
class ReplayFileInfo:
    path: Path
    collections: int
    first: datetime
    last: datetime
    gap: timedelta       # mediana do intervalo entre coletas (usado entre voltas)
    first_lead: timedelta = timedelta(0)  # collected_at - ta mais antigo da primeira coleta

    @property
    def duration(self) -> timedelta:
        return self.last - self.first


def iter_collections(path: Path) -> Iterator[tuple[datetime, dict[str, Any]]]:
    """Le o arquivo (gzip ou texto) linha a linha. Linhas invalidas sao logadas e puladas."""
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as fh:
        for number, raw in enumerate(fh, start=1):
            raw = raw.strip()
            if not raw:
                continue
            try:
                record = json.loads(raw)
                collected_at = parse_ts(record["collected_at"])
                payload = record["payload"]
                if not isinstance(payload, dict):
                    raise TypeError("payload nao e objeto")
            except (ValueError, KeyError, TypeError) as exc:
                log.warning("linha invalida no arquivo de replay", line=number, error=str(exc))
                continue
            yield collected_at, payload


def collection_lead(collected_at: datetime, payload: dict[str, Any]) -> timedelta:
    """``collected_at`` menos o ``ta`` valido mais antigo da coleta (0 se nao houver ou se negativo)."""
    oldest: datetime | None = None
    for line in payload.get("l") or []:
        if not isinstance(line, dict) or not isinstance(line.get("vs"), list):
            continue
        for vehicle in line["vs"]:
            try:
                ta = parse_ts(vehicle["ta"])
            except (KeyError, TypeError, ValueError, AttributeError):
                continue
            if oldest is None or ta < oldest:
                oldest = ta
    if oldest is None:
        return timedelta(0)
    return max(timedelta(0), collected_at - oldest)


def scan_file(path: Path) -> ReplayFileInfo:
    """Primeira passada: conta coletas e mede duracao, intervalo tipico e defasagem inicial."""
    if not path.is_file():
        raise ReplayFileError(f"arquivo de replay nao encontrado: {path}")
    stamps: list[datetime] = []
    first_lead = timedelta(0)
    for collected_at, payload in iter_collections(path):
        if not stamps:
            first_lead = collection_lead(collected_at, payload)
        stamps.append(collected_at)
    if not stamps:
        raise ReplayFileError(f"arquivo de replay sem coletas validas: {path}")
    diffs = [(b - a).total_seconds() for a, b in zip(stamps, stamps[1:]) if b > a]
    gap = statistics.median(diffs) if diffs else DEFAULT_GAP_S
    return ReplayFileInfo(
        path, len(stamps), stamps[0], stamps[-1], timedelta(seconds=max(1, round(gap))), first_lead
    )


# --------------------------------------------------------------------------- loop


def run_replay(
    settings: Settings,
    producer: "Producer",
    shutdown: ShutdownFlag,
    last_event_ts: datetime | None = None,
) -> None:
    """Reproduz o arquivo. ``last_event_ts``: maior instante ja publicado no topico."""
    info = scan_file(Path(settings.replay_file))
    publisher = Publisher(producer, settings.topic_raw)
    dedup = Deduplicator()
    speed = settings.replay_speed
    rebase = settings.replay_rebase_timestamps
    now = utcnow()
    start_at = replay_anchor(now, last_event_ts, info.gap, info.first_lead)
    # Segundos inteiros (arredondando para cima): timestamps rebaseados sem fracao de segundo
    # e nunca antes da ancora.
    base_delta = timedelta(seconds=math.ceil((start_at - info.first).total_seconds())) if rebase else timedelta(0)
    cycle_s = (info.duration + info.gap).total_seconds()
    log.info(
        "modo replay iniciado",
        file=str(info.path),
        collections=info.collections,
        duration_s=info.duration.total_seconds(),
        gap_s=info.gap.total_seconds(),
        speed=speed,
        loop=settings.replay_loop,
        rebase=rebase,
        start_at=to_iso(info.first + base_delta) if rebase else None,
        last_published=to_iso(last_event_ts) if last_event_ts else None,
        ahead_of_clock_s=round((start_at - now).total_seconds()) if rebase else 0,
    )
    if settings.replay_loop and not rebase:
        log.warning("loop sem rebase: a partir da segunda volta todas as leituras serao duplicatas")

    start_mono = time.monotonic()
    lap = 0
    total_published = 0
    while not shutdown.is_set():
        delta = lap_delta(base_delta, lap, info.duration, info.gap) if rebase else timedelta(0)
        lap_published = 0
        for index, (collected_at, payload) in enumerate(iter_collections(info.path)):
            offset_s = lap * cycle_s + (collected_at - info.first).total_seconds()
            if shutdown.wait(replay_wait_seconds(offset_s, speed, time.monotonic() - start_mono)):
                break
            try:
                if rebase:
                    payload = rebase_payload(payload, delta)
                stats = process_collection(payload, collected_at + delta, SOURCE_REPLAY, dedup, publisher)
            except Exception as exc:  # noqa: BLE001 - uma coleta ruim nao derruba o replay
                log.exception("erro ao reproduzir coleta", lap=lap, index=index, error_type=type(exc).__name__)
                continue
            lap_published += stats.published
            log.info(
                "coleta reproduzida",
                lap=lap,
                index=index,
                collected_at=to_iso(collected_at + delta),
                lines=stats.lines,
                vehicles=stats.vehicles,
                published=stats.published,
                duplicates=stats.duplicates,
                malformed=stats.malformed,
                publish_ms=stats.publish_ms,
                delivery_failures=publisher.failed,
            )
        total_published += lap_published
        if shutdown.is_set():
            break
        log.info("volta do replay concluida", lap=lap, published=lap_published)
        if not settings.replay_loop:
            break
        lap += 1
    log.info("modo replay encerrado", laps=lap + 1, published=total_published,
             delivery_failures=publisher.failed)
