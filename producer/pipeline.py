"""Etapa comum a live e replay: achatar, deduplicar, publicar e medir uma coleta."""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from common.schemas import flatten_posicao
from producer.dedup import Deduplicator
from producer.publisher import Publisher


@dataclass(frozen=True)
class CollectionStats:
    lines: int          # entradas linha+sentido no payload
    vehicles: int       # leituras validas apos o flatten
    published: int      # leituras novas enviadas ao Kafka
    duplicates: int     # leituras descartadas pelo deduplicador
    malformed: int      # veiculos descartados pelo flatten (campo ausente/invalido)
    publish_ms: float   # tempo gasto em flatten + dedup + publish


def process_collection(
    payload: dict[str, Any],
    collected_at: datetime,
    source: str,
    dedup: Deduplicator,
    publisher: Publisher,
) -> CollectionStats:
    started = time.monotonic()
    events, malformed = flatten_posicao(payload, collected_at, source=source)
    fresh, duplicates = dedup.filter(events)
    published = publisher.publish(fresh)
    return CollectionStats(
        lines=len(payload.get("l") or []),
        vehicles=len(events),
        published=published,
        duplicates=duplicates,
        malformed=malformed,
        publish_ms=round((time.monotonic() - started) * 1000, 1),
    )
