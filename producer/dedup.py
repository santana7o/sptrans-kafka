"""Deduplicacao de leituras antes de publicar no Kafka.

A API repete a mesma leitura (mesmo ``ta``) quando o veiculo nao enviou GPS novo entre
duas coletas. Guardamos o ultimo ``event_ts`` publicado por veiculo e descartamos tudo
que for igual ou mais antigo.
"""

from __future__ import annotations

from datetime import datetime
from typing import Iterable

from common.schemas import BusPosition


class Deduplicator:
    """Filtro por (vehicle_id, event_ts) com memoria do ultimo event_ts por veiculo."""

    def __init__(self) -> None:
        self._last: dict[str, datetime] = {}

    def __len__(self) -> int:
        return len(self._last)

    def last_event_ts(self, vehicle_id: str) -> datetime | None:
        return self._last.get(vehicle_id)

    def is_new(self, event: BusPosition) -> bool:
        last = self._last.get(event.vehicle_id)
        return last is None or event.event_ts > last

    def filter(self, events: Iterable[BusPosition]) -> tuple[list[BusPosition], int]:
        """Retorna (eventos novos, quantidade descartada). Atualiza a memoria."""
        kept: list[BusPosition] = []
        duplicates = 0
        for event in events:
            if not self.is_new(event):
                duplicates += 1
                continue
            self._last[event.vehicle_id] = event.event_ts
            kept.append(event)
        return kept, duplicates

    def reset(self) -> None:
        self._last.clear()
