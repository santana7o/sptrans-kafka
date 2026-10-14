"""Gravacao em lote de BusPosition na tabela bus_positions (PostgreSQL, psycopg 3).

``insert_positions`` faz um unico ``INSERT ... SELECT FROM unnest(arrays)`` com
``ON CONFLICT (vehicle_id, event_ts) DO NOTHING``: uma ida ao banco por lote, idempotente
(reprocessar o mesmo lote nao duplica linhas) e ``rowcount`` exato de linhas novas.
Nao faz commit: a transacao e controlada por quem chama.

``PostgresPositionWriter`` encapsula a conexao (commit, rollback, reconexao) e e o
writer usado pelo loop do consumer.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

from common.config import Settings
from common.db import connect
from common.log import get_logger
from common.schemas import BusPosition

if TYPE_CHECKING:  # pragma: no cover
    import psycopg

log = get_logger(__name__)

INSERT_SQL = """
INSERT INTO bus_positions (
  vehicle_id, line_code, line_sign, direction, destination_sign, origin_sign,
  accessible, lat, lon, event_ts, collected_at, source
)
SELECT * FROM unnest(
  %s::text[], %s::integer[], %s::text[], %s::smallint[], %s::text[], %s::text[],
  %s::boolean[], %s::double precision[], %s::double precision[],
  %s::timestamptz[], %s::timestamptz[], %s::text[]
)
ON CONFLICT (vehicle_id, event_ts) DO NOTHING
"""

INSERT_ONE_SQL = """
INSERT INTO bus_positions (
  vehicle_id, line_code, line_sign, direction, destination_sign, origin_sign,
  accessible, lat, lon, event_ts, collected_at, source
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (vehicle_id, event_ts) DO NOTHING
"""


def _row(p: BusPosition) -> tuple:
    return (
        p.vehicle_id,
        p.line_code,
        p.line_sign,
        p.direction,
        p.destination_sign,
        p.origin_sign,
        p.accessible,
        p.lat,
        p.lon,
        p.event_ts,
        p.collected_at,
        p.source,
    )


def insert_positions(conn: "psycopg.Connection", positions: Sequence[BusPosition]) -> int:
    """Insere o lote em um unico comando. Retorna quantas linhas novas foram gravadas.

    Duplicatas (no banco ou dentro do proprio lote) sao ignoradas pelo ON CONFLICT.
    Nao faz commit.
    """
    if not positions:
        return 0
    columns = [list(col) for col in zip(*(_row(p) for p in positions))]
    with conn.cursor() as cur:
        cur.execute(INSERT_SQL, columns)
        return max(cur.rowcount, 0)


def insert_positions_one_by_one(conn: "psycopg.Connection", positions: Sequence[BusPosition]) -> tuple[int, int]:
    """Fallback para lote com dado invalido: grava linha a linha, cada uma em um savepoint.

    Retorna (linhas_novas, linhas_rejeitadas). Nao faz commit.
    """
    import psycopg

    inserted = 0
    rejected = 0
    with conn.cursor() as cur:
        for p in positions:
            try:
                with conn.transaction():
                    cur.execute(INSERT_ONE_SQL, _row(p))
                    inserted += max(cur.rowcount, 0)
            except psycopg.DataError as exc:
                rejected += 1
                log.warning(
                    "posicao rejeitada pelo banco",
                    vehicle_id=p.vehicle_id,
                    event_ts=str(p.event_ts),
                    error=str(exc).strip(),
                )
    return inserted, rejected


class PostgresPositionWriter:
    """Writer transacional de posicoes com reconexao sob demanda.

    ``write`` grava e faz commit; qualquer falha faz rollback e propaga a excecao para o
    chamador decidir (retry com backoff). ``reset`` descarta a conexao atual para forcar
    reconexao na proxima escrita.
    """

    def __init__(self, settings: Settings, connect_timeout_s: float = 0.0) -> None:
        self._settings = settings
        self._connect_timeout_s = connect_timeout_s
        self._conn: "psycopg.Connection | None" = None

    def connect(self, timeout_s: float | None = None) -> None:
        """Abre a conexao se necessario (com retry ate ``timeout_s``, via common.db)."""
        if self._conn is None or self._conn.closed:
            self._conn = connect(
                self._settings,
                timeout_s=self._connect_timeout_s if timeout_s is None else timeout_s,
            )

    def write(self, positions: Sequence[BusPosition]) -> int:
        """Grava o lote em uma transacao e faz commit. Retorna linhas novas."""
        import psycopg

        if not positions:
            return 0
        self.connect()
        conn = self._conn
        assert conn is not None
        try:
            try:
                inserted = insert_positions(conn, positions)
            except psycopg.DataError as exc:
                # Valor que o banco nao aceita (ex.: caractere NUL em texto). Isola as
                # linhas ruins para que um evento nao bloqueie o lote inteiro para sempre.
                conn.rollback()
                log.warning("lote com dado invalido, gravando linha a linha", error=str(exc).strip())
                inserted, _ = insert_positions_one_by_one(conn, positions)
            conn.commit()
            return inserted
        except Exception:
            self._safe_rollback()
            raise

    def _safe_rollback(self) -> None:
        if self._conn is None or self._conn.closed:
            return
        try:
            self._conn.rollback()
        except Exception as exc:  # noqa: BLE001 - conexao possivelmente quebrada
            log.warning("rollback falhou", error=str(exc).strip())

    def reset(self) -> None:
        """Fecha a conexao (se houver) para que a proxima escrita reconecte."""
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:  # noqa: BLE001
                pass
        self._conn = None

    def close(self) -> None:
        self.reset()
