"""Consultas do dashboard: leem as views do PostgreSQL e devolvem DataFrames.

As views e colunas sao contrato (docs/CONTRACTS.md secao 5). O dashboard nunca fala com
o Kafka. Todo SQL e fixo ou parametrizado (sem f-string com input do usuario).

A conexao e unica por processo (``st.cache_resource``), em autocommit e somente leitura.
Se a conexao cair, ela e descartada e reaberta uma vez; se o banco continuar fora,
``DatabaseUnavailable`` e levantada para o app mostrar um aviso amigavel.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any, Sequence

import pandas as pd
import streamlit as st

from common.config import Settings, load_settings
from common.db import connect
from common.log import get_logger

log = get_logger(__name__)

# Janela de destaque no mapa: veiculo com alerta detectado ate N minutos antes da
# referencia de tempo (max(event_ts), DECISIONS D04) aparece em outra cor.
ALERT_HIGHLIGHT_MINUTES = 15

# Limite de linhas da tabela de alertas (a view ja restringe a 24 h).
ALERTS_LIMIT = 200

POSITIONS_COLUMNS = [
    "vehicle_id", "line_code", "line_sign", "direction", "accessible", "lat", "lon", "event_ts",
]
LINE_METRICS_COLUMNS = [
    "window_start", "window_end", "line_code", "line_sign", "direction", "avg_speed_kmh",
    "speed_samples", "active_vehicles", "accessible_vehicles", "accessible_pct", "computed_at",
]
CITY_KPIS_COLUMNS = [
    "active_buses", "operating_lines", "avg_city_speed_kmh", "last_event_ts", "latest_window_start",
]
ALERTS_COLUMNS = [
    "alert_id", "alert_type", "vehicle_id", "line_code", "line_sign", "direction", "lat", "lon",
    "stopped_since", "detected_at", "stopped_minutes", "last_speed_kmh", "created_at",
]

_TIMESTAMP_COLUMNS = {
    "event_ts", "window_start", "window_end", "computed_at", "last_event_ts",
    "latest_window_start", "stopped_since", "detected_at", "created_at",
}

SQL_LATEST_POSITIONS = (
    "SELECT " + ", ".join(POSITIONS_COLUMNS) + " FROM vw_latest_positions"
)
SQL_LATEST_LINE_METRICS = (
    "SELECT " + ", ".join(LINE_METRICS_COLUMNS) + " FROM vw_latest_line_metrics"
)
SQL_CITY_KPIS = "SELECT " + ", ".join(CITY_KPIS_COLUMNS) + " FROM vw_city_kpis"
SQL_RECENT_ALERTS = (
    "SELECT " + ", ".join(ALERTS_COLUMNS) + " FROM vw_recent_alerts"
    " ORDER BY detected_at DESC, alert_id LIMIT %(limit)s"
)
SQL_LINE_SIGNS = "SELECT DISTINCT line_sign FROM vw_latest_positions ORDER BY line_sign"


class DatabaseUnavailable(RuntimeError):
    """O PostgreSQL nao respondeu (fora do ar, credenciais erradas ou conexao perdida)."""


@st.cache_resource(show_spinner=False)
def get_settings() -> Settings:
    """Configuracao do processo (lida uma vez)."""
    return load_settings()


@st.cache_resource(show_spinner=False)
def _get_connection() -> Any:
    """Conexao compartilhada pelo processo do Streamlit (uma tentativa, sem bloquear a tela)."""
    conn = connect(get_settings(), timeout_s=0, autocommit=True)
    conn.read_only = True
    return conn


def _reset_connection(conn: Any | None) -> None:
    """Fecha ``conn`` (se aberta) e descarta o cache para que a proxima chamada reconecte."""
    if conn is not None:
        try:
            conn.close()
        except Exception:  # noqa: BLE001 - a conexao ja pode estar quebrada
            pass
    _get_connection.clear()


def _to_frame(rows: Sequence[Sequence[Any]], columns: list[str]) -> pd.DataFrame:
    """Monta o DataFrame e normaliza timestamps para datetime64 em UTC."""
    df = pd.DataFrame(list(rows), columns=columns)
    for col in columns:
        if col in _TIMESTAMP_COLUMNS:
            df[col] = pd.to_datetime(df[col], utc=True)
    return df


def run_query(sql: str, columns: list[str], params: dict[str, Any] | None = None) -> pd.DataFrame:
    """Executa uma consulta e devolve um DataFrame com ``columns`` (mesmo se vazio).

    Erro de conexao: reconecta uma vez. Persistindo, levanta ``DatabaseUnavailable``.
    """
    import psycopg

    last_error: Exception | None = None
    for attempt in (1, 2):
        conn = None
        try:
            conn = _get_connection()
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
            return _to_frame(rows, columns)
        except (psycopg.OperationalError, psycopg.InterfaceError) as exc:
            last_error = exc
            log.warning("falha ao consultar o postgres", attempt=attempt, error=str(exc).strip())
            _reset_connection(conn)
    raise DatabaseUnavailable(str(last_error).strip() if last_error else "postgres indisponivel")


def fetch_city_kpis() -> pd.DataFrame:
    """1 linha de ``vw_city_kpis`` (a view sempre devolve 1 linha)."""
    return run_query(SQL_CITY_KPIS, CITY_KPIS_COLUMNS)


def fetch_latest_positions() -> pd.DataFrame:
    """Ultima posicao de cada veiculo ativo (``vw_latest_positions``)."""
    return run_query(SQL_LATEST_POSITIONS, POSITIONS_COLUMNS)


def fetch_latest_line_metrics() -> pd.DataFrame:
    """Metricas por linha e sentido da janela mais recente (``vw_latest_line_metrics``)."""
    return run_query(SQL_LATEST_LINE_METRICS, LINE_METRICS_COLUMNS)


def fetch_recent_alerts(limit: int = ALERTS_LIMIT) -> pd.DataFrame:
    """Alertas das ultimas 24 h (``vw_recent_alerts``), do mais novo ao mais antigo."""
    return run_query(SQL_RECENT_ALERTS, ALERTS_COLUMNS, {"limit": int(limit)})


def fetch_line_signs() -> list[str]:
    """Letreiros das linhas com veiculos ativos (opcoes do filtro)."""
    df = run_query(SQL_LINE_SIGNS, ["line_sign"])
    return [str(v) for v in df["line_sign"].dropna().tolist()]


# ---------------------------------------------------------------------------
# Logica pura (sem banco), testavel com DataFrames de exemplo
# ---------------------------------------------------------------------------

def reference_time(kpis: pd.DataFrame, alerts: pd.DataFrame) -> pd.Timestamp | None:
    """Referencia de "agora": ultimo event_ts; sem posicoes, o ultimo detected_at (D04)."""
    if not kpis.empty and "last_event_ts" in kpis and pd.notna(kpis["last_event_ts"].iloc[0]):
        return pd.Timestamp(kpis["last_event_ts"].iloc[0])
    if not alerts.empty and alerts["detected_at"].notna().any():
        return pd.Timestamp(alerts["detected_at"].max())
    return None


def recently_alerted_vehicles(
    alerts: pd.DataFrame,
    reference: pd.Timestamp | None,
    minutes: int = ALERT_HIGHLIGHT_MINUTES,
) -> set[str]:
    """Veiculos com alerta detectado nos ``minutes`` anteriores a ``reference``."""
    if alerts.empty or reference is None:
        return set()
    cutoff = reference - timedelta(minutes=minutes)
    recent = alerts[alerts["detected_at"] >= cutoff]
    return {str(v) for v in recent["vehicle_id"].dropna()}


def filter_by_lines(df: pd.DataFrame, line_signs: Sequence[str]) -> pd.DataFrame:
    """Mantem apenas as linhas selecionadas (lista vazia = todas)."""
    if not line_signs or df.empty:
        return df
    return df[df["line_sign"].isin(list(line_signs))]


def speed_rankings(
    metrics: pd.DataFrame, top_n: int = 10, min_vehicles: int = 2
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """(mais lentas, mais rapidas) da ultima janela, ignorando velocidade nula e
    linhas com menos de ``min_vehicles`` veiculos ativos."""
    if metrics.empty:
        return metrics, metrics
    valid = metrics[metrics["avg_speed_kmh"].notna() & (metrics["active_vehicles"] >= min_vehicles)]
    slowest = valid.sort_values(["avg_speed_kmh", "line_sign"], ascending=[True, True]).head(top_n)
    fastest = valid.sort_values(["avg_speed_kmh", "line_sign"], ascending=[False, True]).head(top_n)
    return slowest, fastest
