"""Dashboard Streamlit: onibus de Sao Paulo em tempo (quase) real.

Executar a partir da raiz do repositorio:
    streamlit run dashboard/app.py

Le apenas as views do PostgreSQL (docs/CONTRACTS.md secao 5) por meio de
``dashboard.queries``. A parte de dados roda dentro de um ``st.fragment`` com
``run_every``, entao a atualizacao automatica nao recarrega a pagina nem perde o filtro.
"""

from __future__ import annotations

import sys
from pathlib import Path

# O `streamlit run dashboard/app.py` coloca apenas a pasta `dashboard/` no sys.path.
# Inserimos a raiz do repositorio explicitamente para que `import common...` e
# `from dashboard import queries` funcionem fora do container (no compose o
# PYTHONPATH=/app ja resolve isso; a insercao e inofensiva).
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from datetime import timedelta  # noqa: E402

import pandas as pd  # noqa: E402
import pydeck as pdk  # noqa: E402
import streamlit as st  # noqa: E402

from common.config import ConfigError  # noqa: E402
from common.log import get_logger, setup_logging  # noqa: E402
from dashboard import queries  # noqa: E402

LOCAL_TZ = "America/Sao_Paulo"
SP_CENTER = {"latitude": -23.5505, "longitude": -46.6333, "zoom": 10}
COLOR_NORMAL = [30, 110, 200, 170]
COLOR_ALERT = [220, 40, 40, 235]
FILTER_KEY = "line_filter"
DEFAULT_REFRESH_SECONDS = 30

log = get_logger("dashboard")


@st.cache_resource(show_spinner=False)
def _init_logging(level: str) -> bool:
    """Configura o log JSON uma unica vez por processo."""
    setup_logging("dashboard", level)
    return True


# ---------------------------------------------------------------------------
# Formatacao
# ---------------------------------------------------------------------------

def to_local(ts: pd.Timestamp | None) -> pd.Timestamp | None:
    if ts is None or pd.isna(ts):
        return None
    ts = pd.Timestamp(ts)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.tz_convert(LOCAL_TZ)


def fmt_time(ts: pd.Timestamp | None, pattern: str = "%H:%M:%S") -> str:
    local = to_local(ts)
    return local.strftime(pattern) if local is not None else "-"


def fmt_local_series(series: pd.Series, pattern: str = "%d/%m %H:%M:%S") -> pd.Series:
    return series.map(lambda v: fmt_time(v, pattern))


def direction_label(value: object) -> str:
    try:
        return {1: "ida", 2: "volta"}.get(int(value), str(value))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return "-"


# ---------------------------------------------------------------------------
# Secoes
# ---------------------------------------------------------------------------

def render_kpis(kpis: pd.DataFrame) -> None:
    row = kpis.iloc[0] if not kpis.empty else pd.Series(dtype=object)
    active = int(row.get("active_buses", 0) or 0)
    lines = int(row.get("operating_lines", 0) or 0)
    speed = row.get("avg_city_speed_kmh")
    last_ts = row.get("last_event_ts")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Onibus ativos", f"{active:,}".replace(",", "."))
    c2.metric(
        "Velocidade media da cidade",
        f"{speed:.1f} km/h".replace(".", ",") if speed is not None and pd.notna(speed) else "-",
    )
    c3.metric("Linhas operando", f"{lines:,}".replace(",", "."))
    c4.metric("Ultimo dado (horario de SP)", fmt_time(last_ts, "%d/%m %H:%M:%S"))


def build_map_frame(positions: pd.DataFrame, alerted: set[str]) -> pd.DataFrame:
    """Colunas simples e serializaveis em JSON para o pydeck (sem datetime)."""
    df = positions.copy()
    df["vehicle_id"] = df["vehicle_id"].astype(str)
    df["has_alert"] = df["vehicle_id"].isin(alerted)
    df["color"] = df["has_alert"].map(lambda a: COLOR_ALERT if a else COLOR_NORMAL)
    df["radius"] = df["has_alert"].map(lambda a: 90 if a else 45)
    df["direction_label"] = df["direction"].map(direction_label)
    df["time_label"] = fmt_local_series(df["event_ts"], "%H:%M:%S")
    df["alert_label"] = df["has_alert"].map(lambda a: "parado (alerta recente)" if a else "")
    cols = [
        "vehicle_id", "line_sign", "direction_label", "time_label", "alert_label",
        "lat", "lon", "color", "radius", "has_alert",
    ]
    # Pontos com alerta por ultimo para ficarem por cima dos demais.
    return df[cols].sort_values("has_alert", kind="stable").reset_index(drop=True)


def render_map(positions: pd.DataFrame, alerted: set[str]) -> None:
    st.subheader("Mapa ao vivo")
    if positions.empty:
        st.info("Nenhum veiculo ativo para os filtros atuais.")
        return
    data = build_map_frame(positions, alerted)
    layer = pdk.Layer(
        "ScatterplotLayer",
        data=data,
        get_position="[lon, lat]",
        get_fill_color="color",
        get_radius="radius",
        radius_min_pixels=2,
        radius_max_pixels=12,
        pickable=True,
        auto_highlight=True,
    )
    tooltip = {
        "html": (
            "<b>Linha {line_sign}</b><br/>Prefixo: {vehicle_id}<br/>"
            "Sentido: {direction_label}<br/>Horario: {time_label}<br/>{alert_label}"
        ),
        "style": {"fontSize": "12px"},
    }
    deck = pdk.Deck(
        layers=[layer],
        initial_view_state=pdk.ViewState(**SP_CENTER, pitch=0),
        map_provider="carto",
        map_style="light",  # estilo Carto: nao exige token do Mapbox
        tooltip=tooltip,
    )
    st.pydeck_chart(deck, use_container_width=True)
    n_alert = int(data["has_alert"].sum())
    st.caption(
        f"{len(data)} veiculos no mapa. Em vermelho: {n_alert} com alerta de parada nos "
        f"ultimos {queries.ALERT_HIGHLIGHT_MINUTES} minutos."
    )


def _ranking_table(df: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Linha": df["line_sign"].astype(str),
            "Sentido": df["direction"].map(direction_label),
            "Velocidade media (km/h)": df["avg_speed_kmh"].astype(float).round(1),
            "Veiculos": df["active_vehicles"].astype(int),
        }
    ).reset_index(drop=True)


def render_rankings(metrics: pd.DataFrame) -> None:
    st.subheader("Velocidade por linha (ultima janela)")
    if not metrics.empty and metrics["window_start"].notna().any():
        start = metrics["window_start"].max()
        end = metrics["window_end"].max()
        st.caption(f"Janela de {fmt_time(start, '%H:%M')} a {fmt_time(end, '%H:%M')} (horario de SP), linhas com 2 ou mais veiculos.")
    slowest, fastest = queries.speed_rankings(metrics)
    if slowest.empty:
        st.info("Ainda nao ha metricas de velocidade para os filtros atuais.")
        return
    left, right = st.columns(2)
    with left:
        st.markdown("**10 linhas mais lentas**")
        st.dataframe(_ranking_table(slowest), hide_index=True, use_container_width=True)
    with right:
        st.markdown("**10 linhas mais rapidas**")
        st.dataframe(_ranking_table(fastest), hide_index=True, use_container_width=True)


def render_alerts(alerts: pd.DataFrame) -> None:
    st.subheader("Alertas recentes")
    if alerts.empty:
        st.info("Nenhum alerta nas ultimas 24 horas para os filtros atuais.")
        return
    table = pd.DataFrame(
        {
            "Detectado em": fmt_local_series(alerts["detected_at"]),
            "Linha": alerts["line_sign"].astype(str),
            "Sentido": alerts["direction"].map(direction_label),
            "Prefixo": alerts["vehicle_id"].astype(str),
            "Tipo": alerts["alert_type"].astype(str),
            "Parado desde": fmt_local_series(alerts["stopped_since"]),
            "Minutos parado": alerts["stopped_minutes"].astype(float).round(1),
            "Ultima velocidade (km/h)": alerts["last_speed_kmh"].astype(float).round(1),
        }
    ).reset_index(drop=True)
    st.dataframe(table, hide_index=True, use_container_width=True)


# ---------------------------------------------------------------------------
# Pagina
# ---------------------------------------------------------------------------

def render_live(selected_lines: list[str]) -> None:
    """Le as views e desenha KPIs, mapa, rankings e alertas."""
    try:
        kpis = queries.fetch_city_kpis()
        positions = queries.fetch_latest_positions()
        metrics = queries.fetch_latest_line_metrics()
        alerts = queries.fetch_recent_alerts()
    except queries.DatabaseUnavailable as exc:
        log.warning("dashboard sem banco", error=str(exc))
        st.warning(
            "Banco de dados indisponivel no momento. O painel tenta novamente a cada "
            "atualizacao automatica."
        )
        return
    except Exception as exc:  # noqa: BLE001 - nunca mostrar traceback na tela
        log.exception("erro ao carregar dados do dashboard", error=str(exc))
        st.error("Nao foi possivel carregar os dados agora. Detalhes no log do servico.")
        return

    no_data = positions.empty and metrics.empty and alerts.empty
    if no_data:
        st.info(
            "Ainda nao ha dados no banco. Assim que o producer e os consumers gravarem as "
            "primeiras leituras, o painel e preenchido automaticamente."
        )

    render_kpis(kpis)

    reference = queries.reference_time(kpis, alerts)
    alerted = queries.recently_alerted_vehicles(alerts, reference)

    render_map(queries.filter_by_lines(positions, selected_lines), alerted)
    render_rankings(queries.filter_by_lines(metrics, selected_lines))
    render_alerts(queries.filter_by_lines(alerts, selected_lines))

    st.caption(
        "Referencia de tempo: horario do ultimo dado recebido (no replay acelerado ele "
        "pode estar a frente do relogio)."
    )


def line_options(selected: list[str]) -> list[str]:
    """Letreiros disponiveis no filtro, sempre incluindo os ja selecionados."""
    try:
        available = queries.fetch_line_signs()
    except Exception as exc:  # noqa: BLE001 - sem banco o filtro fica so com a selecao atual
        log.warning("opcoes do filtro indisponiveis", error=str(exc))
        available = []
    return sorted(set(available) | set(selected))


def main() -> None:
    st.set_page_config(page_title="Onibus de Sao Paulo ao vivo", layout="wide")

    try:
        settings = queries.get_settings()
        _init_logging(settings.log_level)
        refresh_seconds = max(5, int(settings.dashboard_refresh_seconds))
    except ConfigError as exc:
        st.error(f"Configuracao invalida: {exc}")
        return
    except Exception:  # noqa: BLE001
        refresh_seconds = DEFAULT_REFRESH_SECONDS

    st.title("Onibus de Sao Paulo em tempo real")
    st.caption(
        f"Posicoes GPS da SPTrans processadas via Kafka. Atualizacao automatica a cada "
        f"{refresh_seconds} s."
    )

    with st.sidebar:
        st.header("Filtros")
        current = list(st.session_state.get(FILTER_KEY, []))
        st.multiselect(
            "Linhas (letreiro)",
            options=line_options(current),
            key=FILTER_KEY,
            placeholder="Todas as linhas",
            help="Afeta o mapa, os rankings e a tabela de alertas. Vazio = todas.",
        )
        # A lista de opcoes e lida na execucao completa da pagina; o clique no botao
        # dispara essa execucao (o fragmento abaixo so atualiza os dados).
        st.button("Atualizar lista de linhas")

    @st.fragment(run_every=timedelta(seconds=refresh_seconds))
    def live_section() -> None:
        render_live(list(st.session_state.get(FILTER_KEY, [])))

    live_section()


main()
