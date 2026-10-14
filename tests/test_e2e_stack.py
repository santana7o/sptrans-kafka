"""Teste ponta a ponta contra a stack Docker (``make up`` e depois ``make e2e``).

Pulado automaticamente quando o PostgreSQL ou o Kafka nao estao acessiveis. Espera ate
``WAIT_MINUTES`` por linhas em ``bus_positions``, ``line_metrics`` e ``bus_alerts`` e confere
as views do dashboard (contrato secao 5).
"""

from __future__ import annotations

import time
from datetime import timedelta

import pytest

from common.config import Settings, load_settings
from common.terminals import is_near_terminal

pytestmark = pytest.mark.e2e

WAIT_MINUTES = 8          # replay a 5x: janelas fecham em ~1,5 min e o 1o alerta sai em ~2,5 min
POLL_SECONDS = 5
ALERT_VEHICLES = {"86775", "53291", "78285", "85920"}
TERMINAL_WAITERS = {"22908", "46920", "58928", "61050"}
TOPICS_TIMEOUT_S = 5


@pytest.fixture(scope="module")
def settings() -> Settings:
    return load_settings()


@pytest.fixture(scope="module")
def db(settings: Settings):
    psycopg = pytest.importorskip("psycopg")
    try:
        conn = psycopg.connect(settings.postgres_dsn, connect_timeout=3, autocommit=True)
    except psycopg.OperationalError as exc:
        pytest.skip(f"postgres inacessivel em {settings.postgres_host}:{settings.postgres_port}: {type(exc).__name__}")
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def kafka_topics(settings: Settings) -> set[str]:
    admin_mod = pytest.importorskip("confluent_kafka.admin")
    admin = admin_mod.AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers,
                                   "socket.timeout.ms": 3000})
    try:
        metadata = admin.list_topics(timeout=TOPICS_TIMEOUT_S)
    except Exception as exc:  # noqa: BLE001 - qualquer falha = stack fora
        pytest.skip(f"kafka inacessivel em {settings.kafka_bootstrap_servers}: {type(exc).__name__}")
    return set(metadata.topics)


def _scalar(conn, sql: str):
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchone()[0]


def _rows(conn, sql: str) -> tuple[list[str], list[tuple]]:
    with conn.cursor() as cur:
        cur.execute(sql)
        return [d.name for d in cur.description], cur.fetchall()


@pytest.fixture(scope="module")
def stack_with_data(db, kafka_topics):
    """Espera ate as tres tabelas terem dados (ou falha com as contagens obtidas)."""
    deadline = time.monotonic() + WAIT_MINUTES * 60
    counts: dict[str, int] = {}
    while True:
        counts = {t: _scalar(db, f"SELECT count(*) FROM {t}")  # noqa: S608 - nomes fixos
                  for t in ("bus_positions", "line_metrics", "bus_alerts")}
        if all(counts.values()):
            return db, kafka_topics, counts
        if time.monotonic() > deadline:
            pytest.fail(f"stack sem dados apos {WAIT_MINUTES} min: {counts}")
        time.sleep(POLL_SECONDS)


def test_topics_exist(stack_with_data, settings):
    _, topics, _ = stack_with_data
    assert {settings.topic_raw, settings.topic_metrics, settings.topic_alerts} <= topics


def test_positions_are_unique_and_complete(stack_with_data):
    db, _, counts = stack_with_data
    assert counts["bus_positions"] > 0
    dup = _scalar(db, "SELECT count(*) FROM (SELECT 1 FROM bus_positions GROUP BY vehicle_id, event_ts "
                      "HAVING count(*) > 1) d")
    assert dup == 0
    assert _scalar(db, "SELECT count(DISTINCT source) FROM bus_positions") >= 1


def test_vw_latest_positions(stack_with_data):
    db, _, _ = stack_with_data
    cols, rows = _rows(db, "SELECT * FROM vw_latest_positions")
    assert cols == ["vehicle_id", "line_code", "line_sign", "direction", "accessible", "lat", "lon", "event_ts"]
    assert rows, "nenhum veiculo ativo"
    ids = [r[0] for r in rows]
    assert len(ids) == len(set(ids))                                    # uma linha por veiculo
    assert all(r[5] != 0 and r[6] != 0 for r in rows)                   # sem coordenada zerada
    reference = _scalar(db, "SELECT max(event_ts) FROM bus_positions")
    assert min(r[7] for r in rows) >= reference - timedelta(minutes=5)


def test_vw_latest_line_metrics(stack_with_data):
    db, _, _ = stack_with_data
    cols, rows = _rows(db, "SELECT * FROM vw_latest_line_metrics")
    assert cols[:11] == ["window_start", "window_end", "line_code", "line_sign", "direction", "avg_speed_kmh",
                         "speed_samples", "active_vehicles", "accessible_vehicles", "accessible_pct", "computed_at"]
    assert rows and len({r[0] for r in rows}) == 1                       # janela mais recente
    for r in rows:
        assert r[1] - r[0] == timedelta(minutes=5)
        assert r[5] is None or 0 <= r[5] <= 100
        assert 0 <= r[9] <= 100


def test_vw_city_kpis(stack_with_data):
    db, _, _ = stack_with_data
    cols, rows = _rows(db, "SELECT * FROM vw_city_kpis")
    assert cols == ["active_buses", "operating_lines", "avg_city_speed_kmh", "last_event_ts", "latest_window_start"]
    assert len(rows) == 1
    active, lines, speed, last_ts, window = rows[0]
    assert active > 0 and lines > 0 and last_ts is not None and window is not None
    assert speed is None or 0 < speed < 100


def test_vw_recent_alerts(stack_with_data, settings):
    db, _, _ = stack_with_data
    cols, rows = _rows(db, "SELECT * FROM vw_recent_alerts")
    assert cols[:12] == ["alert_id", "alert_type", "vehicle_id", "line_code", "line_sign", "direction", "lat",
                         "lon", "stopped_since", "detected_at", "stopped_minutes", "last_speed_kmh"]
    assert rows
    detected = [r[9] for r in rows]
    assert detected == sorted(detected, reverse=True)
    for r in rows:
        assert r[1] == "STOPPED_VEHICLE" and r[10] >= 10.0
        assert not is_near_terminal(r[6], r[7], settings.terminal_radius_m)
    if settings.mode == "replay" and settings.replay_file.endswith("sample_synthetic.jsonl.gz"):
        vehicles = {r[2] for r in rows}
        assert vehicles <= ALERT_VEHICLES
        assert not vehicles & TERMINAL_WAITERS
