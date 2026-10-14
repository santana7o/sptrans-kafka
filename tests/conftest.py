"""Fixtures compartilhadas: fabrica de BusPosition, Settings de teste, payload de /Posicao e sample."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from common.config import Settings, load_settings
from common.schemas import BusPosition, SOURCE_REPLAY

ROOT = Path(__file__).resolve().parent.parent
SAMPLE_PATH = ROOT / "data" / "samples" / "sample_synthetic.jsonl.gz"

# Instante base dos testes (UTC) e um ponto parado longe de qualquer terminal (Vila Mariana).
BASE_TS = datetime(2026, 10, 14, 12, 0, 0, tzinfo=timezone.utc)
FREE_LAT, FREE_LON = -23.5900, -46.6600
# Centro do Terminal Lapa (common/terminals.py).
LAPA_LAT, LAPA_LON = -23.5220, -46.7050

# Todas as variaveis lidas por common.config.load_settings.
CONFIG_ENV_VARS = (
    "MODE", "LOG_LEVEL", "KAFKA_BOOTSTRAP_SERVERS", "TOPIC_RAW", "TOPIC_METRICS", "TOPIC_ALERTS",
    "SPTRANS_BASE_URL", "SPTRANS_TOKEN", "POLL_INTERVAL_SECONDS", "HTTP_TIMEOUT_SECONDS",
    "HTTP_MAX_RETRIES", "REPLAY_FILE", "REPLAY_SPEED", "REPLAY_LOOP", "REPLAY_REBASE_TIMESTAMPS",
    "RECORD_OUTPUT_DIR", "RECORD_COLLECTIONS", "POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB",
    "POSTGRES_USER", "POSTGRES_PASSWORD", "PERSISTENCE_BATCH_SIZE", "PERSISTENCE_FLUSH_SECONDS",
    "WINDOW_MINUTES", "WINDOW_LATENESS_SECONDS", "IDLE_FLUSH_SECONDS", "MAX_SPEED_KMH",
    "MIN_INTERVAL_SECONDS", "MAX_INTERVAL_SECONDS", "ALERT_SPEED_KMH", "ALERT_MIN_MINUTES",
    "TERMINAL_RADIUS_M", "DASHBOARD_REFRESH_SECONDS",
)


def make_position(
    vehicle_id: str = "11433",
    t: float = 0.0,
    lat: float = FREE_LAT,
    lon: float = FREE_LON,
    line_code: int = 1273,
    direction: int = 1,
    line_sign: str = "8000-10",
    accessible: bool = True,
    event_ts: datetime | None = None,
    collected_at: datetime | None = None,
    source: str = SOURCE_REPLAY,
) -> BusPosition:
    """BusPosition com ``event_ts = BASE_TS + t segundos`` (ou ``event_ts`` explicito)."""
    ts = event_ts if event_ts is not None else BASE_TS + timedelta(seconds=t)
    return BusPosition(
        vehicle_id=vehicle_id,
        line_code=line_code,
        line_sign=line_sign,
        direction=direction,
        destination_sign="TERM. LAPA",
        origin_sign="PCA. RAMOS DE AZEVEDO",
        accessible=accessible,
        lat=lat,
        lon=lon,
        event_ts=ts,
        collected_at=collected_at if collected_at is not None else ts + timedelta(seconds=10),
        reference_time="09:00",
        source=source,
    )


@pytest.fixture
def position_factory():
    return make_position


@pytest.fixture
def clean_env(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    """Remove do ambiente todas as variaveis do projeto (padroes de load_settings)."""
    for name in CONFIG_ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    return monkeypatch


@pytest.fixture
def test_settings(clean_env: pytest.MonkeyPatch) -> Settings:
    """Settings com os valores padrao do contrato."""
    return load_settings()


@pytest.fixture
def posicao_payload() -> dict[str, Any]:
    """Resposta de GET /Posicao no formato v2.1, com campos extras e um veiculo malformado."""
    return {
        "hr": "20:28",
        "campo_novo": {"x": 1},
        "l": [
            {
                "c": "8000-10", "cl": 1273, "sl": 1, "lt0": "TERM. LAPA",
                "lt1": "PCA. RAMOS DE AZEVEDO", "qv": 2, "extra_linha": "ignorar",
                "vs": [
                    {"p": 11433, "a": True, "ta": "2017-05-31T20:28:49Z", "py": -23.5402, "px": -46.6402,
                     "is": "campo extra"},
                    {"p": 11434, "a": False, "ta": "2017-05-31T20:28:40Z", "py": 0, "px": 0},
                ],
            },
            {
                "c": "875A-10", "cl": 33887, "sl": 2, "lt0": "METRO BARRA FUNDA",
                "lt1": "PERDIZES", "qv": 2,
                "vs": [
                    {"p": "22001", "a": True, "ta": "2017-05-31T20:28:10Z", "py": -23.53, "px": -46.66},
                    {"p": 22002, "a": True, "ta": "2017-05-31T20:28:11Z", "px": -46.66},  # sem py
                ],
            },
        ],
    }


@pytest.fixture(scope="session")
def sample_path() -> Path:
    assert SAMPLE_PATH.is_file(), f"sample versionado ausente: {SAMPLE_PATH}"
    return SAMPLE_PATH


@pytest.fixture(scope="session")
def sample_collections(sample_path: Path) -> list[tuple[datetime, dict[str, Any]]]:
    """Todas as coletas do sample versionado (collected_at, payload)."""
    from producer.replay import iter_collections

    return list(iter_collections(sample_path))
