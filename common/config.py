"""Configuracao centralizada, lida exclusivamente de variaveis de ambiente.

Nenhum outro modulo deve ler ``os.environ`` diretamente. Os nomes e valores padrao
estao documentados em ``docs/CONTRACTS.md`` e ``.env.example``.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

VALID_MODES = ("live", "replay")


class ConfigError(ValueError):
    """Valor de configuracao invalido."""


def _str(name: str, default: str) -> str:
    value = os.environ.get(name)
    return default if value is None or value.strip() == "" else value.strip()


def _int(name: str, default: int) -> int:
    raw = _str(name, str(default))
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} deve ser inteiro, recebido {raw!r}") from exc


def _float(name: str, default: float) -> float:
    raw = _str(name, str(default))
    try:
        return float(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} deve ser numero, recebido {raw!r}") from exc


def _bool(name: str, default: bool) -> bool:
    raw = _str(name, "true" if default else "false").lower()
    if raw in ("1", "true", "yes", "y", "on"):
        return True
    if raw in ("0", "false", "no", "n", "off"):
        return False
    raise ConfigError(f"{name} deve ser booleano, recebido {raw!r}")


@dataclass(frozen=True)
class Settings:
    # Geral
    mode: str
    log_level: str

    # Kafka
    kafka_bootstrap_servers: str
    topic_raw: str
    topic_metrics: str
    topic_alerts: str

    # API Olho Vivo (modo live)
    sptrans_base_url: str
    sptrans_token: str = field(repr=False)
    poll_interval_seconds: int
    http_timeout_seconds: float
    http_max_retries: int

    # Replay e gravacao de samples
    replay_file: str
    replay_speed: float
    replay_loop: bool
    replay_rebase_timestamps: bool
    record_output_dir: str
    record_collections: int

    # PostgreSQL
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str = field(repr=False)

    # Consumer de persistencia
    persistence_batch_size: int
    persistence_flush_seconds: float

    # Regras de processamento
    window_minutes: int
    window_lateness_seconds: int
    idle_flush_seconds: float
    max_speed_kmh: float
    min_interval_seconds: float
    max_interval_seconds: float
    alert_speed_kmh: float
    alert_min_minutes: float
    terminal_radius_m: float

    # Dashboard
    dashboard_refresh_seconds: int

    @property
    def postgres_dsn(self) -> str:
        return (
            f"host={self.postgres_host} port={self.postgres_port} dbname={self.postgres_db} "
            f"user={self.postgres_user} password={self.postgres_password}"
        )

    def redacted(self) -> dict:
        """Representacao segura para log (sem token e sem senha)."""
        data = dict(self.__dict__)
        data["sptrans_token"] = "***" if self.sptrans_token else ""
        data["postgres_password"] = "***"
        return data


def load_settings() -> Settings:
    """Le e valida todas as variaveis de ambiente do projeto."""
    settings = Settings(
        mode=_str("MODE", "replay").lower(),
        log_level=_str("LOG_LEVEL", "INFO").upper(),
        kafka_bootstrap_servers=_str("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        topic_raw=_str("TOPIC_RAW", "bus-positions-raw"),
        topic_metrics=_str("TOPIC_METRICS", "bus-metrics"),
        topic_alerts=_str("TOPIC_ALERTS", "bus-alerts"),
        sptrans_base_url=_str("SPTRANS_BASE_URL", "https://api.olhovivo.sptrans.com.br/v2.1").rstrip("/"),
        sptrans_token=_str("SPTRANS_TOKEN", ""),
        poll_interval_seconds=_int("POLL_INTERVAL_SECONDS", 30),
        http_timeout_seconds=_float("HTTP_TIMEOUT_SECONDS", 20.0),
        http_max_retries=_int("HTTP_MAX_RETRIES", 5),
        replay_file=_str("REPLAY_FILE", "data/samples/sample_synthetic.jsonl.gz"),
        replay_speed=_float("REPLAY_SPEED", 5.0),
        replay_loop=_bool("REPLAY_LOOP", True),
        replay_rebase_timestamps=_bool("REPLAY_REBASE_TIMESTAMPS", True),
        record_output_dir=_str("RECORD_OUTPUT_DIR", "data/samples"),
        record_collections=_int("RECORD_COLLECTIONS", 20),
        postgres_host=_str("POSTGRES_HOST", "localhost"),
        postgres_port=_int("POSTGRES_PORT", 5432),
        postgres_db=_str("POSTGRES_DB", "sptrans"),
        postgres_user=_str("POSTGRES_USER", "sptrans"),
        postgres_password=_str("POSTGRES_PASSWORD", "sptrans"),
        persistence_batch_size=_int("PERSISTENCE_BATCH_SIZE", 500),
        persistence_flush_seconds=_float("PERSISTENCE_FLUSH_SECONDS", 2.0),
        window_minutes=_int("WINDOW_MINUTES", 5),
        window_lateness_seconds=_int("WINDOW_LATENESS_SECONDS", 60),
        idle_flush_seconds=_float("IDLE_FLUSH_SECONDS", 120.0),
        max_speed_kmh=_float("MAX_SPEED_KMH", 100.0),
        min_interval_seconds=_float("MIN_INTERVAL_SECONDS", 1.0),
        max_interval_seconds=_float("MAX_INTERVAL_SECONDS", 600.0),
        alert_speed_kmh=_float("ALERT_SPEED_KMH", 2.0),
        alert_min_minutes=_float("ALERT_MIN_MINUTES", 10.0),
        terminal_radius_m=_float("TERMINAL_RADIUS_M", 400.0),
        dashboard_refresh_seconds=_int("DASHBOARD_REFRESH_SECONDS", 30),
    )
    _validate(settings)
    return settings


def _validate(s: Settings) -> None:
    if s.mode not in VALID_MODES:
        raise ConfigError(f"MODE deve ser um de {VALID_MODES}, recebido {s.mode!r}")
    if s.poll_interval_seconds <= 0:
        raise ConfigError("POLL_INTERVAL_SECONDS deve ser positivo")
    if s.replay_speed <= 0:
        raise ConfigError("REPLAY_SPEED deve ser positivo")
    if s.window_minutes <= 0 or 60 % s.window_minutes != 0:
        raise ConfigError("WINDOW_MINUTES deve ser divisor de 60 (1, 2, 3, 5, 10, 15, 30, 60)")
    if s.persistence_batch_size <= 0:
        raise ConfigError("PERSISTENCE_BATCH_SIZE deve ser positivo")
    if s.min_interval_seconds <= 0 or s.max_interval_seconds <= s.min_interval_seconds:
        raise ConfigError("Intervalos invalidos: exige 0 < MIN_INTERVAL_SECONDS < MAX_INTERVAL_SECONDS")
