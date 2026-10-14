"""common.config.load_settings: padroes do contrato (secao 6) e validacoes."""

from __future__ import annotations

import dataclasses

import pytest

from common.config import ConfigError, Settings, load_settings


def test_defaults_match_contract(test_settings: Settings):
    s = test_settings
    assert s.mode == "replay" and s.log_level == "INFO"
    assert s.kafka_bootstrap_servers == "localhost:9092"
    assert (s.topic_raw, s.topic_metrics, s.topic_alerts) == ("bus-positions-raw", "bus-metrics", "bus-alerts")
    assert s.sptrans_token == "" and s.sptrans_base_url == "https://api.olhovivo.sptrans.com.br/v2.1"
    assert (s.poll_interval_seconds, s.http_timeout_seconds, s.http_max_retries) == (30, 20.0, 5)
    assert s.replay_file == "data/samples/sample_synthetic.jsonl.gz"
    assert (s.replay_speed, s.replay_loop, s.replay_rebase_timestamps) == (5.0, True, True)
    assert (s.record_output_dir, s.record_collections) == ("data/samples", 20)
    assert (s.postgres_host, s.postgres_port, s.postgres_db, s.postgres_user, s.postgres_password) == (
        "localhost", 5432, "sptrans", "sptrans", "sptrans")
    assert (s.persistence_batch_size, s.persistence_flush_seconds) == (500, 2.0)
    assert (s.window_minutes, s.window_lateness_seconds, s.idle_flush_seconds) == (5, 60, 120.0)
    assert (s.max_speed_kmh, s.min_interval_seconds, s.max_interval_seconds) == (100.0, 1.0, 600.0)
    assert (s.alert_speed_kmh, s.alert_min_minutes, s.terminal_radius_m) == (2.0, 10.0, 400.0)
    assert s.dashboard_refresh_seconds == 30


def test_overrides_and_normalization(clean_env):
    clean_env.setenv("MODE", " LIVE ")
    clean_env.setenv("LOG_LEVEL", "debug")
    clean_env.setenv("SPTRANS_BASE_URL", "http://mock:8000/v2.1/")
    clean_env.setenv("REPLAY_SPEED", "2.5")
    clean_env.setenv("WINDOW_MINUTES", "15")
    clean_env.setenv("POSTGRES_PORT", "6543")
    s = load_settings()
    assert s.mode == "live" and s.log_level == "DEBUG"
    assert s.sptrans_base_url == "http://mock:8000/v2.1"
    assert s.replay_speed == 2.5 and s.window_minutes == 15 and s.postgres_port == 6543


def test_blank_value_uses_default(clean_env):
    clean_env.setenv("POLL_INTERVAL_SECONDS", "   ")
    clean_env.setenv("TOPIC_RAW", "")
    s = load_settings()
    assert s.poll_interval_seconds == 30 and s.topic_raw == "bus-positions-raw"


@pytest.mark.parametrize("raw, expected", [("1", True), ("yes", True), ("ON", True), ("y", True),
                                           ("0", False), ("false", False), ("No", False), ("off", False)])
def test_bool_parsing(clean_env, raw, expected):
    clean_env.setenv("REPLAY_LOOP", raw)
    assert load_settings().replay_loop is expected


@pytest.mark.parametrize("name, value", [
    ("POLL_INTERVAL_SECONDS", "trinta"), ("HTTP_TIMEOUT_SECONDS", "x"), ("REPLAY_LOOP", "talvez"),
    ("POSTGRES_PORT", "5432.5"), ("MODE", "batch"), ("POLL_INTERVAL_SECONDS", "0"), ("REPLAY_SPEED", "0"),
    ("REPLAY_SPEED", "-1"), ("WINDOW_MINUTES", "7"), ("WINDOW_MINUTES", "0"), ("PERSISTENCE_BATCH_SIZE", "0"),
    ("MIN_INTERVAL_SECONDS", "0"), ("MAX_INTERVAL_SECONDS", "1"),
])
def test_invalid_values_raise_config_error(clean_env, name, value):
    clean_env.setenv(name, value)
    with pytest.raises(ConfigError) as exc:
        load_settings()
    assert name.split("_")[0] in str(exc.value) or "Intervalos" in str(exc.value)


def test_config_error_is_value_error():
    assert issubclass(ConfigError, ValueError)


@pytest.mark.parametrize("minutes", ["1", "2", "3", "5", "10", "15", "30", "60"])
def test_window_minutes_divisors_of_60_are_accepted(clean_env, minutes):
    clean_env.setenv("WINDOW_MINUTES", minutes)
    assert load_settings().window_minutes == int(minutes)


def test_redacted_hides_secrets(clean_env):
    clean_env.setenv("SPTRANS_TOKEN", "tok-secreto")
    clean_env.setenv("POSTGRES_PASSWORD", "pg-secreta")
    s = load_settings()
    red = s.redacted()
    assert red["sptrans_token"] == "***" and red["postgres_password"] == "***"
    assert "tok-secreto" not in str(red) and "pg-secreta" not in str(red)
    assert red["mode"] == "replay"


def test_redacted_without_token_is_empty(test_settings):
    assert test_settings.redacted()["sptrans_token"] == ""


def test_postgres_dsn(clean_env):
    clean_env.setenv("POSTGRES_HOST", "postgres")
    s = load_settings()
    assert s.postgres_dsn == "host=postgres port=5432 dbname=sptrans user=sptrans password=sptrans"


def test_settings_is_immutable(test_settings):
    with pytest.raises(dataclasses.FrozenInstanceError):
        test_settings.mode = "live"  # type: ignore[misc]
