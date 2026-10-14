"""Ponto de entrada do producer: ``python -m producer.main``.

Escolhe o modo por ``MODE`` (live ou replay), espera o Kafka e roda o loop ate receber
SIGTERM/SIGINT. Codigos de saida: 0 = encerramento normal, 1 = infraestrutura
indisponivel ou erro fatal, 2 = configuracao invalida.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from common.config import ConfigError, Settings, load_settings
from common.kafka_utils import make_producer, wait_for_kafka
from common.log import get_logger, setup_logging
from common.runtime import ShutdownFlag

log = get_logger("producer.main")

FINAL_FLUSH_TIMEOUT_S = 15.0


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="producer.main", description="Producer de posicoes SPTrans")
    parser.add_argument(
        "--kafka-wait-timeout",
        type=float,
        default=120.0,
        help="segundos aguardando o broker antes de desistir (padrao 120)",
    )
    return parser.parse_args(argv)


def check_settings(settings: Settings) -> str | None:
    """Retorna uma mensagem de erro de configuracao do modo escolhido, ou None."""
    if settings.mode == "live" and not settings.sptrans_token:
        return "MODE=live exige SPTRANS_TOKEN (defina no .env ou use MODE=replay)"
    if settings.mode == "replay" and not Path(settings.replay_file).is_file():
        return f"REPLAY_FILE nao encontrado: {settings.replay_file}"
    return None


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        settings = load_settings()
    except ConfigError as exc:
        setup_logging("producer", "INFO")
        log.error("configuracao invalida", error=str(exc))
        return 2
    setup_logging("producer", settings.log_level)

    problem = check_settings(settings)
    if problem:
        log.error("configuracao invalida", error=problem, mode=settings.mode)
        return 2
    log.info("producer iniciando", mode=settings.mode, bootstrap=settings.kafka_bootstrap_servers,
             topic=settings.topic_raw)

    try:
        wait_for_kafka(settings, timeout_s=args.kafka_wait_timeout)
    except TimeoutError:
        log.error("kafka indisponivel, encerrando", bootstrap=settings.kafka_bootstrap_servers,
                  waited_s=args.kafka_wait_timeout)
        return 1

    # Os handlers de sinal sao instalados so depois da espera pelo Kafka: durante a espera
    # um SIGTERM encerra o processo imediatamente (wait_for_kafka usa time.sleep).
    shutdown = ShutdownFlag()
    producer = make_producer(settings, client_id=f"producer-{settings.mode}")
    try:
        if settings.mode == "live":
            from producer.live import run_live

            run_live(settings, producer, shutdown)
        else:
            from producer.replay import run_replay

            run_replay(settings, producer, shutdown)
    except Exception as exc:  # noqa: BLE001 - erro fatal: loga e sai com codigo 1
        log.exception("erro fatal no producer", error_type=type(exc).__name__)
        return 1
    finally:
        remaining = producer.flush(FINAL_FLUSH_TIMEOUT_S)
        if remaining:
            log.warning("mensagens nao entregues no encerramento", pending=remaining)
    log.info("producer encerrado")
    return 0


if __name__ == "__main__":
    sys.exit(main())
