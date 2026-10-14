#!/bin/sh
# Cria os topicos de docs/CONTRACTS.md (secao 3). Idempotente (--if-not-exists).
# Executado pelo servico kafka-init do docker-compose.yml.
set -eu

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka:19092}"
KAFKA_TOPICS=/opt/kafka/bin/kafka-topics.sh

create_topic() {
    topic="$1"
    partitions="$2"
    echo "kafka-init: garantindo topico ${topic} (${partitions} particoes)"
    "$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
        --topic "$topic" --partitions "$partitions" --replication-factor 1
}

create_topic "${TOPIC_RAW:-bus-positions-raw}" 3
create_topic "${TOPIC_METRICS:-bus-metrics}" 1
create_topic "${TOPIC_ALERTS:-bus-alerts}" 1

echo "kafka-init: topicos existentes:"
"$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP" --list
