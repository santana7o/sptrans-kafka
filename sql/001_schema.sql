-- 001_schema.sql - Tabelas do pipeline (contrato: docs/CONTRACTS.md secao 5).
--
-- Aplicado pelo entrypoint do container postgres:16 (/docker-entrypoint-initdb.d, ordem
-- alfabetica) no banco POSTGRES_DB. Idempotente: pode ser reaplicado sem erro.
-- Todos os timestamps sao timestamptz em UTC.

SET client_min_messages = warning;

-- Posicoes brutas de GPS. Escrita: consumer de persistencia.
CREATE TABLE IF NOT EXISTS bus_positions (
  id               bigserial PRIMARY KEY,
  vehicle_id       text        NOT NULL,
  line_code        integer     NOT NULL,
  line_sign        text        NOT NULL,
  direction        smallint    NOT NULL,
  destination_sign text,
  origin_sign      text,
  accessible       boolean     NOT NULL,
  lat              double precision NOT NULL,
  lon              double precision NOT NULL,
  event_ts         timestamptz NOT NULL,
  collected_at     timestamptz NOT NULL,
  source           text        NOT NULL,
  ingested_at      timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT bus_positions_vehicle_event_uq UNIQUE (vehicle_id, event_ts)
);

COMMENT ON TABLE bus_positions IS
  'Leituras de GPS (topico bus-positions-raw). Idempotente via UNIQUE (vehicle_id, event_ts) + ON CONFLICT DO NOTHING.';
COMMENT ON COLUMN bus_positions.vehicle_id IS 'Prefixo do veiculo (vs[].p), como texto';
COMMENT ON COLUMN bus_positions.line_code IS 'Codigo identificador da linha (l[].cl), unico por linha + sentido';
COMMENT ON COLUMN bus_positions.line_sign IS 'Letreiro completo (l[].c), ex. 8000-10';
COMMENT ON COLUMN bus_positions.direction IS '1 = principal -> secundario; 2 = secundario -> principal';
COMMENT ON COLUMN bus_positions.event_ts IS 'Horario UTC da captura do GPS (vs[].ta): tempo de evento';
COMMENT ON COLUMN bus_positions.collected_at IS 'Momento em que o producer obteve a resposta da API';
COMMENT ON COLUMN bus_positions.source IS 'live ou replay';
COMMENT ON COLUMN bus_positions.ingested_at IS 'Momento da gravacao no banco (relogio do servidor)';

-- Metricas por linha + sentido em janelas fixas. Escrita: consumer de metricas (upsert).
CREATE TABLE IF NOT EXISTS line_metrics (
  window_start        timestamptz NOT NULL,
  window_end          timestamptz NOT NULL,
  line_code           integer     NOT NULL,
  line_sign           text        NOT NULL,
  direction           smallint    NOT NULL,
  avg_speed_kmh       double precision,
  speed_samples       integer     NOT NULL,
  active_vehicles     integer     NOT NULL,
  accessible_vehicles integer     NOT NULL,
  accessible_pct      double precision NOT NULL,
  computed_at         timestamptz NOT NULL,
  PRIMARY KEY (line_code, direction, window_start)
);

COMMENT ON TABLE line_metrics IS
  'Metricas por (line_code, direction) em janelas tumbling (topico bus-metrics). Upsert pela PK.';
COMMENT ON COLUMN line_metrics.avg_speed_kmh IS 'Media das velocidades validas da janela; NULL se nao houve amostra';
COMMENT ON COLUMN line_metrics.speed_samples IS 'Quantidade de velocidades validas usadas na media';
COMMENT ON COLUMN line_metrics.accessible_pct IS '100 * accessible_vehicles / active_vehicles';

-- Alertas de veiculo parado. Escrita: consumer de alertas.
CREATE TABLE IF NOT EXISTS bus_alerts (
  alert_id        text PRIMARY KEY,
  alert_type      text        NOT NULL,
  vehicle_id      text        NOT NULL,
  line_code       integer     NOT NULL,
  line_sign       text        NOT NULL,
  direction       smallint    NOT NULL,
  lat             double precision NOT NULL,
  lon             double precision NOT NULL,
  stopped_since   timestamptz NOT NULL,
  detected_at     timestamptz NOT NULL,
  stopped_minutes double precision NOT NULL,
  last_speed_kmh  double precision NOT NULL,
  created_at      timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE bus_alerts IS
  'Alertas (topico bus-alerts). alert_id deterministico + ON CONFLICT DO NOTHING evita duplicatas.';
COMMENT ON COLUMN bus_alerts.stopped_since IS 'Inicio da sequencia de leituras lentas (tempo de evento)';
COMMENT ON COLUMN bus_alerts.detected_at IS 'event_ts da leitura que disparou o alerta';
