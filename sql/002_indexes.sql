-- 002_indexes.sql - Indices para as views do dashboard (003_views.sql). Idempotente.
--
-- Indices que ja existem pelas constraints e NAO sao duplicados aqui:
--   bus_positions_vehicle_event_uq (vehicle_id, event_ts): serve a busca "ultima leitura
--     do veiculo X" (varredura invertida do btree equivale a (vehicle_id, event_ts DESC))
--     e o ON CONFLICT da persistencia. Um indice (vehicle_id, event_ts DESC) separado seria
--     redundante e so encareceria cada INSERT.
--   line_metrics_pkey (line_code, direction, window_start): upsert do consumer de metricas
--     e historico de uma linha/sentido.
--   bus_alerts_pkey (alert_id): ON CONFLICT do consumer de alertas.

SET client_min_messages = warning;

-- max(event_ts) (referencia de "agora" em todas as views) vira uma leitura de uma folha do
-- indice, e o filtro "event_ts >= referencia - 5 min" de vw_latest_positions le apenas a
-- faixa recente em vez da tabela inteira (que cresce sem limite).
CREATE INDEX IF NOT EXISTS bus_positions_event_ts_idx
  ON bus_positions (event_ts DESC);

-- max(window_start) e o filtro "window_start = max" de vw_latest_line_metrics; a PK
-- comeca por line_code e nao ajuda nessa consulta. Tambem atende graficos por periodo.
CREATE INDEX IF NOT EXISTS line_metrics_window_start_idx
  ON line_metrics (window_start DESC);

-- max(detected_at), filtro das ultimas 24 h e ORDER BY detected_at DESC de vw_recent_alerts.
CREATE INDEX IF NOT EXISTS bus_alerts_detected_at_idx
  ON bus_alerts (detected_at DESC);
