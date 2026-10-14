-- 003_views.sql - Views consumidas pelo dashboard (nomes e colunas sao contrato,
-- docs/CONTRACTS.md secao 5). Idempotente (CREATE OR REPLACE VIEW).
--
-- Referencia de "agora": max(event_ts) de bus_positions, nunca now(). No replay acelerado
-- o tempo de evento anda mais rapido que o relogio (DECISIONS D04). As referencias sao
-- subconsultas escalares para que o planejador use os indices de 002_indexes.sql.
--
-- Tabelas vazias: max() retorna NULL, os filtros "coluna >= NULL" nao casam nenhuma linha
-- e as views de lista ficam vazias; vw_city_kpis sempre retorna 1 linha.

SET client_min_messages = warning;

-- Ultima leitura com coordenada valida de cada veiculo visto nos 5 minutos anteriores
-- a referencia. Coordenada zerada (lat = 0 ou lon = 0, dado bruto da API) e ignorada,
-- entao o veiculo aparece na sua ultima posicao valida dentro da janela.
CREATE OR REPLACE VIEW vw_latest_positions AS
SELECT DISTINCT ON (p.vehicle_id)
  p.vehicle_id,
  p.line_code,
  p.line_sign,
  p.direction,
  p.accessible,
  p.lat,
  p.lon,
  p.event_ts
FROM bus_positions p
WHERE p.event_ts >= (SELECT max(event_ts) FROM bus_positions) - interval '5 minutes'
  AND p.lat <> 0
  AND p.lon <> 0
ORDER BY p.vehicle_id, p.event_ts DESC;

COMMENT ON VIEW vw_latest_positions IS
  'Ultima posicao valida por veiculo nos 5 min anteriores a max(event_ts)';

-- Metricas de todas as linhas da janela mais recente presente em line_metrics.
CREATE OR REPLACE VIEW vw_latest_line_metrics AS
SELECT
  m.window_start,
  m.window_end,
  m.line_code,
  m.line_sign,
  m.direction,
  m.avg_speed_kmh,
  m.speed_samples,
  m.active_vehicles,
  m.accessible_vehicles,
  m.accessible_pct,
  m.computed_at
FROM line_metrics m
WHERE m.window_start = (SELECT max(window_start) FROM line_metrics);

COMMENT ON VIEW vw_latest_line_metrics IS
  'Metricas de linha da janela mais recente (max(window_start))';

-- KPIs da cidade: sempre exatamente 1 linha (agregados sem GROUP BY).
-- Com tabelas vazias: active_buses = 0, operating_lines = 0 e os demais NULL.
-- avg_city_speed_kmh: media das medias por linha ponderada por speed_samples
-- (equivale a media de todas as amostras da janela); NULL se nao houver amostras.
CREATE OR REPLACE VIEW vw_city_kpis AS
SELECT
  pos.active_buses,
  pos.operating_lines,
  met.avg_city_speed_kmh,
  ref.last_event_ts,
  met.latest_window_start
FROM
  (SELECT count(*)::integer               AS active_buses,
          count(DISTINCT line_sign)::integer AS operating_lines
     FROM vw_latest_positions) AS pos
CROSS JOIN
  (SELECT sum(avg_speed_kmh * speed_samples) FILTER (WHERE avg_speed_kmh IS NOT NULL)
            / nullif(sum(speed_samples) FILTER (WHERE avg_speed_kmh IS NOT NULL), 0)
            AS avg_city_speed_kmh,
          max(window_start) AS latest_window_start
     FROM vw_latest_line_metrics) AS met
CROSS JOIN
  (SELECT max(event_ts) AS last_event_ts FROM bus_positions) AS ref;

COMMENT ON VIEW vw_city_kpis IS
  'Uma linha com onibus ativos, linhas em operacao, velocidade media ponderada e referencias de tempo';

-- Alertas das ultimas 24 h em relacao ao alerta mais recente, do mais novo ao mais antigo.
CREATE OR REPLACE VIEW vw_recent_alerts AS
SELECT
  a.alert_id,
  a.alert_type,
  a.vehicle_id,
  a.line_code,
  a.line_sign,
  a.direction,
  a.lat,
  a.lon,
  a.stopped_since,
  a.detected_at,
  a.stopped_minutes,
  a.last_speed_kmh,
  a.created_at
FROM bus_alerts a
WHERE a.detected_at >= (SELECT max(detected_at) FROM bus_alerts) - interval '24 hours'
ORDER BY a.detected_at DESC, a.alert_id;

COMMENT ON VIEW vw_recent_alerts IS
  'Alertas das ultimas 24 h relativas a max(detected_at), ordenados por detected_at desc';
