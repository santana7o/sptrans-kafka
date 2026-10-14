# Contratos

Fonte da verdade para formatos de eventos, tabelas, variaveis de ambiente e pontos de
entrada. A implementacao de referencia dos eventos esta em `common/schemas.py`.
Mudancas aqui exigem registro em `docs/DECISIONS.md`.

## 1. Fonte: API Olho Vivo v2.1

- Base: `https://api.olhovivo.sptrans.com.br/v2.1`
- Autenticacao: `POST /Login/Autenticar?token={SPTRANS_TOKEN}` retorna `true`/`false` no
  corpo e um cookie de sessao (`apiCredentials`). O cookie deve ser reutilizado em todas
  as chamadas seguintes (use `requests.Session`). Sessao expirada: a API responde HTTP 401
  (ou corpo de erro "Authorization has been denied"); o producer reautentica e repete.
- Posicoes: `GET /Posicao`, atualizado pela SPTrans a cada ~30 s.

```json
{
  "hr": "20:28",
  "l": [
    {
      "c": "8000-10", "cl": 1273, "sl": 1, "lt0": "TERM. LAPA", "lt1": "PCA. RAMOS DE AZEVEDO", "qv": 2,
      "vs": [
        {"p": 11433, "a": true, "ta": "2017-05-31T20:28:49Z", "py": -23.5402, "px": -46.6402}
      ]
    }
  ]
}
```

| Campo | Tipo | Significado |
|---|---|---|
| hr | string | horario de referencia da geracao (hora local, "HH:MM") |
| l[].c | string | letreiro completo |
| l[].cl | int | codigo identificador da linha (unico por linha + sentido) |
| l[].sl | int | sentido: 1 = Terminal Principal -> Secundario; 2 = Secundario -> Principal |
| l[].lt0 | string | letreiro de destino |
| l[].lt1 | string | letreiro de origem |
| l[].qv | int | quantidade de veiculos localizados |
| vs[].p | int | prefixo do veiculo |
| vs[].a | bool | veiculo acessivel |
| vs[].ta | string | horario UTC da captura (ISO 8601) |
| vs[].py / vs[].px | double | latitude / longitude |

Campos extras na resposta devem ser ignorados sem erro.

## 2. Arquivos de sample (gravacao e replay)

- Local: `data/samples/*.jsonl.gz` (gzip, JSON Lines).
- Uma linha por coleta: `{"collected_at": "<ISO UTC Z>", "payload": <resposta crua de /Posicao>}`.
- Linhas em ordem cronologica de `collected_at`.
- Sample versionado: `data/samples/sample_synthetic.jsonl.gz` (sintetico, ver DECISIONS).

## 3. Topicos Kafka

| Topico | Chave | Valor | Particoes | Produtor | Consumidores |
|---|---|---|---|---|---|
| bus-positions-raw | vehicle_id (UTF-8) | BusPosition JSON | 3 | producer | persistence, metrics, alerts |
| bus-metrics | "{line_code}-{direction}" | LineMetric JSON | 1 | metrics | (Kafka UI / consumidores futuros) |
| bus-alerts | vehicle_id | BusAlert JSON | 1 | alerts | (Kafka UI / consumidores futuros) |

Consumer groups: `persistence-consumer`, `metrics-consumer`, `alerts-consumer`.
Entrega at-least-once: em falha entre publicar e commitar offsets, `bus-metrics` e
`bus-alerts` podem conter mensagens repetidas. Consumidores desses topicos devem
deduplicar pela chave + `window_start` (metricas) ou por `alert_id` (alertas). As
tabelas do Postgres ja sao idempotentes. O consumer de metricas roda com uma unica
instancia (D13).
Todos os valores sao JSON UTF-8 compacto; datas ISO 8601 UTC com sufixo `Z`.

### 3.1 BusPosition (bus-positions-raw)

```json
{"vehicle_id":"11433","line_code":1273,"line_sign":"8000-10","direction":1,
 "destination_sign":"TERM. LAPA","origin_sign":"PCA. RAMOS DE AZEVEDO","accessible":true,
 "lat":-23.5402,"lon":-46.6402,"event_ts":"2017-05-31T20:28:49Z",
 "collected_at":"2017-05-31T20:29:02Z","reference_time":"20:28","source":"live","schema_version":1}
```

Gerado por `common.schemas.flatten_posicao`. O producer descarta duplicatas antes de
publicar: leitura com `event_ts` igual ou anterior ao ultimo publicado para o mesmo
`vehicle_id` (repetida ou fora de ordem). Coordenadas zeradas sao
publicadas (dado bruto) e filtradas no processamento.

### 3.2 LineMetric (bus-metrics)

```json
{"window_start":"2026-10-14T12:00:00Z","window_end":"2026-10-14T12:05:00Z","line_code":1273,
 "line_sign":"8000-10","direction":1,"avg_speed_kmh":17.4,"speed_samples":42,
 "active_vehicles":9,"accessible_vehicles":8,"accessible_pct":88.89,
 "computed_at":"2026-10-14T12:06:01Z","schema_version":1}
```

### 3.3 BusAlert (bus-alerts)

```json
{"alert_id":"11433-20261014T120130Z","alert_type":"STOPPED_VEHICLE","vehicle_id":"11433",
 "line_code":1273,"line_sign":"8000-10","direction":1,"lat":-23.5600,"lon":-46.6600,
 "stopped_since":"2026-10-14T12:01:30Z","detected_at":"2026-10-14T12:11:45Z",
 "stopped_minutes":10.25,"last_speed_kmh":0.4,"schema_version":1}
```

`alert_id` e deterministico (`BusAlert.make_id`) para que reprocessamento nao duplique.

## 4. Regras de processamento

Todas usam tempo de evento (`event_ts`). Parametros vem de `Settings`.

### 4.1 Velocidade (`common.tracker.SpeedTracker` + `common.geo.compute_speed_kmh`)
- Estado: ultima leitura valida por veiculo (`vehicle_id`).
- Velocidade = haversine(anterior, atual) / (event_ts atual - event_ts anterior) * 3.6.
- Leitura invalida (descartada da media e nao atualiza o estado "ultimo ponto"):
  coordenada zerada ou fora da Grande SP. Para os demais descartes (intervalo menor que
  `MIN_INTERVAL_SECONDS`, incluindo zero e negativo; maior que `MAX_INTERVAL_SECONDS`;
  velocidade acima de `MAX_SPEED_KMH`): a velocidade e descartada. Se o intervalo for
  negativo ou zero (leitura antiga ou repetida) o estado nao e atualizado; nos demais
  casos o estado passa a ser a leitura atual.
- Troca de linha/sentido do veiculo: o estado continua valendo (o veiculo e o mesmo).

### 4.2 Janelas de metricas
- Janelas fixas (tumbling) de `WINDOW_MINUTES` (5) alinhadas ao relogio UTC
  (`common.timeutils.floor_to_window`), por (`line_code`, `direction`).
- A leitura entra na janela de seu `event_ts`. Para cada janela:
  - `active_vehicles`: veiculos distintos com pelo menos uma leitura de coordenada valida;
  - `accessible_vehicles` / `accessible_pct`: entre os ativos;
  - `avg_speed_kmh`: media aritmetica das velocidades validas cuja leitura atual cai na
    janela; `None` se nao houver; `speed_samples` = quantidade.
- Fechamento por watermark: `watermark = max(event_ts visto) - WINDOW_LATENESS_SECONDS`.
  Janela com `window_end <= watermark` e emitida uma unica vez (bus-metrics + upsert
  em `line_metrics`) e descartada da memoria. Leitura que chega para janela ja fechada
  e descartada (contada em log).
- Sem mensagens por `IDLE_FLUSH_SECONDS`: emite todas as janelas abertas (fim do replay).
- Leitura com `event_ts` mais de 5 min a frente de `collected_at` e ignorada nas janelas (D11).
- Dentro de um lote as leituras sao processadas em ordem de `event_ts`.

### 4.3 Alerta de onibus parado
- Leitura "lenta": velocidade valida < `ALERT_SPEED_KMH` (2 km/h) e veiculo fora de
  qualquer terminal (`common.terminals.is_near_terminal` com `TERMINAL_RADIUS_M`).
- Primeira leitura lenta inicia `stopped_since` (= event_ts da leitura ANTERIOR, isto e,
  o inicio do intervalo em que o veiculo ficou parado).
- Leitura com velocidade valida >= limite, ou dentro de terminal, zera a sequencia.
  Leituras com velocidade descartada nao alteram a sequencia.
- Quando `event_ts - stopped_since >= ALERT_MIN_MINUTES` (10) e a sequencia ainda nao
  alertou: emite um BusAlert. Um alerta por sequencia; nova sequencia pode alertar de novo.

## 5. PostgreSQL

Banco `sptrans`. Schema em `sql/` (aplicado pelo entrypoint do container do Postgres em
ordem alfabetica). Todos os timestamps `timestamptz` em UTC.

```sql
CREATE TABLE bus_positions (
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
  UNIQUE (vehicle_id, event_ts)              -- idempotencia (INSERT ... ON CONFLICT DO NOTHING)
);

CREATE TABLE line_metrics (
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
  PRIMARY KEY (line_code, direction, window_start)   -- upsert
);

CREATE TABLE bus_alerts (
  alert_id        text PRIMARY KEY,                   -- INSERT ... ON CONFLICT DO NOTHING
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
```

Quem grava: `bus_positions` = consumer de persistencia; `line_metrics` = consumer de
metricas; `bus_alerts` = consumer de alertas.

### Views para o dashboard (nomes e colunas sao contrato)

Referencia de "agora" = `max(event_ts)` em `bus_positions` (funciona em live e replay).

| View | Colunas | Regra |
|---|---|---|
| vw_latest_positions | vehicle_id, line_code, line_sign, direction, accessible, lat, lon, event_ts | ultima leitura de cada veiculo com event_ts >= referencia - 5 min, coordenada nao zerada |
| vw_latest_line_metrics | todas de line_metrics | linhas da janela mais recente presente em line_metrics (max(window_start)) |
| vw_city_kpis | active_buses, operating_lines, avg_city_speed_kmh, last_event_ts, latest_window_start | 1 linha. active_buses = count(vw_latest_positions); operating_lines = linhas distintas (line_sign) em vw_latest_positions; avg_city_speed = media ponderada por speed_samples de vw_latest_line_metrics |
| vw_recent_alerts | todas de bus_alerts | ordenado por detected_at desc, ultimas 24 h em relacao a max(detected_at) |

## 6. Variaveis de ambiente

| Variavel | Padrao | Uso |
|---|---|---|
| MODE | replay | `live` (API) ou `replay` (samples) |
| LOG_LEVEL | INFO | nivel de log |
| KAFKA_BOOTSTRAP_SERVERS | localhost:9092 | no compose: `kafka:19092` (listener interno) |
| TOPIC_RAW / TOPIC_METRICS / TOPIC_ALERTS | bus-positions-raw / bus-metrics / bus-alerts | nomes dos topicos |
| SPTRANS_TOKEN | (vazio) | obrigatorio em live e para `make record` |
| SPTRANS_BASE_URL | https://api.olhovivo.sptrans.com.br/v2.1 | base da API |
| POLL_INTERVAL_SECONDS | 30 | intervalo entre coletas em live |
| HTTP_TIMEOUT_SECONDS | 20 | timeout de cada requisicao |
| HTTP_MAX_RETRIES | 5 | tentativas com backoff exponencial |
| REPLAY_FILE | data/samples/sample_synthetic.jsonl.gz | arquivo reproduzido |
| REPLAY_SPEED | 5 | fator de aceleracao (1 = tempo real) |
| REPLAY_LOOP | true | recomeca o arquivo ao terminar |
| REPLAY_REBASE_TIMESTAMPS | true | desloca timestamps para "agora" e soma a duracao a cada volta |
| RECORD_OUTPUT_DIR | data/samples | destino do gravador |
| RECORD_COLLECTIONS | 20 | quantas coletas gravar |
| POSTGRES_HOST / PORT / DB / USER / PASSWORD | localhost / 5432 / sptrans / sptrans / sptrans | conexao |
| PERSISTENCE_BATCH_SIZE | 500 | tamanho do lote |
| PERSISTENCE_FLUSH_SECONDS | 2 | flush do lote por tempo |
| WINDOW_MINUTES | 5 | tamanho da janela |
| WINDOW_LATENESS_SECONDS | 60 | atraso tolerado antes de fechar janela |
| IDLE_FLUSH_SECONDS | 120 | fecha janelas abertas apos ociosidade |
| MAX_SPEED_KMH | 100 | acima disso a velocidade e descartada |
| MIN_INTERVAL_SECONDS / MAX_INTERVAL_SECONDS | 1 / 600 | intervalo valido entre leituras |
| ALERT_SPEED_KMH | 2 | limite de "parado" |
| ALERT_MIN_MINUTES | 10 | tempo parado para alertar |
| TERMINAL_RADIUS_M | 400 | raio dos terminais |
| DASHBOARD_REFRESH_SECONDS | 30 | atualizacao do dashboard |
| POSTGRES_HOST_PORT | 5432 | somente docker compose: porta publicada no host |

## 7. Pontos de entrada e servicos do compose

| Servico compose | Comando |
|---|---|
| kafka | imagem apache/kafka (KRaft) |
| kafka-init | cria os 3 topicos (idempotente) |
| kafka-ui | provectuslabs/kafka-ui, porta 8080 |
| postgres | postgres:16.4-alpine, porta 5432, monta `sql/` em `/docker-entrypoint-initdb.d` |
| producer | `python -m producer.main` (le MODE) |
| persistence | `python -m consumers.persistence.main` |
| metrics | `python -m consumers.metrics.main` |
| alerts | `python -m consumers.alerts.main` |
| dashboard | `streamlit run dashboard/app.py --server.port 8501 --server.address 0.0.0.0` |
| recorder (profile `tools`) | `python -m producer.record` |
| tests (profile `tools`) | `pytest -q` |

Todos os servicos Python usam a mesma imagem (`docker/python.Dockerfile`), com o
repositorio copiado em `/app` e `PYTHONPATH=/app`.
