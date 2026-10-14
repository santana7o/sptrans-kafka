# Registro de decisoes tecnicas

Formato: data, decisao, motivo, alternativas descartadas.

## D01 - 2026-10-14 - Tempo de evento em todo o processamento
- Decisao: velocidade, janelas e alertas usam `event_ts` (campo `ta` da API), nunca o relogio local.
- Motivo: resultados deterministicos, replay acelerado possivel, robustez a atraso de entrega.
- Descartado: tempo de processamento (resultados mudariam com a velocidade do replay e com lag do consumer).

## D02 - 2026-10-14 - Prefixo do veiculo como string
- Decisao: `vs[].p` e `int` na documentacao oficial, mas no evento vira `vehicle_id: str`.
- Motivo: e um identificador, nao um numero; tambem e a chave Kafka (bytes UTF-8).
- Descartado: manter int (exigiria conversoes na chave e no banco).

## D03 - 2026-10-14 - Sample versionado sintetico
- Decisao: o sample versionado (`data/samples/sample_synthetic.jsonl.gz`) e gerado por
  `producer/synthetic.py` no formato exato da resposta de `/Posicao`, com onibus em
  trajetos entre terminais reais, paradas fora de terminal, leituras invalidas e duplicatas.
- Motivo: durante a construcao nao havia token da SPTrans disponivel; alem disso um sample
  sintetico cobre de proposito os casos de borda (alerta, coordenada zerada, duplicata).
  O gravador (`make record`) produz arquivos reais no mesmo formato, que podem substituir
  o sintetico via `REPLAY_FILE`.
- Descartado: versionar uma gravacao real (indisponivel no momento; e o tamanho seria grande).

## D04 - 2026-10-14 - Replay com rebase de timestamps
- Decisao: no replay, todos os timestamps (`ta` e `collected_at`) sao deslocados para que a
  primeira coleta corresponda ao momento de inicio; a cada volta do loop soma-se a duracao do
  arquivo. `REPLAY_SPEED` acelera a reproducao (padrao 5x).
- Motivo: o dashboard mostra dados "recentes", o loop nao gera duplicatas e as velocidades
  continuam corretas (intervalos de tempo de evento preservados).
- Consequencia: com REPLAY_SPEED > 1 o tempo de evento anda mais rapido que o relogio. Por
  isso as views usam `max(event_ts)` como referencia de "agora", e nao `now()`.

## D05 - 2026-10-14 - Terminais por lista estatica aproximada
- Decisao: "fora dos terminais" usa `common/terminals.py` (34 terminais com coordenadas
  aproximadas) e raio `TERMINAL_RADIUS_M` (400 m).
- Motivo: a API so informa o letreiro dos terminais (lt0/lt1), sem coordenadas.
- Descartado: inferir terminais pelos pontos extremos da linha (instavel com poucos dados);
  usar GTFS (fica como proximo passo).
- Limitacao: coordenadas aproximadas; garagens e pontos finais que nao sao terminais podem gerar alertas.

## D06 - 2026-10-14 - Uma imagem Python para todos os servicos
- Decisao: `docker/python.Dockerfile` unico; cada servico muda apenas o comando.
- Motivo: build unico e cache compartilhado; o codigo comum (`common/`) fica sempre consistente.
- Descartado: uma imagem por servico (mais lento e duplicado para um projeto deste tamanho).

## D07 - 2026-10-14 - Idempotencia no banco
- Decisao: `bus_positions` com UNIQUE (vehicle_id, event_ts) e `ON CONFLICT DO NOTHING`;
  `line_metrics` com upsert por (line_code, direction, window_start); `bus_alerts` com
  `alert_id` deterministico.
- Motivo: o commit de offset acontece depois da escrita; em falha, mensagens sao
  reprocessadas (at-least-once) sem duplicar linhas.

## D08 - 2026-10-14 - Estado dos consumers de processamento em memoria
- Decisao: ultimo ponto por veiculo, janelas abertas e sequencias de parada ficam em memoria.
- Motivo: simplicidade; o estado se reconstroi em poucos minutos de dados.
- Limitacao: ao reiniciar, a janela em andamento pode sair incompleta e uma parada em curso
  recomeca a contagem. Alternativa futura: Kafka Streams/Flink/Spark com state store.

## D09 - 2026-10-14 - Estrategia de validacao
- Decisao: tres niveis de teste. (1) Testes unitarios com pytest para as regras puras;
  (2) teste ponta a ponta em processo (replay do sample -> logica dos consumers, sem
  broker); (3) teste e2e contra a stack Docker (`make up` + `make e2e`), que espera
  linhas em todas as tabelas e confere as views e os alertas esperados do sample.
- Motivo: os dois primeiros niveis rodam em segundos sem infraestrutura; o terceiro
  valida a integracao real (Kafka, Postgres, healthchecks, imagens arm64).

## D10 - 2026-10-14 - Loop de processamento compartilhado em common/stream_loop.py
- Decisao: os consumers de metricas e alertas usam `common.stream_loop.StreamLoop`
  (consume -> process -> publica + grava -> commit de offsets) e `KafkaPublisher`.
  Cada lote e aplicado ao estado em memoria uma unica vez; as saidas ficam pendentes e
  apenas elas sao reenviadas com backoff se Kafka ou Postgres falharem.
- Motivo: mesma garantia (at-least-once + idempotencia) e mesmo tratamento de erro nos
  dois consumers, sem duplicar codigo e sem contar leituras duas vezes no retry.
- Descartado: reaplicar o lote inteiro no retry (contaria velocidades duas vezes nas janelas).
- Atualizacao: o sink grava no Postgres antes de publicar no Kafka
  (`PostgresKafkaSink`), para nao republicar tudo quando so o banco esta fora.

## D11 - 2026-10-14 - Guarda contra timestamp no futuro nas janelas
- Decisao: leitura com `event_ts` mais de 5 minutos a frente de `collected_at` nao entra
  nas janelas (contador "future").
- Motivo: um relogio de GPS errado empurraria o watermark e fecharia janelas cedo demais.

## D12 - 2026-10-14 - Correcoes apos revisao de codigo
- `parse_ts` levanta TypeError para tipos invalidos; `from_dict`/`flatten_posicao`
  tratam o caso; o decode dos consumers trata qualquer falha como mensagem malformada.
  Uma mensagem envenenada nao derruba mais nenhum servico.
- O replay ancora o inicio em max(agora, ultimo event_ts do topico + intervalo),
  lido do fim de cada particao (`producer/topic_tail.py`). Reiniciar o producer ou
  fazer `make down`/`make up` nao faz o tempo de evento voltar.
- O watermark das janelas e por particao (minimo entre as particoes).
- Portas so em 127.0.0.1; tags de imagem fixas; token so no producer e no recorder.
- `Settings` nao mostra token nem senha no repr; as esperas de inicializacao
  respeitam SIGTERM; decode e sink compartilhados em `common/stream_loop.py`.
- Falhas de entrega logadas em resumo por coleta.

## D13 - 2026-10-14 - Limitacoes aceitas
- `bus_positions` nao tem retencao automatica; `make prune h=24` apaga leituras antigas.
  Em live sao centenas de milhares de linhas por hora. Alternativa futura: particionamento por dia.
- Se o consumer de alertas reiniciar no meio de uma parada que ja alertou, a
  sequencia recomeca e pode gerar um segundo alerta (outro `stopped_since`) para a mesma parada.
- O consumer de metricas roda com uma unica instancia. As mensagens sao particionadas
  por veiculo; duas instancias calculariam janelas parciais da mesma linha. Escalar exigiria
  reparticionar por linha em um topico intermediario.
