# Samples de coletas da API Olho Vivo

Arquivos usados pelo producer em modo replay (`MODE=replay`, `REPLAY_FILE=...`).

## Formato

- gzip + JSON Lines (`*.jsonl.gz`), uma linha por coleta, em ordem cronologica.
- Cada linha: `{"collected_at": "<ISO 8601 UTC com Z>", "payload": <resposta crua de GET /Posicao>}`.
- `payload` segue exatamente a API v2.1: `{"hr": "HH:MM", "l": [{"c", "cl", "sl", "lt0", "lt1", "qv", "vs": [{"p", "a", "ta", "py", "px"}]}]}`.

Contrato completo em `docs/CONTRACTS.md` (secoes 1 e 2).

## sample_synthetic.jsonl.gz (versionado, sintetico)

Gerado por `producer/synthetic.py` (ver `docs/DECISIONS.md`, D03). Nao contem dados reais:
durante a construcao do projeto nao havia token da SPTrans, e o sintetico cobre de
proposito os casos de borda do pipeline.

Regenerar (deterministico, mesmo arquivo byte a byte):

```bash
.venv/bin/python -m producer.synthetic
```

Conteudo:

| Item | Valor |
|---|---|
| Coletas | 81, a cada 30 s (40 minutos, 2026-10-14 11:00 a 11:40 UTC) |
| Linhas | 25 linhas x 2 sentidos (50 codigos `cl`; sentido 2 = `cl` + 32768) |
| Veiculos | 495 (8 a 12 por linha/sentido), cerca de 77% acessiveis |
| Leituras | 40.095 (cerca de 630 KB comprimido) |
| Trajetos | entre pares de terminais de `common/terminals.py`, com pontos intermediarios |
| Velocidades | cruzeiro de 12 a 26 km/h com variacao (8 a 35 km/h), paradas curtas em pontos |

Ao chegar ao terminal o veiculo espera alguns minutos e volta no sentido oposto (passa a
aparecer sob o `cl` do outro sentido, como na API real). O `ta` de cada veiculo tem fase
propria (alguns segundos antes da coleta) e ha ruido de GPS de poucos metros.

Casos de borda plantados:

| Caso | Quantidade | Efeito esperado no pipeline |
|---|---|---|
| (a) parado 14-16 min fora de terminal (> 1,8 km do terminal mais proximo) | 4 veiculos: 53291, 78285, 85920, 86775 | 1 alerta STOPPED_VEHICLE cada |
| (b) parado 14-17 min dentro do terminal (< 50 m do centro) | 4 veiculos: 22908, 46920, 58928, 61050 | nenhum alerta |
| (c) leitura com `py`/`px` = 0 | 12 leituras | publicada; descartada no processamento |
| (d) leitura repetida (mesmo `ta` da coleta anterior) | 562 leituras | descartada pelo deduplicador do producer |
| (e) salto de GPS (2,5 km em ~30 s) | 2 veiculos: 52480, 62144 | velocidade descartada (> 100 km/h) |

## Gravar coletas reais

Com `SPTRANS_TOKEN` definido no `.env` (token gratuito em https://www.sptrans.com.br/desenvolvedores/):

```bash
make record                      # ou: python -m producer.record
```

Grava `RECORD_COLLECTIONS` coletas (padrao 20), uma a cada `POLL_INTERVAL_SECONDS`, em
`RECORD_OUTPUT_DIR/recording_<UTC>.jsonl.gz`. Para reproduzir:

```bash
# no .env
MODE=replay
REPLAY_FILE=data/samples/recording_20261014T120000Z.jsonl.gz
```

Gravacoes reais tem dezenas de MB (toda a frota, ~15 mil veiculos por coleta) e nao
devem ser versionadas.
