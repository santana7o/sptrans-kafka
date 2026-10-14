# Atalhos do projeto. Rode "make" ou "make help" para ver os alvos.

COMPOSE ?= docker compose
VENV_PY ?= .venv/bin/python
VENV_PYTEST ?= .venv/bin/pytest

# Token preenchido no .env? (nao imprime o valor)
TOKEN_CHECK = grep -qE '^SPTRANS_TOKEN=["'\'']?[^"'\''[:space:]]' .env

.DEFAULT_GOAL := help

.PHONY: help env up down clean logs ps test test-local replay live record e2e psql sample \
	require-token urls

help: ## Mostra esta ajuda
	@echo "Uso: make <alvo>"
	@echo ""
	@grep -E '^[a-zA-Z0-9_-]+:.*## ' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*## "}; {printf "  %-12s %s\n", $$1, $$2}'
	@echo ""
	@echo "Exemplo: make logs s=producer"

env: ## Cria o .env a partir do .env.example se ainda nao existir
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo "Arquivo .env criado a partir de .env.example (MODE=replay, sem token)."; \
	fi

up: env ## Sobe a stack completa (modo definido no .env, padrao replay)
	$(COMPOSE) up -d --build
	@$(MAKE) --no-print-directory urls

urls:
	@echo ""
	@echo "Stack no ar (os servicos podem levar alguns segundos para ficar saudaveis):"
	@echo "  Kafka UI    http://localhost:8080"
	@echo "  Dashboard   http://localhost:8501"
	@echo "  Kafka       localhost:9092"
	@echo "  PostgreSQL  localhost:5432 (make psql)"
	@echo "Acompanhe com: make ps | make logs s=producer"

down: env ## Para e remove os containers (mantem os volumes)
	$(COMPOSE) --profile tools down

clean: env ## Para tudo e apaga os volumes (Kafka e PostgreSQL)
	$(COMPOSE) --profile tools down -v --remove-orphans

logs: env ## Segue os logs (todos ou um servico: make logs s=producer)
	$(COMPOSE) logs -f --tail=200 $(s)

ps: env ## Lista os containers e o estado de saude
	$(COMPOSE) ps -a

test: env ## Testes unitarios no container (pytest -m "not e2e")
	$(COMPOSE) run --rm --no-deps --build tests pytest -q -m "not e2e"

test-local: ## Testes unitarios no venv local (.venv)
	$(VENV_PYTEST) -q -m "not e2e"

replay: env ## Sobe a stack com o producer em modo replay (sem token)
	MODE=replay $(COMPOSE) up -d --build
	@$(MAKE) --no-print-directory urls

require-token: env
	@if ! $(TOKEN_CHECK); then \
		echo "Erro: SPTRANS_TOKEN esta vazio no .env."; \
		echo "Gere um token em https://www.sptrans.com.br/desenvolvedores/ e preencha SPTRANS_TOKEN no .env."; \
		exit 1; \
	fi

live: require-token ## Sobe a stack com o producer lendo a API Olho Vivo (exige SPTRANS_TOKEN)
	MODE=live $(COMPOSE) up -d --build
	@$(MAKE) --no-print-directory urls

record: require-token ## Grava coletas reais em data/samples (exige SPTRANS_TOKEN)
	$(COMPOSE) run --rm --no-deps --build recorder

e2e: env ## Teste ponta a ponta contra a stack rodando (rode make up antes)
	$(COMPOSE) run --rm --no-deps --build tests pytest -q -m e2e

psql: env ## Abre o psql no banco do container
	$(COMPOSE) exec postgres sh -c 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"'

sample: env ## Regenera o sample sintetico (data/samples/sample_synthetic.jsonl.gz)
	$(COMPOSE) run --rm --no-deps --build recorder python -m producer.synthetic
