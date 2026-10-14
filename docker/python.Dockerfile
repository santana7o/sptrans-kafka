# Imagem unica para todos os servicos Python (ver docs/DECISIONS.md, D06).
#
# Stages:
#   base    - Python 3.12 slim, usuario nao root e dependencias de runtime
#   runtime - base + codigo do projeto (producer, consumers, dashboard, recorder)
#   dev     - runtime + dependencias de teste (servico "tests")
#
# Todas as dependencias tem wheel para linux/arm64 (aarch64) e amd64, entao
# nenhum compilador e instalado.

FROM python:3.12.7-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN groupadd --system --gid 1000 app \
    && useradd --system --uid 1000 --gid app --home-dir /home/app --create-home app

WORKDIR /app

COPY requirements.txt ./
RUN pip install -r requirements.txt


FROM base AS runtime

COPY --chown=app:app . .
USER app

CMD ["python", "-m", "producer.main"]


FROM base AS dev

COPY requirements-dev.txt ./
RUN pip install -r requirements-dev.txt

COPY --chown=app:app . .
USER app

CMD ["pytest", "-q"]
