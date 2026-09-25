# syntax=docker/dockerfile:1
# ETAPA 1 — Builder: Instalación limpia y compilación de dependencias
FROM python:3.12-slim AS builder

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential gcc \
    && rm -rf /var/lib/apt/lists/*

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ETAPA 2 — Runtime: Imagen final ligera y segura (Hardening)

FROM python:3.12-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONFAULTHANDLER=1 \
    PATH="/opt/venv/bin:$PATH"

RUN apt-get update && apt-get install -y --no-install-recommends tini \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --gid 10001 appgroup \
    && useradd --uid 10001 --gid appgroup --create-home --shell /usr/sbin/nologin appuser

WORKDIR /app

COPY --from=builder /opt/venv /opt/venv

# Copia la carpeta scr respetando los permisos del usuario appuser
COPY --chown=appuser:appgroup scr/ ./scr/

RUN mkdir -p /app/data && chown -R appuser:appgroup /app/data

USER appuser
EXPOSE 9009

ENTRYPOINT ["/usr/bin/tini", "--"]

CMD ["python", "-u", "scr/receptor.py"]
