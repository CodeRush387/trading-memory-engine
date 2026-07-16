FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    TME_DB=/data/tme.db \
    TME_HOST=0.0.0.0

WORKDIR /app

COPY pyproject.toml README.md ./
COPY tme ./tme
COPY web ./web

RUN python -m pip install --no-cache-dir . \
    && useradd --create-home --uid 10001 tme \
    && mkdir -p /data \
    && chown -R tme:tme /app /data

USER tme
EXPOSE 8080
CMD ["tme", "serve"]