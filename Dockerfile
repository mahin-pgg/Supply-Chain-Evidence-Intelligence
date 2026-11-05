FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    SCEI_SQLITE_PATH=/app/data/local/scei.db \
    OLLAMA_BASE_URL=http://host.docker.internal:11434

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN python -m pip install --upgrade pip && pip install .[documents]
RUN mkdir -p /app/data/local
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=3)"
CMD ["uvicorn", "scei.api.app:app", "--host", "0.0.0.0", "--port", "8000"]
