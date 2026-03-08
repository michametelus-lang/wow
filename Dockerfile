FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PORT=7860 \
    WEB_CONCURRENCY=4 \
    GUNICORN_THREADS=4 \
    GUNICORN_TIMEOUT=180

WORKDIR /app

COPY requirements.txt ./
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY api ./api
COPY templates ./templates
COPY README.md ./README.md

EXPOSE 7860

CMD ["sh", "-c", "gunicorn --bind 0.0.0.0:${PORT} --workers ${WEB_CONCURRENCY} --threads ${GUNICORN_THREADS} --timeout ${GUNICORN_TIMEOUT} api.index:app"]
