FROM python:3.12-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*

RUN useradd --create-home --uid 1000 app
WORKDIR /app

COPY requirements.txt requirements-dashboard.txt ./
RUN pip install --no-cache-dir -r requirements.txt -r requirements-dashboard.txt

COPY producer ./producer
COPY spark_jobs ./spark_jobs
COPY dashboard ./dashboard
COPY scripts ./scripts

# Pre-create the mount points so Docker volumes inherit the right owner
RUN mkdir -p /app/data /app/checkpoints /home/app/.ivy2 \
 && chown -R app:app /app /home/app

USER app
ENV DATA_DIR=/app/data CHECKPOINT_DIR=/app/checkpoints