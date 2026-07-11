# One image runs any stage of the pipeline (trainer / producer / streaming /
# dashboard). Spark needs a JVM, so we start from a slim Python base and add a
# headless JRE.
FROM python:3.11-slim

# Java for PySpark + procps for basic diagnostics.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install Python deps first so this layer caches across code changes.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Application code.
COPY src/ ./src/
COPY data/ ./data/

# Default command is overridden per-service in docker-compose.yml.
CMD ["python", "src/producer.py"]
