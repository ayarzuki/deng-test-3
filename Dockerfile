FROM python:3.12-slim

# Install Java (required by PySpark)
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-21-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt "pyspark>=3.5.0,<4.0.0"

COPY . .
RUN pip install --no-cache-dir -e .

# Default: run the pipeline
CMD ["python", "-m", "job.pipeline", "--config", "config/pipeline.yaml"]
