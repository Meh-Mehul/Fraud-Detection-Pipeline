FROM python:3.10-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Increase pip timeout and use better download settings
ENV PIP_DEFAULT_TIMEOUT=300
ENV PIP_NO_CACHE_DIR=1

# Copy requirements first (layer caching)
COPY requirements.txt .

# Install dependencies with retry logic and increased timeout
RUN pip install --default-timeout=300 --retries 5 -r requirements.txt || \
    pip install --default-timeout=600 --retries 10 -r requirements.txt

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p pathway_persistence fraud_reports publisher/temp shared ato

# Don't run pretrain in Dockerfile - do it in entrypoint or externally
# RUN python pretrain.py

CMD ["python", "run_detector.py"]