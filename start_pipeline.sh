#!/bin/bash

set -e

# Stop any existing containers
docker-compose -f docker-compose-monitoring.yml down 2>/dev/null || true
docker volume rm fraud-detection-pipeline_prometheus-data 2>/dev/null || true
docker volume rm fraud-detection-pipeline_grafana-data 2>/dev/null || true

# Start monitoring stack
docker-compose -f docker-compose-monitoring.yml up -d

# Clean up previous run data
./clean.sh > /dev/null 2>&1 || true

# Load Redis stats
python redis_manager.py load > /dev/null 2>&1 || true

# Start unified pipeline
python run_pipeline.py