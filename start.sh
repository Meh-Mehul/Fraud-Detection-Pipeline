#!/bin/bash

echo "Starting Fraud Detection Pipeline..."

# Clean previous data
./clean.sh

# Start all services
docker compose up -d

# Wait for services to be ready
echo "Waiting for services to initialize..."
sleep 10

# Load Redis stats
docker compose exec -T detector python redis_manager.py load

echo "✓ Pipeline started successfully!"
echo "✓ Grafana: http://localhost:3000 (admin/admin)"
echo "✓ Frontend: http://localhost:8000"