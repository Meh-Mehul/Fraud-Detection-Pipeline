#!/bin/bash

echo "Stopping Fraud Detection Pipeline..."
docker compose down

echo "Removing volumes (optional)..."
# docker-compose down -v  # Uncomment to remove volumes