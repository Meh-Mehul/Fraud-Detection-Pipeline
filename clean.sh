#!/bin/bash

# Exit on error
set -e

echo "Removing all __pycache__ directories..."
find . -type d -name "__pycache__" -exec rm -rf {} +

echo "Removing checkpoints_* dirs in ./pathway_persistence/ ..."
if [ -d "./pathway_persistence" ]; then
    find ./pathway_persistence -maxdepth 1 -type d -name "checkpoints_*" -exec rm  -rf {} +
else
    echo "Directory ./pathway_persistence does not exist — skipping."
fi
echo "Removing temp stream files used for simulation..."
if [ -d "./publisher" ]; then
    find ./publisher -maxdepth 1 -type f -name "temp_*" -exec rm  -rf {} +
else
    echo "Directory ./publisher does not exist — skipping."
fi
echo "Removing generated reports used for simulation..."
if [ -d "./fraud_reports" ]; then
    find ./fraud_reports -maxdepth 1 -type f -name "*" -exec rm  -rf {} +
else
    echo "Directory ./publisher does not exist — skipping."
fi

echo "Removing frontend review stats..."
rm -f ./review_stats.json ./frontend_queue.json 2>/dev/null || true

echo "Cleanup complete."
