#!/bin/bash
# cleanup.sh - Remove corrupted model files and start fresh

echo "════════════════════════════════════════════════════════"
echo "  CLEANUP CORRUPTED MODELS"
echo "════════════════════════════════════════════════════════"
echo ""

MODEL_FILE="pathway_persistence/ml_models.pkl"

if [ -f "$MODEL_FILE" ]; then
    echo "Found existing model file: $MODEL_FILE"
    
    # Check if it's the old dict format
    python3 -c "
import pickle
from pathlib import Path
try:
    with open('$MODEL_FILE', 'rb') as f:
        models = pickle.load(f)
    if isinstance(models, dict):
        print('⚠️  CORRUPTED: File is in old dict format')
        exit(1)
    else:
        print('✓ File format looks OK')
        exit(0)
except Exception as e:
    print(f'⚠️  ERROR: {e}')
    exit(1)
"
    
    if [ $? -ne 0 ]; then
        echo ""
        echo "Deleting corrupted model file..."
        rm -f "$MODEL_FILE"
        rm -f "${MODEL_FILE}.tmp"
        echo "✓ Deleted"
    fi
else
    echo "No existing model file found (this is OK)"
fi

echo ""
echo "════════════════════════════════════════════════════════"
echo "Now run: python3 pretrain.py"
echo "════════════════════════════════════════════════════════"