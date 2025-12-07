#!/usr/bin/env python3
"""
Quick script to verify the pre-trained models are valid.
"""
import pickle
from pathlib import Path
import sys

MODEL_FILE = Path("pathway_persistence/ml_models.pkl")

print("=" * 60)
print("  MODEL VERIFICATION")
print("=" * 60)

# Check if file exists
if not MODEL_FILE.exists():
    print(f"[ERROR] Model file not found: {MODEL_FILE}")
    print("   Run: python3 pretrain.py")
    sys.exit(1)

print(f"[OK] Model file exists: {MODEL_FILE}")
print(f"  Size: {MODEL_FILE.stat().st_size / 1024:.2f} KB")

# Try to load it
try:
    with open(MODEL_FILE, 'rb') as f:
        models = pickle.load(f)
    
    print(f"[OK] File loaded successfully")
    print(f"  Type: {type(models)}")
    
    # Check if it's a tuple
    if not isinstance(models, tuple):
        print(f"[ERROR] ERROR: Expected tuple, got {type(models)}")
        print("   The model file may be corrupted")
        sys.exit(1)
    
    # Check tuple length
    if len(models) != 2:
        print(f"[ERROR] ERROR: Expected 2 models, got {len(models)}")
        sys.exit(1)
    
    model_main, model_validator = models
    print(f"[OK] Contains 2 models:")
    print(f"  - Main: {type(model_main).__name__}")
    print(f"  - Validator: {type(model_validator).__name__}")
    
    # Try a test prediction
    test_feats = {
        "amt": 100.0, "z_amt": 0.0, "amt_ratio": 1.0,
        "dist": 10.0, "z_dist": 0.0, "hr": 12.0,
        "merch_risk": 0.0, "cat_risk": 0.0,
        "online": 0.0, "late_night": 0.0,
        "fraud_history": 0.0, "n": 10.0
    }
    
    pred1 = model_main.predict_proba_one(test_feats)
    pred2 = model_validator.predict_proba_one(test_feats)
    
    if not pred1 or not pred2:
        print("[WARN]  WARNING: Models return empty predictions")
        print("   They may not be trained yet")
    else:
        ml_score = (pred1.get(1, 0.0) + pred2.get(1, 0.0)) / 2 * 100
        print(f"[OK] Test prediction successful:")
        print(f"  - Main fraud prob: {pred1.get(1, 0.0)*100:.1f}%")
        print(f"  - Validator fraud prob: {pred2.get(1, 0.0)*100:.1f}%")
        print(f"  - Combined ML score: {ml_score:.1f}%")
    
    print("\n" + "=" * 60)
    print("✅ MODELS ARE VALID AND READY TO USE!")
    print("=" * 60)
    print("\nYou can now start the pipeline:")
    print("  bash startup_warm.sh")
    print("\nOr manually:")
    print("  python3 pipeline/detector/detector_ronly_debug.py")
    
except Exception as e:
    print(f"[ERROR] ERROR loading models: {e}")
    import traceback
    traceback.print_exc()
    print("\n[ERROR] MODELS ARE CORRUPTED OR INVALID")
    print("   Delete and retrain:")
    print(f"   rm {MODEL_FILE}")
    print("   python3 pretrain.py")
    sys.exit(1)