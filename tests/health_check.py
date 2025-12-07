#!/usr/bin/env python3
"""
Diagnostic script to check model training and stats state.
Run from project root: python pipeline/diagnostics/check_system.py
"""
import json
import pickle
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))

from shared import stats_store

PERSIST_DIR = Path("./pathway_persistence")
MODEL_FILE = PERSIST_DIR / "ml_models.pkl"
STATS_FILE = PERSIST_DIR / "stats_store.json"

print("=" * 60)
print("  FRAUD DETECTION SYSTEM DIAGNOSTICS")
print("=" * 60)

# ─────────────────────────────────────────────────────────
# 1. CHECK MODEL FILE
# ─────────────────────────────────────────────────────────
print("\n[1] MODEL FILE CHECK")
print("-" * 60)

if not MODEL_FILE.exists():
    print(f"❌ Model file NOT FOUND: {MODEL_FILE}")
    print("   → Feedback writer hasn't saved any models yet!")
    print("   → Solution: Ensure feedback writer is running and receiving data")
else:
    print(f"✓ Model file exists: {MODEL_FILE}")
    print(f"  Size: {MODEL_FILE.stat().st_size / 1024:.2f} KB")
    
    # Try loading it
    try:
        with open(MODEL_FILE, 'rb') as f:
            models = pickle.load(f)
        
        if isinstance(models, tuple) and len(models) == 2:
            model_main, model_validator = models
            print(f"✓ Models loaded successfully")
            print(f"  - Main model: {type(model_main).__name__}")
            print(f"  - Validator model: {type(model_validator).__name__}")
            
            # Check if models have been trained
            try:
                # River models have n_samples attribute after training
                if hasattr(model_main, 'n_samples'):
                    print(f"  - Main model samples: {model_main.n_samples if hasattr(model_main, 'n_samples') else 'N/A'}")
                
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
                
                print(f"✓ Test prediction successful:")
                print(f"  - Main: {pred1}")
                print(f"  - Validator: {pred2}")
                
                # If models return empty dict, they're untrained
                if not pred1 or not pred2:
                    print("⚠️  WARNING: Models return empty predictions!")
                    print("   → Models may not be trained yet")
                
            except Exception as e:
                print(f"⚠️  Error during test prediction: {e}")
        else:
            print(f"❌ Invalid model format: {type(models)}")
    
    except Exception as e:
        print(f"❌ Error loading models: {e}")

# ─────────────────────────────────────────────────────────
# 2. CHECK STATS FILE
# ─────────────────────────────────────────────────────────
print("\n[2] STATS FILE CHECK")
print("-" * 60)

if not STATS_FILE.exists():
    print(f"❌ Stats file NOT FOUND: {STATS_FILE}")
else:
    print(f"✓ Stats file exists: {STATS_FILE}")
    
    try:
        with open(STATS_FILE, 'r') as f:
            stats = json.load(f)
        
        n_customers = len(stats.get("customers", {}))
        n_merchants = len(stats.get("merchants", {}))
        n_categories = len(stats.get("categories", {}))
        
        print(f"✓ Stats loaded successfully:")
        print(f"  - Customers: {n_customers}")
        print(f"  - Merchants: {n_merchants}")
        print(f"  - Categories: {n_categories}")
        
        # Show sample customer stats
        if n_customers > 0:
            sample_cc = list(stats["customers"].keys())[0]
            sample_data = stats["customers"][sample_cc]
            print(f"\n  Sample customer ({sample_cc}):")
            print(f"    - Transactions: {sample_data['count']}")
            print(f"    - Total amount: ${sample_data['sum_amt']:.2f}")
            print(f"    - Fraud history: {sample_data['fraud_history']}")
            
            # Compute derived stats
            if sample_data['count'] > 0:
                avg_amt = sample_data['sum_amt'] / sample_data['count']
                print(f"    - Avg amount: ${avg_amt:.2f}")
        
        # Check for any fraud history
        total_fraud = sum(c['fraud_history'] for c in stats.get("customers", {}).values())
        print(f"\n  Total fraud labels received: {total_fraud}")
        if total_fraud == 0:
            print("⚠️  WARNING: No fraud labels found!")
            print("   → Feedback writer may not be receiving data")
            print("   → Or all transactions in feedback are labeled as non-fraud")
    
    except Exception as e:
        print(f"❌ Error loading stats: {e}")

# ─────────────────────────────────────────────────────────
# 3. CHECK NATS TOPICS (if nats-py is available)
# ─────────────────────────────────────────────────────────
print("\n[3] DATA FLOW CHECK")
print("-" * 60)

try:
    import nats
    print("✓ NATS library available")
    print("  To manually check topics, run:")
    print("    nats sub 'fraud.transactions' --count=1")
    print("    nats sub 'fraud.feedback' --count=1")
    print("    nats sub 'fraud.results' --count=1")
    print("    nats sub 'fraud.alerts' --count=1")
except ImportError:
    print("⚠️  NATS CLI not available for automatic check")
    print("  Install: pip install nats-py")

# ─────────────────────────────────────────────────────────
# 4. RECOMMENDATIONS
# ─────────────────────────────────────────────────────────
print("\n[4] RECOMMENDATIONS")
print("-" * 60)

issues = []
recommendations = []

if not MODEL_FILE.exists():
    issues.append("Models not trained")
    recommendations.append("1. Check feedback writer is running: python pipeline/feedback/feedback_writer.py")
    recommendations.append("2. Verify fraud.feedback topic has data with is_fraud labels")
    recommendations.append("3. Check feedback writer logs for errors")

if STATS_FILE.exists():
    with open(STATS_FILE, 'r') as f:
        stats = json.load(f)
    total_fraud = sum(c['fraud_history'] for c in stats.get("customers", {}).values())
    
    if total_fraud == 0:
        issues.append("No fraud labels in training data")
        recommendations.append("4. Ensure your feedback data includes is_fraud=1 samples")
        recommendations.append("5. Models need fraud examples to learn detection patterns")

if not issues:
    print("✓ No obvious issues detected")
    print("\nIf still no alerts, check:")
    print("  - Are your transactions actually fraudulent?")
    print("  - Are ML scores reaching threshold (75+)?")
    print("  - Add debug logging to detector's run_infer() function")
else:
    print("Issues found:")
    for issue in issues:
        print(f"  ❌ {issue}")
    print("\nRecommended actions:")
    for rec in recommendations:
        print(f"  → {rec}")

print("\n" + "=" * 60)