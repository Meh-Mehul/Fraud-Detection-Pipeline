#!/usr/bin/env python3
"""
Pre-train models on fraudTrain.csv before deployment.
This creates initial models and stats so the detector starts with knowledge.
Also computes baseline F1 metrics on a validation set for fast dashboard warm-up.
"""
import pandas as pd
import math
import json
from datetime import datetime
from pathlib import Path
from river import tree, preprocessing, compose
from tqdm import tqdm
import sys

# Add project root to path
ROOT = Path(__file__).resolve().parent
sys.path.append(str(ROOT))

from shared import model_store, stats_store

TRAIN_FILE = "fraudTrain.csv"
PERSIST_DIR = Path("./pathway_persistence")

# Sampling strategy for fast pre-training
TARGET_FRAUD = 3000      # Target ~3K fraud cases
TARGET_LEGITIMATE = 9000  # Target ~9K legitimate cases
VALIDATION_SPLIT = 0.2   # 20% for validation

print("═" * 70)
print("  PRE-TRAINING MODELS ON HISTORICAL DATA (FAST MODE)")
print("═" * 70)
print(f"Training file: {TRAIN_FILE}")
print(f"Target sample: {TARGET_FRAUD:,} frauds + {TARGET_LEGITIMATE:,} legitimate")
print(f"Validation split: {VALIDATION_SPLIT*100:.0f}%")
print()

# ──────────────────────────────────────────────────────────────
# 1. LOAD DATA WITH BALANCED SAMPLING
# ──────────────────────────────────────────────────────────────
print("[1/5] Loading and sampling dataset...")

df_full = pd.read_csv(TRAIN_FILE)

# Separate fraud and legitimate
df_fraud = df_full[df_full['is_fraud'] == 1]
df_legit = df_full[df_full['is_fraud'] == 0]

# Sample what we need
n_fraud = min(TARGET_FRAUD, len(df_fraud))
n_legit = min(TARGET_LEGITIMATE, len(df_legit))

df_fraud_sample = df_fraud.sample(n=n_fraud, random_state=42)
df_legit_sample = df_legit.sample(n=n_legit, random_state=42)

# Combine and shuffle
df_all = pd.concat([df_fraud_sample, df_legit_sample]).sample(frac=1, random_state=42).reset_index(drop=True)

# Split into train/validation
val_size = int(len(df_all) * VALIDATION_SPLIT)
df_val = df_all.iloc[:val_size].reset_index(drop=True)
df = df_all.iloc[val_size:].reset_index(drop=True)

print(f"[OK] Loaded {len(df):,} training + {len(df_val):,} validation samples")
print()

# ──────────────────────────────────────────────────────────────
# 2. INITIALIZE MODELS
# ──────────────────────────────────────────────────────────────
print("[2/5] Initializing models...")
model_main = compose.Pipeline(
    preprocessing.StandardScaler(),
    tree.HoeffdingAdaptiveTreeClassifier(grace_period=200, delta=1e-5, seed=42)
)
model_validator = tree.HoeffdingAdaptiveTreeClassifier(
    grace_period=150, delta=1e-4, seed=123
)
print("[OK] Models initialized")
print()

# ──────────────────────────────────────────────────────────────
# 3. TRAIN MODELS & UPDATE STATS
# ──────────────────────────────────────────────────────────────
print("[3/5] Training models and building stats...")
print()

fraud_count = 0
legit_count = 0
errors = 0

for idx, row in tqdm(df.iterrows(), total=len(df), desc="Training", unit="txn"):
    try:
        # Extract basic info
        cc_num = str(row['cc_num'])
        amt = float(row['amt'])
        merchant = str(row['merchant'])
        category = str(row['category'])
        is_fraud = int(row['is_fraud'])
        
        # Compute hour
        try:
            unix_time = int(row['unix_time'])
            hour = datetime.fromtimestamp(unix_time).hour
        except:
            hour = 0
        
        # Compute distance
        try:
            lat = float(row['lat'])
            lon = float(row['long'])
            merch_lat = float(row['merch_lat'])
            merch_long = float(row['merch_long'])
            
            d_lat = math.radians(merch_lat - lat)
            d_lon = math.radians(merch_long - lon)
            a = (math.sin(d_lat/2)**2 +
                 math.cos(math.radians(lat)) * math.cos(math.radians(merch_lat)) *
                 math.sin(d_lon/2)**2)
            distance = 6371 * 2 * math.asin(math.sqrt(a))
        except:
            distance = 0.0
        
        # Get current stats (before updating)
        cust = stats_store.get_customer_profile(cc_num)
        merch = stats_store.get_merchant_profile(merchant)
        cat = stats_store.get_category_profile(category)
        
        # Build features
        feats = {
            "amt": float(amt),
            "z_amt": float((amt - cust["avg_amt"]) / cust["std_amt"]) if cust["std_amt"] > 0 else 0.0,
            "amt_ratio": amt / cust["avg_amt"] if cust["avg_amt"] > 0 else 1.0,
            "dist": float(distance),
            "z_dist": float((distance - cust["avg_dist"]) / cust["std_dist"]) if cust["std_dist"] > 0 else 0.0,
            "hr": float(hour),
            "merch_risk": float(merch["fraud_rate"]),
            "cat_risk": float(cat["fraud_rate"]),
            "online": float(1 if category in ["shopping_net", "misc_net", "grocery_net"] else 0),
            "late_night": float(1 if 1 <= hour <= 5 else 0),
            "fraud_history": float(cust["fraud_history"]),
            "n": float(min(cust["txn_count"], 1000))
        }
        
        # Train models
        model_main.learn_one(feats, is_fraud)
        model_validator.learn_one(feats, is_fraud)
        
        # Update stats AFTER training
        stats_store.update_customer(cc_num, amt, distance, is_fraud)
        stats_store.update_merchant(merchant, amt, is_fraud)
        stats_store.update_category(category, is_fraud)
        
        # Track progress
        if is_fraud == 1:
            fraud_count += 1
        else:
            legit_count += 1
    
    except Exception as e:
        errors += 1
        if errors < 10:  # Only print first few errors
            print(f"\n[WARN]  Error processing row {idx}: {e}")

print(f"\n[OK] Training complete!")
print(f"  Total trained: {fraud_count + legit_count:,}")
print(f"  Fraud samples: {fraud_count:,}")
print(f"  Legit samples: {legit_count:,}")
print(f"  Errors: {errors}")
print()

# ──────────────────────────────────────────────────────────────
# 4. EVALUATE ON VALIDATION SET & SAVE BASELINE METRICS
# ──────────────────────────────────────────────────────────────
print("[4/5] Evaluating model on validation set...")

# Confusion matrix counters
tp, fp, tn, fn = 0, 0, 0, 0
ML_THRESHOLD = 50.0  # Same threshold as detector uses

for idx, row in df_val.iterrows():
    try:
        cc_num = str(row['cc_num'])
        amt = float(row['amt'])
        merchant = str(row['merchant'])
        category = str(row['category'])
        actual = int(row['is_fraud'])
        
        # Compute hour
        try:
            unix_time = int(row['unix_time'])
            hour = datetime.fromtimestamp(unix_time).hour
        except:
            hour = 0
        
        # Compute distance
        try:
            lat = float(row['lat'])
            lon = float(row['long'])
            merch_lat = float(row['merch_lat'])
            merch_long = float(row['merch_long'])
            d_lat = math.radians(merch_lat - lat)
            d_lon = math.radians(merch_long - lon)
            a = (math.sin(d_lat/2)**2 +
                 math.cos(math.radians(lat)) * math.cos(math.radians(merch_lat)) *
                 math.sin(d_lon/2)**2)
            distance = 6371 * 2 * math.asin(math.sqrt(a))
        except:
            distance = 0.0
        
        # Get stats (using stats from training)
        cust = stats_store.get_customer_profile(cc_num)
        merch_stats = stats_store.get_merchant_profile(merchant)
        cat_stats = stats_store.get_category_profile(category)
        
        # Build features
        feats = {
            "amt": float(amt),
            "z_amt": float((amt - cust["avg_amt"]) / cust["std_amt"]) if cust["std_amt"] > 0 else 0.0,
            "amt_ratio": amt / cust["avg_amt"] if cust["avg_amt"] > 0 else 1.0,
            "dist": float(distance),
            "z_dist": float((distance - cust["avg_dist"]) / cust["std_dist"]) if cust["std_dist"] > 0 else 0.0,
            "hr": float(hour),
            "merch_risk": float(merch_stats["fraud_rate"]),
            "cat_risk": float(cat_stats["fraud_rate"]),
            "online": float(1 if category in ["shopping_net", "misc_net", "grocery_net"] else 0),
            "late_night": float(1 if 1 <= hour <= 5 else 0),
            "fraud_history": float(cust["fraud_history"]),
            "n": float(min(cust["txn_count"], 1000))
        }
        
        # Get predictions
        pred1 = model_main.predict_proba_one(feats)
        pred2 = model_validator.predict_proba_one(feats)
        ml_score = (pred1.get(1, 0.0) + pred2.get(1, 0.0)) / 2 * 100
        
        # Threshold: alert if score >= 50
        predicted = 1 if ml_score >= ML_THRESHOLD else 0
        
        # Update confusion matrix
        if predicted == 1 and actual == 1:
            tp += 1
        elif predicted == 1 and actual == 0:
            fp += 1
        elif predicted == 0 and actual == 1:
            fn += 1
        else:
            tn += 1
    except:
        pass

# Compute metrics
precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0.0
accuracy = (tp + tn) / (tp + fp + tn + fn) if (tp + fp + tn + fn) > 0 else 0.0

print(f"[OK] Validation evaluation complete:")
print(f"  Samples: {len(df_val):,}")
print(f"  TP: {tp}, FP: {fp}, TN: {tn}, FN: {fn}")
print(f"  Precision: {precision:.3f}")
print(f"  Recall: {recall:.3f}")
print(f"  F1 Score: {f1:.3f}")
print(f"  Accuracy: {accuracy:.3f}")

# Save baseline metrics
baseline_metrics = {
    "f1": f1,
    "precision": precision,
    "recall": recall,
    "accuracy": accuracy,
    "tp": tp,
    "fp": fp,
    "tn": tn,
    "fn": fn,
    "validation_size": len(df_val),
    "ml_threshold": ML_THRESHOLD
}

baseline_path = PERSIST_DIR / "baseline_metrics.json"
with open(baseline_path, 'w') as f:
    json.dump(baseline_metrics, f, indent=2)

print(f"[OK] Baseline metrics saved to {baseline_path}")
print()

# ──────────────────────────────────────────────────────────────
# 5. SAVE MODELS & VERIFY STATS
# ──────────────────────────────────────────────────────────────
print("[5/5] Saving models and verifying stats...")

# Save models
success = model_store.save(model_main, model_validator)
if success:
    print("[OK] Models saved to pathway_persistence/ml_models.pkl")
else:
    print("[ERROR] Failed to save models!")
    sys.exit(1)

# Verify stats
summary = stats_store.get_stats_summary()
print(f"[OK] Stats saved:")
print(f"  Customers: {summary['customers']:,}")
print(f"  Merchants: {summary['merchants']:,}")
print(f"  Categories: {summary['categories']:,}")

print()
print("═" * 70)
print("  PRE-TRAINING COMPLETE!")
print("═" * 70)
print()
print(f"[OK] Trained on {len(df):,} samples")
print(f"[OK] Validated on {len(df_val):,} samples")
print(f"[OK] Baseline F1: {f1*100:.1f}%")
print(f"[OK] Model is ready for deployment with baseline knowledge")
print()