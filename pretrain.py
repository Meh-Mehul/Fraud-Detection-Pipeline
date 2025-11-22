#!/usr/bin/env python3
"""
Pre-train models on fraudTrain.csv before deployment.
This creates initial models and stats so the detector starts with knowledge.
"""
import pandas as pd
import math
from datetime import datetime
from pathlib import Path
from river import tree, preprocessing, compose
import sys

# Add project root to path
ROOT = Path(__file__).resolve().parent
sys.path.append(str(ROOT))

from shared import model_store, stats_store

TRAIN_FILE = "fraudTrain.csv"

# Sampling strategy for fast pre-training
TARGET_FRAUD = 3000      # Target ~3K fraud cases
TARGET_LEGITIMATE = 9000  # Target ~9K legitimate cases

print("═" * 70)
print("  PRE-TRAINING MODELS ON HISTORICAL DATA (FAST MODE)")
print("═" * 70)
print(f"Training file: {TRAIN_FILE}")
print(f"Target sample: {TARGET_FRAUD:,} frauds + {TARGET_LEGITIMATE:,} legitimate")
print()

# ──────────────────────────────────────────────────────────────
# 1. LOAD DATA WITH BALANCED SAMPLING
# ──────────────────────────────────────────────────────────────
print("[1/4] Loading and sampling dataset...")
print("Loading full dataset to extract balanced sample...")

df_full = pd.read_csv(TRAIN_FILE)
print(f"✓ Loaded {len(df_full):,} total transactions")

# Separate fraud and legitimate
df_fraud = df_full[df_full['is_fraud'] == 1]
df_legit = df_full[df_full['is_fraud'] == 0]

print(f"  Available: {len(df_fraud):,} frauds, {len(df_legit):,} legitimate")

# Sample what we need
n_fraud = min(TARGET_FRAUD, len(df_fraud))
n_legit = min(TARGET_LEGITIMATE, len(df_legit))

df_fraud_sample = df_fraud.sample(n=n_fraud, random_state=42)
df_legit_sample = df_legit.sample(n=n_legit, random_state=42)

# Combine and shuffle
df = pd.concat([df_fraud_sample, df_legit_sample]).sample(frac=1, random_state=42).reset_index(drop=True)

print(f"✓ Sampled {len(df):,} transactions for training")
print(f"  Fraud cases: {df['is_fraud'].sum():,} ({df['is_fraud'].mean()*100:.2f}%)")
print(f"  Legitimate: {(~df['is_fraud'].astype(bool)).sum():,} ({(~df['is_fraud'].astype(bool)).sum()/len(df)*100:.2f}%)")
print(f"  ⚡ This will be ~200x faster than full dataset!")
print()

# ──────────────────────────────────────────────────────────────
# 2. INITIALIZE MODELS
# ──────────────────────────────────────────────────────────────
print("[2/4] Initializing models...")
model_main = compose.Pipeline(
    preprocessing.StandardScaler(),
    tree.HoeffdingAdaptiveTreeClassifier(grace_period=200, delta=1e-5, seed=42)
)
model_validator = tree.HoeffdingAdaptiveTreeClassifier(
    grace_period=150, delta=1e-4, seed=123
)
print("✓ Models initialized")
print()

# ──────────────────────────────────────────────────────────────
# 3. TRAIN MODELS & UPDATE STATS
# ──────────────────────────────────────────────────────────────
print("[3/4] Training models and building stats...")
print("This may take a few minutes for large datasets...")
print()

fraud_count = 0
legit_count = 0
errors = 0

for idx, row in df.iterrows():
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
        
        # Progress indicator
        if (idx + 1) % 1000 == 0:
            total = idx + 1
            fraud_pct = (fraud_count / total * 100) if total > 0 else 0
            print(f"  Processed: {total:,} | Fraud: {fraud_count} ({fraud_pct:.2f}%) | Legit: {legit_count}", end="\r")
    
    except Exception as e:
        errors += 1
        if errors < 10:  # Only print first few errors
            print(f"\n⚠️  Error processing row {idx}: {e}")

print(f"\n✓ Training complete!")
print(f"  Total trained: {fraud_count + legit_count:,}")
print(f"  Fraud samples: {fraud_count:,}")
print(f"  Legit samples: {legit_count:,}")
print(f"  Errors: {errors}")
print()

# ──────────────────────────────────────────────────────────────
# 4. SAVE MODELS & VERIFY STATS
# ──────────────────────────────────────────────────────────────
print("[4/4] Saving models and verifying stats...")

# Save models
success = model_store.save(model_main, model_validator)
if success:
    print("✓ Models saved to pathway_persistence/ml_models.pkl")
else:
    print("❌ Failed to save models!")
    sys.exit(1)

# Verify stats
summary = stats_store.get_stats_summary()
print(f"✓ Stats saved:")
print(f"  Customers: {summary['customers']:,}")
print(f"  Merchants: {summary['merchants']:,}")
print(f"  Categories: {summary['categories']:,}")

# Test a prediction
test_feats = {
    "amt": 100.0, "z_amt": 0.0, "amt_ratio": 1.0,
    "dist": 10.0, "z_dist": 0.0, "hr": 12.0,
    "merch_risk": 0.0, "cat_risk": 0.0,
    "online": 0.0, "late_night": 0.0,
    "fraud_history": 0.0, "n": 10.0
}
pred1 = model_main.predict_proba_one(test_feats)
pred2 = model_validator.predict_proba_one(test_feats)
ml_score = (pred1.get(1, 0.0) + pred2.get(1, 0.0)) / 2 * 100

print(f"\n✓ Test prediction successful:")
print(f"  Main model: {pred1}")
print(f"  Validator: {pred2}")
print(f"  ML Score: {ml_score:.1f}%")

print()
print("═" * 70)
print("  PRE-TRAINING COMPLETE!")
print("═" * 70)
print()
print(f"✓ Trained on {len(df):,} samples ({n_fraud:,} frauds, {n_legit:,} legitimate)")
print(f"✓ Model is ready for deployment with baseline knowledge")
print(f"✓ Continuous learning will improve it further during operation")
print()
print("Next steps:")
print("  1. Run: bash startup_warm.sh")
print("  2. Or manually start components in order:")
print("     - python3 pipeline/detector/detector_ronly_debug.py")
print("     - python3 pipeline/stats/stats_updater.py")
print("     - python3 pipeline/feedback/feedback_writer.py")
print("     - python3 pipeline/publisher/pub_detector.py")
print("     - python3 pipeline/publisher/pub_feedback.py  (optional)")
print()