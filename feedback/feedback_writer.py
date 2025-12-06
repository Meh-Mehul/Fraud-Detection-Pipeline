# pipeline/feedback/feedback_writer_enhanced.py
"""
Enhanced Feedback Writer with Model Performance Tracking
Calculates: F1, Precision, Recall using sliding window
"""
from pathlib import Path
import sys
import os
ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))

import pathway as pw
import math
from datetime import datetime
from river import tree, preprocessing, compose

from shared.schema import FeedBackSchema
from shared import model_store
from shared import redis_stats_store

from shared.metrics import (
    initialize_metrics,
    record_model_update,
    record_training_sample,
    set_model_weight_delta,
    get_metrics_manager
)


METRICS_PORT = 8003

NATS_URI = os.environ.get("NATS_URI", "nats://localhost:4222")
FEEDBACK_TOPIC = "fraud.feedback"

PERSIST_DIR = Path("./pathway_persistence")
CHECKPOINT_CONFIG = pw.persistence.Config.simple_config(
    pw.persistence.Backend.filesystem(str(PERSIST_DIR / "checkpoints_feedback")),
    snapshot_interval_ms=10000
)

# Global Redis store
redis_store = redis_stats_store.get_store()


# ═══════════════════════════════════════════════════════════════════════════
# ENHANCED TRAINER WITH PERFORMANCE TRACKING
# ═══════════════════════════════════════════════════════════════════════════
class EnhancedTrainer:
    def __init__(self):
        loaded = model_store.load()
        if loaded:
            self.model_main, self.model_validator = loaded
            print("📄 Loaded existing model into feedback trainer.")
        else:
            self.model_main = compose.Pipeline(
                preprocessing.StandardScaler(),
                tree.HoeffdingAdaptiveTreeClassifier(grace_period=200, delta=1e-5, seed=42)
            )
            self.model_validator = tree.HoeffdingAdaptiveTreeClassifier(
                grace_period=150, delta=1e-4, seed=123
            )
            print("✓ New trainer models initialized.")
        
        self.updates = 0
        self.fraud_count = 0
        self.legit_count = 0
        self.save_every = 50
        
        # For performance calculation and weight delta
        self.last_performance_update = 0
        self.previous_leaf_predictions = None

    def learn(self, feats: dict, label: int, actual_fraud: int):
        """
        Train model and track performance
        label: predicted (1=alert, 0=no alert) - from detector
        actual_fraud: ground truth (1=fraud, 0=legit)
        """
        try:
            # Train the model with ground truth
            self.model_main.learn_one(feats, int(actual_fraud))
            self.model_validator.learn_one(feats, int(actual_fraud))
            self.updates += 1
            
            # Track sample distribution
            if int(actual_fraud) == 1:
                self.fraud_count += 1
                record_training_sample("fraud")
            else:
                self.legit_count += 1
                record_training_sample("legitimate")
            
            # Calculate performance metrics (prediction vs actual)
            # Get current prediction from model
            try:
                pred_proba = self.model_main.predict_proba_one(feats)
                pred_score = pred_proba.get(1, 0.0) * 100
                prediction = 1 if pred_score > 50 else 0  # Threshold for alert
            except:
                prediction = 0
            
            # Add to performance calculator
            metrics_manager = get_metrics_manager()
            if metrics_manager:
                metrics_manager.add_performance_sample(prediction, int(actual_fraud))
            
            # Log progress periodically
            if self.updates % 100 == 0:
                total = self.fraud_count + self.legit_count
                fraud_rate = (self.fraud_count / total * 100) if total > 0 else 0
                print(f"📚 Trained on {total} samples | Fraud: {self.fraud_count} ({fraud_rate:.1f}%)")
                
                # Print performance metrics
                if metrics_manager:
                    stats = metrics_manager.get_performance_stats()
                    print(f"   📊 F1: {stats['f1']:.3f} | Precision: {stats['precision']:.3f} | Recall: {stats['recall']:.3f}")
            
            if self.updates % self.save_every == 0:
                self.save()
        except Exception as e:
            print(f"❌ Training error: {e}")

    def save(self):
        """Save model and record metrics including weight delta"""
        # Calculate weight delta (using test predictions as proxy for weights)
        try:
            test_features = {
                "amt": 100.0, "z_amt": 0.5, "amt_ratio": 1.2, "dist": 10.0,
                "z_dist": 0.3, "hr": 12.0, "merch_risk": 0.02, "cat_risk": 0.01,
                "online": 0.0, "late_night": 0.0, "fraud_history": 0.0, "n": 100.0
            }
            current_pred = self.model_main.predict_proba_one(test_features).get(1, 0.0)
            
            if self.previous_leaf_predictions is not None:
                delta = abs(current_pred - self.previous_leaf_predictions)
                set_model_weight_delta(delta)
            
            self.previous_leaf_predictions = current_pred
        except Exception as e:
            pass  # Ignore weight delta errors
        
        success = model_store.save(self.model_main, self.model_validator)
        if success:
            record_model_update()
            print(f"💾 [FEEDBACK] Models saved @ {datetime.utcnow().isoformat()} ({self.updates} updates)")
        else:
            print(f"❌ [FEEDBACK] Model save FAILED")


trainer = EnhancedTrainer()


# ═══════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════

@pw.udf
def extract_hour(unix_time: int) -> int:
    try:
        return datetime.fromtimestamp(unix_time).hour
    except:
        return 0


# ═══════════════════════════════════════════════════════════════════════════
# ENHANCED TRAINING UDF
# ═══════════════════════════════════════════════════════════════════════════
@pw.udf
def feedback_train_enhanced(trans_num, cc_num, amt, lat, long, merch_lat, merch_long, 
                            unix_time, category, merchant, is_fraud):
    """
    Enhanced training with performance tracking
    Tracks: model updates, F1/Precision/Recall
    """
    
    # Compute distance & hour
    try:
        hour = datetime.fromtimestamp(int(unix_time)).hour
    except:
        hour = 0
    
    try:
        d_lat = math.radians(float(merch_lat) - float(lat))
        d_lon = math.radians(float(merch_long) - float(long))
        a = (math.sin(d_lat/2)**2 +
            math.cos(math.radians(float(lat))) * math.cos(math.radians(float(merch_lat))) * 
            math.sin(d_lon/2)**2)
        distance = 6371 * 2 * math.asin(math.sqrt(a))
    except:
        distance = 0.0

    # Pull existing aggregates from Redis BEFORE updating
    cust = redis_store.get_customer_profile(cc_num)
    merch = redis_store.get_merchant_profile(merchant)
    cat = redis_store.get_category_profile(category)

    # Build features
    feats = {
        "amt": float(amt),
        "z_amt": float((float(amt) - cust["avg_amt"]) / cust["std_amt"]) if cust["std_amt"] > 0 else 0.0,
        "amt_ratio": float(amt) / cust["avg_amt"] if cust["avg_amt"] > 0 else 1.0,
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

    # Train model with performance tracking
    try:
        # For performance calculation, we need prediction
        # Here we just pass the ground truth for training
        trainer.learn(feats, 0, int(is_fraud))  # 0 = dummy prediction, actual_fraud for training
    except Exception as e:
        print(f"❌ Training error on {trans_num}: {e}")

    # Update Redis stats with ground truth
    try:
        redis_store.update_customer(str(cc_num), float(amt), float(distance), int(is_fraud))
        redis_store.update_merchant(str(merchant), float(amt), int(is_fraud))
        redis_store.update_category(str(category), int(is_fraud))
    except Exception as e:
        print(f"❌ Redis stats update error on {trans_num}: {e}")

    return None


metrics_manager = initialize_metrics("feedback", port=METRICS_PORT)


# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════
def run_feedback_writer():
    print("╔═══════════════════════════════════════════════════════╗")
    print("   FEEDBACK TRAINER - ENHANCED")
    print("   (Model + Performance Metrics)")
    print("╚═══════════════════════════════════════════════════════╝")
    print(f"Listening on: {FEEDBACK_TOPIC}")
    print(f"Model file: {PERSIST_DIR / 'ml_models.pkl'}")
    print(f"Redis: {redis_stats_store.REDIS_HOST}:{redis_stats_store.REDIS_PORT}")
    print(f"Metrics: http://localhost:{METRICS_PORT}/metrics")
    print()
    
    # Check Redis connection
    if redis_store.health_check():
        summary = redis_store.get_stats_summary()
        print(f"✓ Redis connected")
        print(f"   Customers: {summary['customers']:,}")
        print(f"   Merchants: {summary['merchants']:,}")
        print(f"   Categories: {summary['categories']:,}")
    else:
        print("❌ Redis not available!")
        return
    
    print()
    print("─" * 59)

    feedback = pw.io.nats.read(
        uri=NATS_URI,
        topic=FEEDBACK_TOPIC,
        schema=FeedBackSchema,
        format="json",
        persistent_id="feedback_writer"
    )

    enriched = feedback.select(
        *pw.this,
        hour=extract_hour(pw.this.unix_time)
    )

    trained = enriched.select(
        _result=feedback_train_enhanced(
            pw.this.trans_num, pw.this.cc_num,
            pw.this.amt, pw.this.lat, pw.this.long,
            pw.this.merch_lat, pw.this.merch_long,
            pw.this.unix_time, pw.this.category, pw.this.merchant,
            pw.this.is_fraud
        )
    )
    
    pw.io.null.write(trained)
    
    print("✓ Feedback writer running...")
    print("✓ Training model and calculating performance metrics")
    print("✓ Tracking: F1, Precision, Recall, Model Updates")
    print()
    
    pw.run(persistence_config=CHECKPOINT_CONFIG)


if __name__ == "__main__":
    run_feedback_writer()