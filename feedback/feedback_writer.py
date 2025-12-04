# pipeline/feedback/feedback_writer_redis.py
"""
Feedback writer: reads fraud.feedback stream (with ground truth),
updates Redis stats AND trains the shared model.
"""
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))

import pathway as pw
import math
from datetime import datetime
from river import tree, preprocessing, compose

from shared.schema import FeedBackSchema
from shared import model_store
from shared import redis_stats_store

from shared.metrics import initialize_metrics, get_metrics_manager


METRICS_PORT = 8003


NATS_URI = "nats://localhost:4222"
FEEDBACK_TOPIC = "fraud.feedback"

PERSIST_DIR = Path("./pathway_persistence")
CHECKPOINT_CONFIG = pw.persistence.Config.simple_config(
    pw.persistence.Backend.filesystem(str(PERSIST_DIR / "checkpoints_feedback")),
    snapshot_interval_ms=10000
)

# Global Redis store
redis_store = redis_stats_store.get_store()


# ───────────────────────────────────────────────
# TRAINER WITH STATS TRACKING
# ───────────────────────────────────────────────
class Trainer:
    def __init__(self):
        loaded = model_store.load()
        if loaded:
            self.model_main, self.model_validator = loaded
            print("🔄 Loaded existing model into feedback trainer.")
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
        self.total_count = 0
        self.save_every = 50

    def learn(self, feats: dict, label: int):
        try:
            self.model_main.learn_one(feats, int(label))
            self.model_validator.learn_one(feats, int(label))
            self.updates += 1
            self.total_count += 1
            
            if int(label) == 1:
                self.fraud_count += 1
            
            # Log progress periodically
            if self.updates % 100 == 0:
                fraud_rate = (self.fraud_count / self.total_count * 100) if self.total_count > 0 else 0
                print(f"📚 Trained on {self.total_count} samples | Fraud: {self.fraud_count} ({fraud_rate:.1f}%)")
            
            if self.updates % self.save_every == 0:
                self.save()
        except Exception as e:
            print(f"❌ Training error: {e}")

    def save(self):
        success = model_store.save(self.model_main, self.model_validator)
        if success:
            print(f"💾 [FEEDBACK] Models saved @ {datetime.utcnow().isoformat()} ({self.updates} updates)")
        else:
            print(f"❌ [FEEDBACK] Model save FAILED")


trainer = Trainer()


# ───────────────────────────────────────────────
# HELPERS
# ───────────────────────────────────────────────

@pw.udf
def extract_hour(unix_time: int) -> int:
    try:
        return datetime.fromtimestamp(unix_time).hour
    except:
        return 0


# ───────────────────────────────────────────────
# TRAINING UDF WITH REDIS STATS UPDATE
# ───────────────────────────────────────────────
@pw.udf
def feedback_train_and_update(trans_num, cc_num, amt, lat, long, merch_lat, merch_long, 
                               unix_time, category, merchant, is_fraud):
    """
    Train model AND update Redis stats with ground truth labels.
    This is critical - fraud_history updates here!
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

    # Pull existing aggregates from REDIS BEFORE updating
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

    # Train model with this sample
    try:
        trainer.learn(feats, int(is_fraud))
    except Exception as e:
        print(f"❌ Training error on {trans_num}: {e}")

    # NOW UPDATE REDIS STATS with ground truth
    # CRITICAL: This updates fraud_history when is_fraud=1
    try:
        redis_store.update_customer(str(cc_num), float(amt), float(distance), int(is_fraud))
        redis_store.update_merchant(str(merchant), float(amt), int(is_fraud))
        redis_store.update_category(str(category), int(is_fraud))
    except Exception as e:
        print(f"❌ Redis stats update error on {trans_num}: {e}")

    return None


metrics_manager = initialize_metrics("feedback", port=METRICS_PORT)


# ───────────────────────────────────────────────
# MAIN
# ───────────────────────────────────────────────
def run_feedback_writer():
    print("══════════════════════════════════════════")
    print("   FEEDBACK TRAINER (Model + Redis Stats)")
    print("══════════════════════════════════════════")
    print(f"Listening on: {FEEDBACK_TOPIC}")
    print(f"Model file: {PERSIST_DIR / 'ml_models.pkl'}")
    print(f"Redis: {redis_stats_store.REDIS_HOST}:{redis_stats_store.REDIS_PORT}")
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
    print("───────────────────────────────────────────")

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
        _result=feedback_train_and_update(
            pw.this.trans_num, pw.this.cc_num,
            pw.this.amt, pw.this.lat, pw.this.long,
            pw.this.merch_lat, pw.this.merch_long,
            pw.this.unix_time, pw.this.category, pw.this.merchant,
            pw.this.is_fraud
        )
    )
    
    # Must write to a sink to force execution
    pw.io.null.write(trained)
    
    print("✓ Feedback writer running...")
    print("✓ Training model and updating Redis with ground truth")
    print()
    
    pw.run(persistence_config=CHECKPOINT_CONFIG)


if __name__ == "__main__":
    run_feedback_writer()