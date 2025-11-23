# pipeline/feedback/feedback_writer.py
"""
Feedback writer: reads fraud.feedback stream (with ground truth),
updates aggregated stats (shared/stat_store) and trains the shared model.
Saves model periodically to shared/model_store.
"""
import pathway as pw
import json, pickle, time
from pathlib import Path
from datetime import datetime
from river import tree, preprocessing, compose

from shared.schema import TransactionSchema
from shared.model_store import model_store
from shared import stats_store
from shared.config import (
    NATS_URI,
    FEEDBACK_TOPIC,
    DETECTOR_PERSIST_DIR,
    FEEDBACK_CHECKPOINT_CONFIG,
    ML_MODEL_GRACE_PERIOD_MAIN,
    ML_MODEL_DELTA_MAIN,
    ML_MODEL_SEED_MAIN,
    ML_MODEL_GRACE_PERIOD_VALIDATOR,
    ML_MODEL_DELTA_VALIDATOR,
    ML_MODEL_SEED_VALIDATOR,
    ONLINE_CATEGORIES,
    LATE_NIGHT_START,
    LATE_NIGHT_END,
    MAX_TXN_COUNT,
    MODEL_SAVE_INTERVAL
)

# local in-memory models (loaded from model_store if present; feedback writer is sole saver)
class Trainer:
    def __init__(self):
        loaded = model_store.load()
        if loaded:
            self.model_main, self.model_validator = loaded
            print("🔄 Loaded existing model into feedback trainer.")
        else:
            self.model_main = compose.Pipeline(
                preprocessing.StandardScaler(),
                tree.HoeffdingAdaptiveTreeClassifier(grace_period=ML_MODEL_GRACE_PERIOD_MAIN, delta=ML_MODEL_DELTA_MAIN, seed=ML_MODEL_SEED_MAIN)
            )
            self.model_validator = tree.HoeffdingAdaptiveTreeClassifier(grace_period=ML_MODEL_GRACE_PERIOD_VALIDATOR, delta=ML_MODEL_DELTA_VALIDATOR, seed=ML_MODEL_SEED_VALIDATOR)
            print("✓ New trainer models initialized.")
        self.updates = 0
        self.save_every = MODEL_SAVE_INTERVAL

    def learn(self, feats: dict, label: int):
        try:
            self.model_main.learn_one(feats, int(label))
            self.model_validator.learn_one(feats, int(label))
            self.updates += 1
            if self.updates % self.save_every == 0:
                self.save()
        except Exception:
            pass

    def save(self):
        model_store.save(self.model_main, self.model_validator)
        print(f"💾 [FEEDBACK] models saved @ {datetime.utcnow().isoformat()}")

trainer = Trainer()

# small helpers
import math
@pw.udf
def haversine(lat1, lon1, lat2, lon2):
    try:
        d_lat = math.radians(lat2 - lat1)
        d_lon = math.radians(lon2 - lon1)
        a = (math.sin(d_lat/2)**2 +
             math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(d_lon/2)**2)
        return 6371 * 2 * math.asin(math.sqrt(a))
    except:
        return 0.0

@pw.udf
def extract_hour(unix_time: int) -> int:
    try:
        return datetime.fromtimestamp(unix_time).hour
    except:
        return 0

# UDF that performs training using stats_store for aggregated features
@pw.udf
def feedback_train(trans_num, cc_num, amt, lat, long, merch_lat, merch_long, unix_time, category, merchant, is_fraud):
    # compute distance & hour in python-level (udf receives args)
    try:
        hour = datetime.fromtimestamp(int(unix_time)).hour
    except:
        hour = 0
    try:
        import math
        d_lat = math.radians(float(merch_lat) - float(lat))
        d_lon = math.radians(float(merch_long) - float(long))
        a = (math.sin(d_lat/2)**2 +
            math.cos(math.radians(float(lat))) * math.cos(math.radians(float(merch_lat))) * math.sin(d_lon/2)**2)
        distance = 6371 * 2 * math.asin(math.sqrt(a))
    except:
        distance = 0.0

    # Pull existing aggregates (customer/merchant/category)
    cust = stats_store.get_customer_profile(cc_num)
    merch = stats_store.get_merchant_profile(merchant)
    cat = stats_store.get_category_profile(category)

    # build feature dict similar to original detector
    feats = {
        "amt": float(amt),
        "z_amt": float((float(amt) - cust["avg_amt"]) / cust["std_amt"]) if cust["std_amt"] > 0 else 0.0,
        "amt_ratio": float(amt) / cust["avg_amt"] if cust["avg_amt"] > 0 else 1.0,
        "dist": float(distance),
        "z_dist": float((distance - cust["avg_dist"]) / cust["std_dist"]) if cust["std_dist"] > 0 else 0.0,
        "hr": float(hour),
        "merch_risk": float(merch["fraud_rate"]),
        "cat_risk": float(cat["fraud_rate"]),
        "online": float(1 if category in ONLINE_CATEGORIES else 0),
        "late_night": float(1 if LATE_NIGHT_START <= hour <= LATE_NIGHT_END else 0),
        "fraud_history": float(cust["fraud_history"]),
        "n": float(min(cust["txn_count"], MAX_TXN_COUNT))
    }

    # Train
    try:
        trainer.learn(feats, int(is_fraud))
    except Exception:
        pass

    # Update stats AFTER learning (so future transactions see this one)
    try:
        stats_store.update_customer(cc_num, float(amt), float(distance), int(is_fraud))
        stats_store.update_merchant(merchant, float(amt), int(is_fraud))
        stats_store.update_category(category, int(is_fraud))
    except Exception:
        pass

    return None

def run_feedback_writer():
    print("══════════════════════════════════════════")
    print("      FEEDBACK TRAINER (sole writer)      ")
    print("══════════════════════════════════════════")
    print(f"Listening on: {FEEDBACK_TOPIC}")
    print("Persistence: pipeline/pathway_persistence")
    print()

    feedback = pw.io.nats.read(
        uri=NATS_URI,
        topic=FEEDBACK_TOPIC,
        schema=TransactionSchema,
        format="json",
        persistent_id="feedback_writer"
    )

    enriched = feedback.select(
        *pw.this,
        hour=extract_hour(pw.this.unix_time),
        distance=haversine(pw.this.lat, pw.this.long, pw.this.merch_lat, pw.this.merch_long)
    )

    trained = enriched.select(
        _do_train=feedback_train(
            pw.this.trans_num, pw.this.cc_num,
            pw.this.amt, pw.this.lat, pw.this.long,
            pw.this.merch_lat, pw.this.merch_long,
            pw.this.unix_time, pw.this.category, pw.this.merchant,
            pw.this.is_fraud
        )
    )

    # We don't publish anything; this pipeline exists to train & persist model/stats
    pw.run(persistence_config=FEEDBACK_CHECKPOINT_CONFIG)
