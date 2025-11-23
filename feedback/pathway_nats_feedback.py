## This is the feedback node, it will recieve a (delayed) stream from fraud.feedback
## That stream will have all those txns that were wrongly marked by the main model (the decision ronly node)
## This node will then re-train the model (saved in the shared space) whenever such an error happens



import pathway as pw
import json, pickle
from datetime import datetime
from pathlib import Path
from river import tree, preprocessing, compose

from shared.schema import TransactionSchema
from shared.config import (
    NATS_URI,
    FEEDBACK_TOPIC,
    PERSISTENCE_DIR,
    CHECKPOINT_CONFIG,
    ML_MODEL_GRACE_PERIOD_MAIN,
    ML_MODEL_DELTA_MAIN,
    ML_MODEL_SEED_MAIN,
    ML_MODEL_GRACE_PERIOD_VALIDATOR,
    ML_MODEL_DELTA_VALIDATOR,
    ML_MODEL_SEED_VALIDATOR,
    MAX_TXN_COUNT,
    MODEL_SAVE_INTERVAL
)

# ---------------------------------------------------------------------

class SharedModel:
    def __init__(self):
        self.model_path = PERSISTENCE_DIR / "ml_models.pkl"
        self.stats_path = PERSISTENCE_DIR / "stats.json"
        self.processed_path = PERSISTENCE_DIR / "processed_trans.json"
        
        if self.model_path.exists():
            print("🔄 Loading existing shared ML models...")
            with open(self.model_path, "rb") as f:
                saved = pickle.load(f)
                self.model_main = saved["model_main"]
                self.model_validator = saved["model_validator"]
            print("✓ Shared models restored.")
        else:
            print("⚠️ No model found. Initializing new models.")
            self.model_main = compose.Pipeline(
                preprocessing.StandardScaler(),
                tree.HoeffdingAdaptiveTreeClassifier(
                    grace_period=ML_MODEL_GRACE_PERIOD_MAIN, delta=ML_MODEL_DELTA_MAIN, seed=ML_MODEL_SEED_MAIN
                )
            )
            self.model_validator = tree.HoeffdingAdaptiveTreeClassifier(
                grace_period=ML_MODEL_GRACE_PERIOD_VALIDATOR, delta=ML_MODEL_DELTA_VALIDATOR, seed=ML_MODEL_SEED_VALIDATOR
            )

        # Load stats & processed IDs
        self.stats = {"learned": 0}
        if self.stats_path.exists():
            self.stats = json.load(open(self.stats_path))

        self.processed = set()
        if self.processed_path.exists():
            self.processed = set(json.load(open(self.processed_path)))

        self.last_save = datetime.now()

    def save(self):
        """WRITE the shared model + stats"""
        with open(self.model_path, "wb") as f:
            pickle.dump({
                "model_main": self.model_main,
                "model_validator": self.model_validator
            }, f)

        json.dump(self.stats, open(self.stats_path, "w"))
        json.dump(list(self.processed), open(self.processed_path, "w"))

shared = SharedModel()

# ---------------------------------------------------------------------

@pw.udf
def learn_model(trans_num, amt, dist, hour, is_fraud, customer_txn_count):
    """Feedback learner trains ONLY."""
    if trans_num in shared.processed:
        return

    shared.processed.add(trans_num)
    shared.stats["learned"] += 1

    feats = {
        "amt": float(amt),
        "dist": float(dist),
        "hr": float(hour),
        "n": float(min(customer_txn_count, MAX_TXN_COUNT))
    }

    try:
        shared.model_main.learn_one(feats, is_fraud)
        shared.model_validator.learn_one(feats, is_fraud)
    except:
        pass

    # Save every N updates
    if shared.stats["learned"] % MODEL_SAVE_INTERVAL == 0:
        print(f"💾 Saved after {shared.stats['learned']} updates")
        shared.save()

# ---------------------------------------------------------------------
import math
@pw.udf
def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate distance between two points in km"""
    try:
        d_lat = math.radians(lat2 - lat1)
        d_lon = math.radians(lon2 - lon1)
        a = (math.sin(d_lat/2)**2 + 
             math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * 
             math.sin(d_lon/2)**2)
        return 6371 * 2 * math.asin(math.sqrt(a))
    except:
        return 0.0


@pw.udf
def extract_hour(unix_time: int) -> int:
    """Extract hour from unix timestamp"""
    try:
        return datetime.fromtimestamp(unix_time).hour
    except:
        return 0
def run_feedback():
    print("══════════════════════════════════════════")
    print("      FEEDBACK TRAINER (sole writer)      ")
    print("══════════════════════════════════════════")
    print(f"Listening on: {FEEDBACK_TOPIC}")
    print(f"Persistence : {PERSISTENCE_DIR}")
    print()

    feedback = pw.io.nats.read(
        uri=NATS_URI,
        topic=FEEDBACK_TOPIC,
        schema=TransactionSchema,
        format="json",
        persistent_id="feedback_writer"    # <- ONLY HERE WRITES!
    )

    enriched = feedback.select(
        *pw.this,
        hour=extract_hour(pw.this.unix_time),
        distance=haversine_distance(pw.this.lat, pw.this.long,
                                    pw.this.merch_lat, pw.this.merch_long)
    )

    trained = enriched.select(
        learn=learn_model(
            pw.this.trans_num,
            pw.this.amt,
            pw.this.distance,
            pw.this.hour,
            pw.this.is_fraud,
            pw.this.customer_txn_count
        )
    )

    pw.run(
        persistence_config=CHECKPOINT_CONFIG
    )
