## This is the feedback node, it will recieve a (delayed) stream from fraud.feedback
## That stream will have all those txns that were wrongly marked by the main model (the decision ronly node)
## This node will then re-train the model (saved in the shared space) whenever such an error happens



import pathway as pw
import json, pickle
from datetime import datetime
from pathlib import Path
from river import tree, preprocessing, compose

NATS_URI = "nats://localhost:4222"
FEEDBACK_TOPIC = "fraud.feedback"

PERSIST = Path("pathway_persistence")
PERSIST.mkdir(exist_ok=True)

MODEL_PATH = PERSIST / "ml_models.pkl"
STATS_PATH = PERSIST / "stats.json"
PROCESSED_PATH = PERSIST / "processed_trans.json"
CHECKPOINT_DIR = PERSIST / "checkpoints"
class TransactionSchema(pw.Schema):
    trans_num: str = pw.column_definition(dtype=str)
    trans_date_trans_time: str = pw.column_definition(dtype=str)
    cc_num: int = pw.column_definition(dtype=int)
    merchant: str = pw.column_definition(dtype=str)
    category: str = pw.column_definition(dtype=str)
    amt: float = pw.column_definition(dtype=float)
    first: str = pw.column_definition(dtype=str)
    last: str = pw.column_definition(dtype=str)
    gender: str = pw.column_definition(dtype=str)
    street: str = pw.column_definition(dtype=str)
    city: str = pw.column_definition(dtype=str)
    state: str = pw.column_definition(dtype=str)
    zip: int = pw.column_definition(dtype=int)
    lat: float = pw.column_definition(dtype=float)
    long: float = pw.column_definition(dtype=float)
    city_pop: int = pw.column_definition(dtype=int)
    job: str = pw.column_definition(dtype=str)
    dob: str = pw.column_definition(dtype=str)
    unix_time: int = pw.column_definition(dtype=int)
    merch_lat: float = pw.column_definition(dtype=float)
    merch_long: float = pw.column_definition(dtype=float)
    is_fraud: int = pw.column_definition(dtype=int)


# ---------------------------------------------------------------------

class SharedModel:
    def __init__(self):
        if MODEL_PATH.exists():
            print("🔄 Loading existing shared ML models...")
            with open(MODEL_PATH, "rb") as f:
                saved = pickle.load(f)
                self.model_main = saved["model_main"]
                self.model_validator = saved["model_validator"]
            print("✓ Shared models restored.")
        else:
            print("⚠️ No model found. Initializing new models.")
            self.model_main = compose.Pipeline(
                preprocessing.StandardScaler(),
                tree.HoeffdingAdaptiveTreeClassifier(
                    grace_period=200, delta=1e-5, seed=42
                )
            )
            self.model_validator = tree.HoeffdingAdaptiveTreeClassifier(
                grace_period=150, delta=1e-4, seed=123
            )

        # Load stats & processed IDs
        self.stats = {"learned": 0}
        if STATS_PATH.exists():
            self.stats = json.load(open(STATS_PATH))

        self.processed = set()
        if PROCESSED_PATH.exists():
            self.processed = set(json.load(open(PROCESSED_PATH)))

        self.last_save = datetime.now()

    def save(self):
        """WRITE the shared model + stats"""
        with open(MODEL_PATH, "wb") as f:
            pickle.dump({
                "model_main": self.model_main,
                "model_validator": self.model_validator
            }, f)

        json.dump(self.stats, open(STATS_PATH, "w"))
        json.dump(list(self.processed), open(PROCESSED_PATH, "w"))

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
        "n": float(min(customer_txn_count, 1000))
    }

    try:
        shared.model_main.learn_one(feats, is_fraud)
        shared.model_validator.learn_one(feats, is_fraud)
    except:
        pass

    # Save every N updates
    if shared.stats["learned"] % 50 == 0:
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
    print(f"Persistence : {PERSIST}")
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
        persistence_config=pw.persistence.Config.simple_config(
            pw.persistence.Backend.filesystem(str(CHECKPOINT_DIR)),
            snapshot_interval_ms=10000
        )
    )
