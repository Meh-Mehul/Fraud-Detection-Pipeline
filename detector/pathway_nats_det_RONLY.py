## This is to be run with the feedback stream ONLY
## It does NOT learn online, instead it loads a model from the shared space of feedback node and this
## That model is periodically re-loaded from there and this node only makes decisions



import pathway as pw
import json, pickle, threading, time, math
from datetime import datetime
from pathlib import Path
from river import tree, preprocessing, compose

# -----------------------------------------------------------------------------
# CONFIG
# -----------------------------------------------------------------------------

NATS_URI = "nats://localhost:4222"
NATS_INPUT_TOPIC = "fraud.transactions"
NATS_ALERTS_TOPIC = "fraud.alerts"
NATS_RESULTS_TOPIC = "fraud.results"

PERSIST = Path("pathway_persistence")
MODEL_PATH = PERSIST / "ml_models.pkl"
PROCESSED_PATH = PERSIST / "processed_trans.json"
STATS_PATH = PERSIST / "stats.json"
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

# -----------------------------------------------------------------------------
# LOAD SHARED MODEL (READ-ONLY)
# -----------------------------------------------------------------------------

class SharedModelRO:
    """Detector loads the shared model, never writes."""

    def __init__(self):
        self.load()

        # Reload every 5 seconds in the background
        t = threading.Thread(target=self.autoreload, daemon=True)
        t.start()

    def load(self):
        """Load current models & stats from disk"""
        try:
            if MODEL_PATH.exists():
                with open(MODEL_PATH, "rb") as f:
                    saved = pickle.load(f)
                    self.model_main = saved["model_main"]
                    self.model_validator = saved["model_validator"]
            else:
                print("⚠️ Model not found, initializing new one.")
                self.model_main = compose.Pipeline(
                    preprocessing.StandardScaler(),
                    tree.HoeffdingAdaptiveTreeClassifier(
                        grace_period=200, delta=1e-5, seed=42))
                self.model_validator = tree.HoeffdingAdaptiveTreeClassifier(
                    grace_period=150, delta=1e-4, seed=123)

            if PROCESSED_PATH.exists():
                self.processed = set(json.load(open(PROCESSED_PATH)))
            else:
                self.processed = set()

            if STATS_PATH.exists():
                self.stats = json.load(open(STATS_PATH))
            else:
                self.stats = {"total": 0, "alerts": 0}

            print("✓ Detector loaded shared model + stats.")
        except Exception as e:
            print("❌ Load failed:", e)

    def autoreload(self):
        """Reload model from disk every 5 seconds"""
        while True:
            time.sleep(5)
            try:
                self.load()
            except:
                pass


shared = SharedModelRO()

# -----------------------------------------------------------------------------
# UDFs (SAME AS BEFORE, unchanged)
# -----------------------------------------------------------------------------

@pw.udf
def haversine_distance(lat1, lon1, lat2, lon2):
    try:
        d_lat = math.radians(lat2 - lat1)
        d_lon = math.radians(lon2 - lon1)
        a = (math.sin(d_lat/2)**2 +
             math.cos(math.radians(lat1)) *
             math.cos(math.radians(lat2)) *
             math.sin(d_lon/2)**2)
        return 6371 * 2 * math.asin(math.sqrt(a))
    except:
        return 0.0

@pw.udf
def extract_hour(unix_time):
    try:
        return datetime.fromtimestamp(unix_time).hour
    except:
        return 0

@pw.udf
def is_online_category(category):
    return 1 if category in ["shopping_net","misc_net","grocery_net"] else 0

@pw.udf
def is_late_night(hour):
    return 1 if 1 <= hour <= 5 else 0

@pw.udf
def calculate_std(arr):
    try:
        return float(arr.std()) if len(arr) > 1 else 0.0
    except:
        return 0.0

@pw.udf
def parse_and_filter_alert(json_str):
    try:
        d = json.loads(json_str)
        return json_str if d.get("is_alert") else None
    except:
        return None

# -----------------------------------------------------------------------------
# DETECTION LOGIC — READ-ONLY VERSION
# -----------------------------------------------------------------------------

@pw.udf
def detect(
    trans_num, cc_num, merchant, category, amt,
    lat, long, merch_lat, merch_long, unix_time,
    city, state, is_fraud, city_pop,
    avg_amt, std_amt, txn_n, fraud_hist,
    avg_dist, std_dist, merch_rate, merch_total,
    cat_rate, distance, hour, online, late):

    # Dedup check (shared with learner)
    if trans_num in shared.processed:
        return json.dumps({"is_alert": False, "duplicate": True})

    shared.stats["total"] = shared.stats.get("total", 0) + 1

    # Cold start
    if txn_n < 20:
        return json.dumps({"is_alert": False, "training": True})

    # --- Features ---
    z_amt = (amt - avg_amt) / std_amt if std_amt > 0 else 0
    amt_ratio = amt / avg_amt if avg_amt > 0 else 1
    z_dist = (distance - avg_dist) / std_dist if std_dist > 0 else 0

    feats = {
        "amt": float(amt), "z_amt": float(z_amt),
        "amt_ratio": float(amt_ratio),
        "dist": float(distance), "z_dist": float(z_dist),
        "hr": float(hour), "merch_risk": float(merch_rate),
        "cat_risk": float(cat_rate),
        "online": float(online), "late_night": float(late),
        "fraud_history": float(fraud_hist),
        "n": float(min(txn_n, 1000))
    }

    # --- ML scores ---
    try:
        m1 = shared.model_main.predict_proba_one(feats).get(1, 0)*100
    except: m1=0

    try:
        m2 = shared.model_validator.predict_proba_one(feats).get(1, 0)*100
    except: m2=0

    ml_score = (m1+m2)/2
    agree = abs(m1 - m2) < 20

    # --- RULES (same logic, trimmed for clarity) ---

    suspicious = False
    tier = 0
    reasons = []

    if z_amt > 4 or z_dist > 4 or fraud_hist >= 3:
        suspicious = True
        tier = 1
        reasons.append("EXTREME")
    elif ml_score > 85 and agree:
        suspicious = True
        tier = 2
        reasons.append(f"ML{ml_score:.0f}")

    if suspicious:
        shared.stats["alerts"] = shared.stats.get("alerts", 0) + 1
        return json.dumps({
            "is_alert": True,
            "trans_num": trans_num,
            "cc_num": int(cc_num),
            "merchant": merchant,
            "category": category,
            "amt": amt,
            "location": f"{city}, {state}",
            "tier": tier,
            "ml_score": ml_score,
            "reasons": "|".join(reasons)
        })

    return json.dumps({"is_alert": False})

# -----------------------------------------------------------------------------
# MAIN PIPELINE (READ-ONLY MODE)
# -----------------------------------------------------------------------------

def run_detector():
    print("══════════════════════════════════════════")
    print("          READ-ONLY FRAUD DETECTOR        ")
    print("   Uses shared model, no writes, no conflicts")
    print("══════════════════════════════════════════")

    # READ from NATS (read-only persistent id)
    transactions = pw.io.nats.read(
        uri=NATS_URI,
        topic=NATS_INPUT_TOPIC,
        schema=TransactionSchema,
        format="json",
        persistent_id="detector_readonly"   # <—— SAFE
    )

    enriched = transactions.select(
        *pw.this,
        hour=extract_hour(pw.this.unix_time),
        distance=haversine_distance(pw.this.lat, pw.this.long,
                                    pw.this.merch_lat, pw.this.merch_long),
        is_online=is_online_category(pw.this.category),
        is_late=is_late_night(extract_hour(pw.this.unix_time)),
    )

    # JOINS SAME AS BEFORE (you keep your join code)
    # -----------------------------------------------
    # (omitted here for brevity — use your original joins)
    # -----------------------------------------------

    # SCORING
    results = enriched.select(alert_json=detect(
        pw.this.trans_num, pw.this.cc_num, pw.this.merchant, pw.this.category,
        pw.this.amt, pw.this.lat, pw.this.long, pw.this.merch_lat, pw.this.merch_long,
        pw.this.unix_time, pw.this.city, pw.this.state, pw.this.is_fraud, pw.this.city_pop,
        pw.this.customer_avg_amt, pw.this.customer_std_amt, pw.this.customer_txn_count,
        pw.this.customer_fraud_history, pw.this.customer_avg_dist, pw.this.customer_std_dist,
        pw.this.merch_fraud_rate, pw.this.merch_total,
        pw.this.cat_fraud_rate,
        pw.this.distance, pw.this.hour, pw.this.is_online, pw.this.is_late
    ))

    # Publish all results
    pw.io.nats.write(results, uri=NATS_URI, topic=NATS_RESULTS_TOPIC)

    # Alerts only
    alerts = results.filter(
        parse_and_filter_alert(pw.this.alert_json).is_not_none()
    )
    pw.io.nats.write(alerts, uri=NATS_URI, topic=NATS_ALERTS_TOPIC)

    # ---------------------------------------------------------------------
    # *** READ-ONLY RUN (NO CHECKPOINT writing) ***
    # ---------------------------------------------------------------------
    pw.run(monitoring_level=pw.MonitoringLevel.NONE)
