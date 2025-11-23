# pipeline/detector/detector_ronly.py
"""
Inference-only detector. Reads fraud.transactions, loads shared model
periodically and reads aggregated stats from stats_store for features.
Publishes fraud.results and fraud.alerts.
"""
import pathway as pw
import math, json, time
from datetime import datetime
from pathlib import Path

from shared.schema import TransactionSchema
from shared.model_store import model_store
from shared import stats_store
from shared.config import (
    NATS_URI,
    NATS_INPUT_TOPIC as INPUT_TOPIC,
    NATS_RESULTS_TOPIC as RESULTS_TOPIC,
    NATS_ALERTS_TOPIC as ALERTS_TOPIC,
    DETECTOR_PERSIST_DIR as PERSIST_DIR,
    DETECTOR_CHECKPOINT_CONFIG as CHECKPOINT_CONFIG,
    FRAUD_HISTORY_THRESHOLD,
    Z_AMT_THRESHOLD,
    AMT_MIN_FOR_HUGE,
    ML_SCORE_HIGH,
    ML_SCORE_MEDIUM,
    ML_SCORE_LOW,
    CONFIDENCE_HIGH,
    CONFIDENCE_MEDIUM,
    CONFIDENCE_LOW
)

# Reader that reloads model periodically
class ModelReader:
    def __init__(self, reload_interval=20):
        self.model_main = None
        self.model_validator = None
        self.reload_interval = reload_interval
        self._last = 0
        self.processed = set()  # shared processed list is stored in file managed by feedback when saving processed, but keep local set too

        loaded = model_store.load()
        if loaded:
            self.model_main, self.model_validator = loaded
            print("🔄 [DETECTOR] model loaded at startup.")

    def reload(self, force=False):
        now = time.time()
        if not force and (now - self._last) < self.reload_interval:
            return
        loaded = model_store.load()
        if loaded:
            self.model_main, self.model_validator = loaded
            print(f"🔄 [DETECTOR] model reloaded @ {datetime.utcnow().isoformat()}")
        self._last = now

model_reader = ModelReader()

# UDFs
@pw.udf
def extract_hour(unix_time: int) -> int:
    try: return datetime.fromtimestamp(unix_time).hour
    except: return 0

@pw.udf
def haversine(lat1, lon1, lat2, lon2) -> float:
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

# inference UDF: loads reader.model_main/model_validator and uses stats_store for features
@pw.udf
def run_infer(trans_num, cc_num, amt, lat, long, merch_lat, merch_long, unix_time, merchant, category):
    # lazy reload
    try:
        model_reader.reload(force=False)
    except:
        pass

    # dedupe
    if trans_num in model_reader.processed:
        return json.dumps({"is_alert": False, "duplicate": True})
    model_reader.processed.add(trans_num)

    # compute hour & distance
    try:
        hour = datetime.fromtimestamp(int(unix_time)).hour
    except:
        hour = 0
    try:
        d_lat = math.radians(float(merch_lat) - float(lat))
        d_lon = math.radians(float(merch_long) - float(long))
        a = (math.sin(d_lat/2)**2 +
            math.cos(math.radians(float(lat))) * math.cos(math.radians(float(merch_lat))) * math.sin(d_lon/2)**2)
        distance = 6371 * 2 * math.asin(math.sqrt(a))
    except:
        distance = 0.0

    # read aggregated stats
    cust = stats_store.get_customer_profile(cc_num)
    merch = stats_store.get_merchant_profile(merchant)
    cat = stats_store.get_category_profile(category)

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

    # predict using loaded models
    try:
        p1 = model_reader.model_main.predict_proba_one(feats)
        p2 = model_reader.model_validator.predict_proba_one(feats)
        ml_score = (p1.get(1, 0.0) + p2.get(1, 0.0)) / 2.0 * 100.0
    except Exception:
        ml_score = 0.0

    # simple decision tiers (same as earlier logic simplified)
    is_alert = False
    tier = 0
    reasons = []
    confidence = 0

    # recreate a couple of bank rules that force alerts (mirrors original)
    if cust["fraud_history"] >= FRAUD_HISTORY_THRESHOLD:
        reasons.append(f"FRAUD_HISTORY({cust['fraud_history']})")
    if (cust["std_amt"] > 0) and ((float(amt) - cust["avg_amt"]) / (cust["std_amt"]) > Z_AMT_THRESHOLD) and float(amt) > AMT_MIN_FOR_HUGE:
        reasons.append("HUGE_AMT")

    if len(reasons) >= 2 or (len(reasons) >= 1 and ml_score >= ML_SCORE_HIGH):
        is_alert = True
        tier = 1
        confidence = CONFIDENCE_HIGH
    elif ml_score >= ML_SCORE_MEDIUM:
        is_alert = True
        tier = 3
        confidence = CONFIDENCE_MEDIUM
    elif ml_score >= ML_SCORE_LOW:
        is_alert = True
        tier = 2
        confidence = CONFIDENCE_LOW

    out = {
        "is_alert": is_alert,
        "trans_num": trans_num,
        "ml_score": round(ml_score, 1),
        "tier": tier,
        "confidence": confidence,
        "reasons": "|".join(reasons) if reasons else ""
    }
    return json.dumps(out)

# helper that extracts strict bool from JSON alert_json
@pw.udf
def extract_is_alert(alert_json: str) -> bool:
    if alert_json is None:
        return False
    try:
        data = json.loads(alert_json)
        return bool(data.get("is_alert", False))
    except:
        return False

def run_detector():
    print("═══════════════════════════════════════════")
    print("      FRAUD DETECTOR — MODEL READER ONLY   ")
    print("═══════════════════════════════════════════")
    print("Listening:", INPUT_TOPIC)
    print("Shared model file: pipeline/pathway_persistence/ml_models.pkl")
    print()

    tx = pw.io.nats.read(
        uri=NATS_URI,
        topic=INPUT_TOPIC,
        schema=TransactionSchema,
        format="json",
        persistent_id="detector_reader"
    )

    enriched = tx.select(
        *pw.this,
        hour=extract_hour(pw.this.unix_time),
        distance=haversine(pw.this.lat, pw.this.long, pw.this.merch_lat, pw.this.merch_long)
    )

    enriched = enriched.select(
        *pw.this,
        feats_json=pw.apply(
            lambda amt, dist, hr: json.dumps({"amt": float(amt), "dist": float(dist), "hr": int(hr)}),
            pw.this.amt, pw.this.distance, pw.this.hour
        )
    )

    results = enriched.select(
        alert_json=run_infer(
            pw.this.trans_num, pw.this.cc_num, pw.this.amt,
            pw.this.lat, pw.this.long, pw.this.merch_lat, pw.this.merch_long,
            pw.this.unix_time, pw.this.merchant, pw.this.category
        )
    )

    # publish results
    pw.io.nats.write(results, uri=NATS_URI, topic=RESULTS_TOPIC)

    # parse bool strictly
    parsed = results.select(
        alert_json=pw.this.alert_json,
        is_alert=extract_is_alert(pw.this.alert_json)
    )

    alerts = parsed.filter(pw.this.is_alert)  # strict bool filter (safe)

    pw.io.nats.write(
        alerts.select(alert_json=pw.this.alert_json),
        uri=NATS_URI, topic=ALERTS_TOPIC
    )

    pw.run(persistence_config=CHECKPOINT_CONFIG)
