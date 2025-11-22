# # pipeline/detector/detector_ronly.py
# """
# Inference-only fraud detector.
# Consumes fraud.transactions, loads shared model periodically,
# uses aggregated stats from stats_store, and emits fraud.results + fraud.alerts.
# """

# import pathway as pw
# import math, json, time
# from datetime import datetime
# from pathlib import Path

# # shared modules (assumed present and correct)
# from shared.model_store import model_store
# from shared import stats_store

# # ───────────────────────────────────────────────
# # LOCAL SCHEMA WITHOUT is_fraud
# # ───────────────────────────────────────────────
# class TransactionSchema(pw.Schema):
#     trans_num: str = pw.column_definition(dtype=str)
#     trans_date_trans_time: str = pw.column_definition(dtype=str)
#     cc_num: int = pw.column_definition(dtype=int)
#     merchant: str = pw.column_definition(dtype=str)
#     category: str = pw.column_definition(dtype=str)
#     amt: float = pw.column_definition(dtype=float)

#     first: str = pw.column_definition(dtype=str)
#     last: str = pw.column_definition(dtype=str)
#     gender: str = pw.column_definition(dtype=str)
#     street: str = pw.column_definition(dtype=str)
#     city: str = pw.column_definition(dtype=str)
#     state: str = pw.column_definition(dtype=str)
#     zip: int = pw.column_definition(dtype=int)

#     lat: float = pw.column_definition(dtype=float)
#     long: float = pw.column_definition(dtype=float)
#     city_pop: int = pw.column_definition(dtype=int)

#     job: str = pw.column_definition(dtype=str)
#     dob: str = pw.column_definition(dtype=str)
#     unix_time: int = pw.column_definition(dtype=int)

#     merch_lat: float = pw.column_definition(dtype=float)
#     merch_long: float = pw.column_definition(dtype=float)


# # ───────────────────────────────────────────────
# # CONFIG
# # ───────────────────────────────────────────────
# NATS_URI = "nats://localhost:4222"
# INPUT_TOPIC = "fraud.transactions"
# RESULTS_TOPIC = "fraud.results"
# ALERTS_TOPIC = "fraud.alerts"

# # IMPORTANT: run from project root so this path is stable
# PERSIST_DIR = Path("./pathway_persistence")

# CHECKPOINT_CONFIG = pw.persistence.Config.simple_config(
#     pw.persistence.Backend.filesystem(str(PERSIST_DIR / "checkpoints_detector")),
#     snapshot_interval_ms=10000
# )


# # ───────────────────────────────────────────────
# # MODEL READER
# # ───────────────────────────────────────────────
# class ModelReader:
#     def __init__(self, reload_interval=20):
#         self.model_main = None
#         self.model_validator = None
#         self.reload_interval = reload_interval
#         self._last = 0
#         # local seen-set only used for avoiding re-processing if you want; removed to ensure stats update
#         self.processed = set()

#         loaded = model_store.load()
#         if loaded:
#             self.model_main, self.model_validator = loaded
#             print("🔄 [DETECTOR] model loaded at startup.")

#     def reload(self, force=False):
#         now = time.time()
#         if not force and (now - self._last) < self.reload_interval:
#             return

#         loaded = model_store.load()
#         if loaded:
#             self.model_main, self.model_validator = loaded
#             print(f"🔄 [DETECTOR] model reloaded @ {datetime.utcnow().isoformat()}")
#         self._last = now


# model_reader = ModelReader()


# # ───────────────────────────────────────────────
# # UDF HELPERS
# # ───────────────────────────────────────────────
# @pw.udf
# def extract_hour(unix_time: int) -> int:
#     try:
#         return datetime.fromtimestamp(unix_time).hour
#     except:
#         return 0


# @pw.udf
# def haversine(lat1, lon1, lat2, lon2) -> float:
#     try:
#         d_lat = math.radians(lat2 - lat1)
#         d_lon = math.radians(lon2 - lon1)
#         a = (math.sin(d_lat / 2) ** 2 +
#              math.cos(math.radians(lat1)) *
#              math.cos(math.radians(lat2)) *
#              math.sin(d_lon / 2) ** 2)
#         return 6371 * 2 * math.asin(math.sqrt(a))
#     except:
#         return 0.0


# # ───────────────────────────────────────────────
# # INFERENCE UDF
# # ───────────────────────────────────────────────
# @pw.udf
# def run_infer(trans_num, cc_num, amt,
#               lat, long, merch_lat, merch_long,
#               unix_time, merchant, category):
#     """
#     Inference-only UDF.
#     Detector will:
#       - reload model periodically (read-only)
#       - compute features using shared stats_store
#       - update stats_store (detector is responsible for updating aggregates)
#       - run inference and return JSON
#     """

#     # lazy reload model (non-blocking)
#     try:
#         model_reader.reload(force=False)
#     except Exception:
#         pass

#     # compute hour
#     try:
#         hour = datetime.fromtimestamp(int(unix_time)).hour
#     except:
#         hour = 0

#     # compute distance
#     try:
#         d_lat = math.radians(float(merch_lat) - float(lat))
#         d_lon = math.radians(float(merch_long) - float(long))
#         a = (math.sin(d_lat / 2) ** 2 +
#              math.cos(math.radians(float(lat))) *
#              math.cos(math.radians(float(merch_lat))) *
#              math.sin(d_lon / 2) ** 2)
#         distance = 6371 * 2 * math.asin(math.sqrt(a))
#     except:
#         distance = 0.0

#     # Read aggregated stats (may return defaults if not present)
#     cust = stats_store.get_customer_profile(cc_num)
#     merch = stats_store.get_merchant_profile(merchant)
#     cat = stats_store.get_category_profile(category)

#     # Build feature vector (same as original detector)
#     feats = {
#         "amt": float(amt),
#         "z_amt": ((float(amt) - cust["avg_amt"]) / cust["std_amt"]) if cust["std_amt"] > 0 else 0.0,
#         "amt_ratio": float(amt) / cust["avg_amt"] if cust["avg_amt"] > 0 else 1.0,
#         "dist": float(distance),
#         "z_dist": (distance - cust["avg_dist"]) / cust["std_dist"] if cust["std_dist"] > 0 else 0.0,
#         "hr": float(hour),
#         "merch_risk": float(merch["fraud_rate"]),
#         "cat_risk": float(cat["fraud_rate"]),
#         "online": float(1 if category in ["shopping_net", "misc_net", "grocery_net"] else 0),
#         "late_night": float(1 if 1 <= hour <= 5 else 0),
#         "fraud_history": float(cust["fraud_history"]),
#         "n": float(min(cust["txn_count"], 1000)),
#     }


#     # Run inference (models may be None if not created yet)
#     try:
#         p1 = model_reader.model_main.predict_proba_one(feats) if model_reader.model_main is not None else {}
#         p2 = model_reader.model_validator.predict_proba_one(feats) if model_reader.model_validator is not None else {}
#         ml_score = (p1.get(1, 0.0) + p2.get(1, 0.0)) / 2 * 100.0
#     except Exception:
#         ml_score = 0.0

#     # rules + ML decision mirror (simplified)
#     reasons = []
#     tier = 0
#     confidence = 0
#     is_alert = False

#     if cust["fraud_history"] >= 3:
#         reasons.append(f"FRAUD_HISTORY({cust['fraud_history']})")

#     if (cust["std_amt"] > 0 and (float(amt) - cust["avg_amt"]) / cust["std_amt"] > 3.5 and float(amt) > 500):
#         reasons.append("HUGE_AMT")

#     if len(reasons) >= 2 or (len(reasons) >= 1 and ml_score >= 80):
#         is_alert = True
#         tier = 1
#         confidence = 90
#     elif ml_score >= 82:
#         is_alert = True
#         tier = 3
#         confidence = 75
#     elif ml_score >= 75:
#         is_alert = True
#         tier = 2
#         confidence = 70

#     # return JSON string
#     return json.dumps({
#         "is_alert": is_alert,
#         "trans_num": trans_num,
#         "ml_score": round(ml_score, 1),
#         "tier": tier,
#         "confidence": confidence,
#         "reasons": "|".join(reasons) if reasons else ""
#     })


# # strict bool extractor
# @pw.udf
# def extract_is_alert(alert_json: str) -> bool:
#     if alert_json is None:
#         return False
#     try:
#         return bool(json.loads(alert_json).get("is_alert", False))
#     except:
#         return False


# # ───────────────────────────────────────────────
# # MAIN
# # ───────────────────────────────────────────────
# def run_detector():
#     print("═══════════════════════════════════════════")
#     print("      FRAUD DETECTOR — MODEL READER ONLY   ")
#     print("═══════════════════════════════════════════")
#     print("Input:", INPUT_TOPIC)
#     print("Shared model file:", str(Path('./pathway_persistence') / "ml_models.pkl"))
#     print("───────────────────────────────────────────")

#     tx = pw.io.nats.read(
#         uri=NATS_URI,
#         topic=INPUT_TOPIC,
#         schema=TransactionSchema,
#         format="json",
#         persistent_id="detector_reader"
#     )

#     enriched = tx.select(
#         *pw.this,
#         hour=extract_hour(pw.this.unix_time),
#         distance=haversine(pw.this.lat, pw.this.long,
#                            pw.this.merch_lat, pw.this.merch_long)
#     )

#     results = enriched.select(
#         alert_json=run_infer(
#             pw.this.trans_num, pw.this.cc_num, pw.this.amt,
#             pw.this.lat, pw.this.long,
#             pw.this.merch_lat, pw.this.merch_long,
#             pw.this.unix_time, pw.this.merchant,
#             pw.this.category
#         )
#     )

#     # publish all results (debug)
#     pw.io.nats.write(results, uri=NATS_URI, topic=RESULTS_TOPIC)

#     # parse to strict bool column and filter
#     parsed = results.select(
#         alert_json=pw.this.alert_json,
#         is_alert=extract_is_alert(pw.this.alert_json)
#     )

#     alerts = parsed.filter(pw.this.is_alert)

#     # publish only alert JSON
#     pw.io.nats.write(
#         alerts.select(alert_json=pw.this.alert_json),
#         uri=NATS_URI, topic=ALERTS_TOPIC
#     )

#     # Run with checkpoint config
#     pw.run(persistence_config=CHECKPOINT_CONFIG)


# # If run as a script
# if __name__ == "__main__":
#     run_detector()

# pipeline/detector/detector_ronly_debug.py
"""
Inference-only fraud detector WITH DEBUG LOGGING.
Use this version to diagnose why no alerts are being generated.
"""

import pathway as pw
import math, json, time
from datetime import datetime
from pathlib import Path

# shared modules (assumed present and correct)
from shared import model_store
from shared import stats_store

# ───────────────────────────────────────────────
# LOCAL SCHEMA WITHOUT is_fraud
# ───────────────────────────────────────────────
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


# ───────────────────────────────────────────────
# CONFIG
# ───────────────────────────────────────────────
NATS_URI = "nats://localhost:4222"
INPUT_TOPIC = "fraud.transactions"
RESULTS_TOPIC = "fraud.results"
ALERTS_TOPIC = "fraud.alerts"

PERSIST_DIR = Path("./pathway_persistence")

CHECKPOINT_CONFIG = pw.persistence.Config.simple_config(
    pw.persistence.Backend.filesystem(str(PERSIST_DIR / "checkpoints_detector")),
    snapshot_interval_ms=10000
)

# Debug counter
debug_counter = {"total": 0, "alerts": 0}


# ───────────────────────────────────────────────
# MODEL READER
# ───────────────────────────────────────────────
class ModelReader:
    def __init__(self, reload_interval=20):
        self.model_main = None
        self.model_validator = None
        self.reload_interval = reload_interval
        self._last = 0

        loaded = model_store.load()
        if loaded:
            self.model_main, self.model_validator = loaded
            print("🔄 [DETECTOR] model loaded at startup.")
        else:
            print("⚠️  [DETECTOR] No model found at startup - will retry")

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


# ───────────────────────────────────────────────
# UDF HELPERS
# ───────────────────────────────────────────────
@pw.udf
def extract_hour(unix_time: int) -> int:
    try:
        return datetime.fromtimestamp(unix_time).hour
    except:
        return 0


@pw.udf
def haversine(lat1, lon1, lat2, lon2) -> float:
    try:
        d_lat = math.radians(lat2 - lat1)
        d_lon = math.radians(lon2 - lon1)
        a = (math.sin(d_lat / 2) ** 2 +
             math.cos(math.radians(lat1)) *
             math.cos(math.radians(lat2)) *
             math.sin(d_lon / 2) ** 2)
        return 6371 * 2 * math.asin(math.sqrt(a))
    except:
        return 0.0


# ───────────────────────────────────────────────
# INFERENCE UDF WITH DEBUG LOGGING
# ───────────────────────────────────────────────
@pw.udf
def run_infer(trans_num, cc_num, amt,
              lat, long, merch_lat, merch_long,
              unix_time, merchant, category):
    """
    Inference-only UDF WITH DEBUG LOGGING.
    """
    
    debug_counter["total"] += 1

    # lazy reload model
    try:
        model_reader.reload(force=False)
    except Exception as e:
        print(f"❌ Model reload error: {e}")

    # compute hour
    try:
        hour = datetime.fromtimestamp(int(unix_time)).hour
    except:
        hour = 0

    # compute distance
    try:
        d_lat = math.radians(float(merch_lat) - float(lat))
        d_lon = math.radians(float(merch_long) - float(long))
        a = (math.sin(d_lat / 2) ** 2 +
             math.cos(math.radians(float(lat))) *
             math.cos(math.radians(float(merch_lat))) *
             math.sin(d_lon / 2) ** 2)
        distance = 6371 * 2 * math.asin(math.sqrt(a))
    except:
        distance = 0.0

    # Read aggregated stats
    cust = stats_store.get_customer_profile(cc_num)
    merch = stats_store.get_merchant_profile(merchant)
    cat = stats_store.get_category_profile(category)

    # Build feature vector
    feats = {
        "amt": float(amt),
        "z_amt": ((float(amt) - cust["avg_amt"]) / cust["std_amt"]) if cust["std_amt"] > 0 else 0.0,
        "amt_ratio": float(amt) / cust["avg_amt"] if cust["avg_amt"] > 0 else 1.0,
        "dist": float(distance),
        "z_dist": (distance - cust["avg_dist"]) / cust["std_dist"] if cust["std_dist"] > 0 else 0.0,
        "hr": float(hour),
        "merch_risk": float(merch["fraud_rate"]),
        "cat_risk": float(cat["fraud_rate"]),
        "online": float(1 if category in ["shopping_net", "misc_net", "grocery_net"] else 0),
        "late_night": float(1 if 1 <= hour <= 5 else 0),
        "fraud_history": float(cust["fraud_history"]),
        "n": float(min(cust["txn_count"], 1000)),
    }

    # Run inference
    try:
        if model_reader.model_main is None or model_reader.model_validator is None:
            ml_score = 0.0
            model_status = "NO_MODELS"
        else:
            p1 = model_reader.model_main.predict_proba_one(feats)
            p2 = model_reader.model_validator.predict_proba_one(feats)
            ml_score = (p1.get(1, 0.0) + p2.get(1, 0.0)) / 2 * 100.0
            model_status = "OK"
    except Exception as e:
        ml_score = 0.0
        model_status = f"ERROR:{str(e)[:30]}"

    # rules + ML decision
    reasons = []
    tier = 0
    confidence = 0
    is_alert = False

    if cust["fraud_history"] >= 3:
        reasons.append(f"FRAUD_HISTORY({cust['fraud_history']})")

    if (cust["std_amt"] > 0 and (float(amt) - cust["avg_amt"]) / cust["std_amt"] > 3.5 and float(amt) > 500):
        reasons.append("HUGE_AMT")

    if len(reasons) >= 2 or (len(reasons) >= 1 and ml_score >= 80):
        is_alert = True
        tier = 1
        confidence = 90
    elif ml_score >= 82:
        is_alert = True
        tier = 3
        confidence = 75
    elif ml_score >= 75:
        is_alert = True
        tier = 2
        confidence = 70

    # DEBUG LOGGING (every 100th transaction or if alert)
    if debug_counter["total"] % 100 == 0 or is_alert:
        print(f"\n{'='*60}")
        print(f"Transaction #{debug_counter['total']}: {trans_num}")
        print(f"  CC: {cc_num} | Amt: ${amt:.2f} | Hour: {hour}")
        print(f"  Customer stats: txns={cust['txn_count']}, avg=${cust['avg_amt']:.2f}, fraud_hist={cust['fraud_history']}")
        print(f"  Features: z_amt={feats['z_amt']:.2f}, z_dist={feats['z_dist']:.2f}")
        print(f"  ML Score: {ml_score:.1f} ({model_status})")
        print(f"  Rules triggered: {reasons if reasons else 'None'}")
        print(f"  🚨 ALERT: {is_alert} (tier={tier}, conf={confidence})")
        print(f"{'='*60}\n")
    
    if is_alert:
        debug_counter["alerts"] += 1
        print(f"🚨 ALERT #{debug_counter['alerts']}: {trans_num} (score={ml_score:.1f}, tier={tier})")

    # return JSON string
    return json.dumps({
        "is_alert": is_alert,
        "trans_num": trans_num,
        "ml_score": round(ml_score, 1),
        "tier": tier,
        "confidence": confidence,
        "reasons": "|".join(reasons) if reasons else "",
        "model_status": model_status  # Added for debugging
    })


# strict bool extractor
@pw.udf
def extract_is_alert(alert_json: str) -> bool:
    if alert_json is None:
        return False
    try:
        return bool(json.loads(alert_json).get("is_alert", False))
    except:
        return False


# ───────────────────────────────────────────────
# MAIN
# ───────────────────────────────────────────────
def run_detector():
    print("═══════════════════════════════════════════")
    print("   FRAUD DETECTOR — DEBUG MODE   ")
    print("═══════════════════════════════════════════")
    print("Input:", INPUT_TOPIC)
    print("Shared model file:", str(Path('./pathway_persistence') / "ml_models.pkl"))
    print("Debug: Logging every 100th transaction + all alerts")
    print("───────────────────────────────────────────")

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
        distance=haversine(pw.this.lat, pw.this.long,
                           pw.this.merch_lat, pw.this.merch_long)
    )

    results = enriched.select(
        alert_json=run_infer(
            pw.this.trans_num, pw.this.cc_num, pw.this.amt,
            pw.this.lat, pw.this.long,
            pw.this.merch_lat, pw.this.merch_long,
            pw.this.unix_time, pw.this.merchant,
            pw.this.category
        )
    )

    # publish all results (debug)
    pw.io.nats.write(results, uri=NATS_URI, topic=RESULTS_TOPIC)

    # parse to strict bool column and filter
    parsed = results.select(
        alert_json=pw.this.alert_json,
        is_alert=extract_is_alert(pw.this.alert_json)
    )

    alerts = parsed.filter(pw.this.is_alert)

    # publish only alert JSON
    pw.io.nats.write(
        alerts.select(alert_json=pw.this.alert_json),
        uri=NATS_URI, topic=ALERTS_TOPIC
    )

    # Run with checkpoint config
    pw.run(persistence_config=CHECKPOINT_CONFIG)


if __name__ == "__main__":
    run_detector()