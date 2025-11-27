"""
Inference-only fraud detector with FULL transaction data in alerts.
Compatible with the report generator.
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
# INFERENCE UDF WITH FULL TRANSACTION DATA
# ───────────────────────────────────────────────
@pw.udf
def run_infer_with_full_data(
    trans_num, cc_num, amt, lat, long, merch_lat, merch_long,
    unix_time, merchant, category, city, state,
    # Additional fields for complete output
    trans_date_trans_time, first, last, gender, street, zip,
    city_pop, job, dob
):
    """
    Inference UDF that returns FULL transaction data in alerts.
    This matches the format expected by the report generator.
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
    merch_stats = stats_store.get_merchant_profile(merchant)
    cat = stats_store.get_category_profile(category)

    # Build feature vector
    z_amt = ((float(amt) - cust["avg_amt"]) / cust["std_amt"]) if cust["std_amt"] > 0 else 0.0
    z_dist = (distance - cust["avg_dist"]) / cust["std_dist"] if cust["std_dist"] > 0 else 0.0
    
    feats = {
        "amt": float(amt),
        "z_amt": z_amt,
        "amt_ratio": float(amt) / cust["avg_amt"] if cust["avg_amt"] > 0 else 1.0,
        "dist": float(distance),
        "z_dist": z_dist,
        "hr": float(hour),
        "merch_risk": float(merch_stats["fraud_rate"]),
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

    # Rules + ML decision (matching old detector logic)
    reasons = []
    tier = 0
    confidence = 0
    is_alert = False

    # TIER 1 - Critical signals
    extreme_signals = []
    
    if z_amt > 4.5:
        extreme_signals.append(f"MASSIVE_AMT(Z={z_amt:.1f})")
    elif z_amt > 3.8 and float(amt) > 500:
        extreme_signals.append(f"HUGE_AMT(Z={z_amt:.1f},${amt:.0f})")
    
    if z_dist > 4:
        extreme_signals.append(f"EXTREME_DIST(Z={z_dist:.1f})")
    elif z_dist > 3.2 and distance > 100:
        extreme_signals.append(f"VERY_FAR({distance:.0f}km)")
    
    if merch_stats["fraud_rate"] > 0.4 and merch_stats["total"] > 50:
        extreme_signals.append(f"FRAUD_MERCHANT({merch_stats['fraud_rate']*100:.0f}%)")
    
    if cust["fraud_history"] >= 3:
        extreme_signals.append(f"FRAUD_HISTORY({cust['fraud_history']})")
    
    if len(extreme_signals) >= 2:
        is_alert = True
        tier = 1
        confidence = 95
        reasons = extreme_signals[:3]
    elif len(extreme_signals) >= 1 and ml_score >= 80:
        is_alert = True
        tier = 1
        confidence = 90
        reasons = extreme_signals + [f"ML{ml_score:.0f}"]
    
    # TIER 2 - Score-based
    if not is_alert:
        tier2_score = 0
        tier2_reasons = []
        
        if z_amt > 3.5:
            tier2_score += 40
            tier2_reasons.append(f"VeryHighAmt(Z={z_amt:.1f})")
        elif z_amt > 3:
            tier2_score += 30
            tier2_reasons.append(f"HighAmt(Z={z_amt:.1f})")
        
        if z_dist > 3.5:
            tier2_score += 35
            tier2_reasons.append(f"VeryFar(Z={z_dist:.1f})")
        elif z_dist > 3:
            tier2_score += 25
            tier2_reasons.append(f"Far(Z={z_dist:.1f})")
        
        if merch_stats["fraud_rate"] > 0.3 and merch_stats["total"] > 40:
            tier2_score += 35
            tier2_reasons.append(f"RiskyMerch({merch_stats['fraud_rate']*100:.0f}%)")
        
        is_online = 1 if category in ["shopping_net", "misc_net", "grocery_net"] else 0
        is_late = 1 if 1 <= hour <= 5 else 0
        
        if is_online and is_late and float(amt) > 400:
            tier2_score += 25
            tier2_reasons.append(f"LateOnline")
        
        if cust["fraud_history"] >= 2:
            tier2_score += 30
            tier2_reasons.append(f"PrevFraud({cust['fraud_history']})")
        
        if z_amt > 2.5 and z_dist > 2.5:
            tier2_score += 25
            tier2_reasons.append("Amt+Dist")
        
        if ml_score >= 80:
            tier2_score += 25
            tier2_reasons.append(f"ML{ml_score:.0f}")
        elif ml_score >= 70:
            tier2_score += 15
        
        if tier2_score >= 75:
            is_alert = True
            tier = 2
            confidence = 80
            reasons = tier2_reasons[:4]
    
    # TIER 3 - ML-based
    if not is_alert:
        if ml_score >= 82:
            support_count = sum([
                z_amt > 2, z_dist > 2, merch_stats["fraud_rate"] > 0.15,
                cat["fraud_rate"] > 0.1, cust["fraud_history"] >= 1
            ])
            
            if support_count >= 2:
                is_alert = True
                tier = 3
                confidence = 75
                reasons.append(f"ML{ml_score:.0f}")
                if z_amt > 2.5: reasons.append(f"HighAmt")
                if z_dist > 2.5: reasons.append(f"FarLoc")
    
    reason_str = "|".join(reasons[:4]) if reasons else ""

    # Log alerts
    if is_alert:
        debug_counter["alerts"] += 1
        if debug_counter["alerts"] % 10 == 0:
            print(f"🚨 ALERT #{debug_counter['alerts']}: {trans_num} | T{tier} | {reason_str[:40]}")
    
    # Log progress
    if debug_counter["total"] % 1000 == 0:
        print(f"[DETECTOR] Processed: {debug_counter['total']:,} | Alerts: {debug_counter['alerts']}")

    # Return JSON with FULL transaction data (matching old detector format)
    return json.dumps({
        "is_alert": is_alert,
        "trans_num": str(trans_num),
        "cc_num": int(cc_num),
        "merchant": merchant,
        "category": category,
        "amt": float(amt),
        "location": f"{city}, {state}",
        "city": city,
        "state": state,
        "risk_score": confidence,
        "reasons": reason_str,
        "confidence": confidence,
        "actual_fraud": 0,  # Inference mode - we don't know actual fraud status
        "tier": tier,
        "ml_score": round(ml_score, 1),
        "model_status": model_status,
        # Additional fields for comprehensive reporting
        "trans_date_trans_time": trans_date_trans_time,
        "first": first,
        "last": last,
        "gender": gender,
        "street": street,
        "zip": int(zip),
        "lat": float(lat),
        "long": float(long),
        "city_pop": int(city_pop),
        "job": job,
        "dob": dob,
        "merch_lat": float(merch_lat),
        "merch_long": float(merch_long),
        "unix_time": int(unix_time),
        "distance": round(distance, 2),
        "hour": hour
    })


# Extract is_alert as boolean
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
    print("   FRAUD DETECTOR — INFERENCE MODE")
    print("   (Report Generator Compatible)")
    print("═══════════════════════════════════════════")
    print("Input:", INPUT_TOPIC)
    print("Alerts (full data):", ALERTS_TOPIC)
    print("Results (debug):", RESULTS_TOPIC)
    print("Model file:", str(Path('./pathway_persistence') / "ml_models.pkl"))
    print("───────────────────────────────────────────")

    tx = pw.io.nats.read(
        uri=NATS_URI,
        topic=INPUT_TOPIC,
        schema=TransactionSchema,
        format="json",
        persistent_id="detector_reader"
    )

    # Add computed fields
    enriched = tx.select(
        *pw.this,
        hour=extract_hour(pw.this.unix_time),
        distance=haversine(pw.this.lat, pw.this.long,
                           pw.this.merch_lat, pw.this.merch_long)
    )

    # Run inference with ALL transaction fields
    results = enriched.select(
        alert_json=run_infer_with_full_data(
            pw.this.trans_num, pw.this.cc_num, pw.this.amt,
            pw.this.lat, pw.this.long,
            pw.this.merch_lat, pw.this.merch_long,
            pw.this.unix_time, pw.this.merchant, pw.this.category,
            pw.this.city, pw.this.state,
            # Pass additional fields
            pw.this.trans_date_trans_time, pw.this.first, pw.this.last,
            pw.this.gender, pw.this.street, pw.this.zip,
            pw.this.city_pop, pw.this.job, pw.this.dob
        )
    )

    # Publish all results for debugging
    pw.io.nats.write(results, uri=NATS_URI, topic=RESULTS_TOPIC)

    # Filter to alerts only
    parsed = results.select(
        alert_json=pw.this.alert_json,
        is_alert=extract_is_alert(pw.this.alert_json)
    )

    alerts = parsed.filter(pw.this.is_alert)

    # Publish alerts with FULL data
    pw.io.nats.write(
        alerts.select(alert_json=pw.this.alert_json),
        uri=NATS_URI, 
        topic=ALERTS_TOPIC
    )

    print("\n✓ Detector active - publishing full transaction data in alerts")
    print("✓ Report generator will receive complete information")
    print()

    # Run with checkpoint config
    pw.run(persistence_config=CHECKPOINT_CONFIG)


if __name__ == "__main__":
    run_detector()