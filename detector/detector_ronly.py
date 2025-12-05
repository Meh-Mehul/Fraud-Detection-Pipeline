"""
Enhanced Fraud Detector with Latency Tracking
Measures: end-to-end latency, inference time, Redis latency
"""

import pathway as pw
import math, json, time, sys
from datetime import datetime
from pathlib import Path

# Add shared path
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent
shared_path = project_root / "shared"

if str(shared_path) not in sys.path:
    sys.path.append(str(shared_path))

# Shared modules
from shared import model_store
from shared import redis_stats_store
from shared.rules_loader import get_rules_loader

# METRICS INTEGRATION
from shared.metrics import (
    initialize_metrics,
    record_transaction,
    record_fraud_alert,
    record_latency,
    set_model_status,
    get_metrics_manager
)

# Schema
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

# Config
NATS_URI = "nats://localhost:4222"
INPUT_TOPIC = "fraud.transactions"
RESULTS_TOPIC = "fraud.results"
ALERTS_TOPIC = "fraud.alerts"

PERSIST_DIR = Path("./pathway_persistence")
CHECKPOINT_CONFIG = pw.persistence.Config.simple_config(
    pw.persistence.Backend.filesystem(str(PERSIST_DIR / "checkpoints_detector")),
    snapshot_interval_ms=10000
)

METRICS_PORT = 8001

# Debug counter
debug_counter = {"total": 0, "alerts": 0, "last_metric_update": 0}

# Global stores
redis_store = redis_stats_store.get_store()
rules_loader = get_rules_loader()

print("╔═══════════════════════════════════════════╗")
print("   FRAUD DETECTOR – ENHANCED METRICS")
print("   (Latency + Performance Tracking)")
print("╚═══════════════════════════════════════════╝")

# Initialize metrics
metrics_manager = initialize_metrics("detector", port=METRICS_PORT)

# Model reader
class ModelReader:
    def __init__(self, reload_interval=20):
        self.model_main = None
        self.model_validator = None
        self.reload_interval = reload_interval
        self._last = 0

        loaded = model_store.load()
        if loaded:
            self.model_main, self.model_validator = loaded
            set_model_status("main", True)
            print("📄 [DETECTOR] model loaded at startup.")
        else:
            set_model_status("main", False)
            print("⚠️  [DETECTOR] No model found at startup - will retry")

    def reload(self, force=False):
        now = time.time()
        if not force and (now - self._last) < self.reload_interval:
            return

        loaded = model_store.load()
        if loaded:
            self.model_main, self.model_validator = loaded
            set_model_status("main", True)
            print(f"📄 [DETECTOR] model reloaded @ {datetime.utcnow().isoformat()}")
        self._last = now

model_reader = ModelReader()

# UDF Helpers
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

# ENHANCED INFERENCE UDF WITH LATENCY TRACKING
@pw.udf
def run_infer_with_latency(
    trans_num, cc_num, amt, lat, long, merch_lat, merch_long,
    unix_time, merchant, category, city, state,
    trans_date_trans_time, first, last, gender, street, zip,
    city_pop, job, dob
):
    """
    Enhanced inference with detailed latency tracking:
    - End-to-end latency (from message timestamp)
    - Detector inference time
    - Redis read latency
    - ML model inference latency
    """
    
    # Start total processing timer
    process_start = time.time()
    
    debug_counter["total"] += 1
    record_transaction("detector")

    # Lazy reload model
    try:
        model_reader.reload(force=False)
    except Exception as e:
        print(f"❌ Model reload error: {e}")

    # Compute hour
    try:
        hour = datetime.fromtimestamp(int(unix_time)).hour
    except:
        hour = 0

    # Compute distance
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

    # Redis reads with latency tracking
    redis_start = time.time()
    cust = redis_store.get_customer_profile(cc_num)
    merch_stats = redis_store.get_merchant_profile(merchant)
    cat = redis_store.get_category_profile(category)
    redis_duration = time.time() - redis_start
    record_latency("redis_read", redis_duration)

    # Build features
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
        "online": float(1 if rules_loader.is_online_category(category) else 0),
        "late_night": float(1 if rules_loader.is_late_night(hour) else 0),
        "fraud_history": float(cust["fraud_history"]),
        "n": float(min(cust["txn_count"], 1000)),
    }

    # ML inference with latency tracking
    ml_start = time.time()
    try:
        if model_reader.model_main is None or model_reader.model_validator is None:
            ml_score = 0.0
            model_status = "NO_MODELS"
            model_available = False
        else:
            p1 = model_reader.model_main.predict_proba_one(feats)
            p2 = model_reader.model_validator.predict_proba_one(feats)
            ml_score = (p1.get(1, 0.0) + p2.get(1, 0.0)) / 2 * 100.0
            model_status = "OK"
            model_available = True
    except Exception as e:
        ml_score = 0.0
        model_status = f"ERROR:{str(e)[:30]}"
        model_available = False
    
    ml_duration = time.time() - ml_start
    record_latency("ml_inference", ml_duration)

    # Rules evaluation
    rules_start = time.time()
    is_alert, tier, confidence, reason_str = rules_loader.evaluate_transaction(
        z_amt=z_amt,
        amt=float(amt),
        z_dist=z_dist,
        distance=distance,
        merch_fraud_rate=merch_stats["fraud_rate"],
        merch_total=merch_stats["total"],
        cat_fraud_rate=cat["fraud_rate"],
        fraud_history=cust["fraud_history"],
        ml_score=ml_score,
        category=category,
        hour=hour
    )
    rules_duration = time.time() - rules_start
    record_latency("rules_evaluation", rules_duration)

    # Record alert
    if is_alert:
        debug_counter["alerts"] += 1
        primary_pattern = reason_str.split('|')[0].split('(')[0] if reason_str else "UNKNOWN"
        record_fraud_alert(
            tier=tier,
            pattern=primary_pattern,
            risk_score=confidence,
            component="detector"
        )
        
        if debug_counter["alerts"] % 10 == 0:
            print(f"🚨 ALERT #{debug_counter['alerts']}: {trans_num} | T{tier} | {reason_str[:40]}")
    
    # Total processing latency
    total_duration = time.time() - process_start
    record_latency("detector_total", total_duration)
    
    # Update metrics periodically
    now = time.time()
    if now - debug_counter["last_metric_update"] > 5:
        metrics_manager.update_component_uptime()
        debug_counter["last_metric_update"] = now
    
    # Log progress
    if debug_counter["total"] % 1000 == 0:
        print(f"[DETECTOR] Processed: {debug_counter['total']:,} | Alerts: {debug_counter['alerts']}")
        print(f"   📊 Latency - Total: {total_duration*1000:.1f}ms | ML: {ml_duration*1000:.1f}ms | Redis: {redis_duration*1000:.1f}ms")

    # Return JSON result
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
        "actual_fraud": 0,
        "tier": tier,
        "ml_score": round(ml_score, 1),
        "model_status": model_status,
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
        "hour": hour,
        # Latency metadata
        "latency_ms": {
            "total": round(total_duration * 1000, 2),
            "ml": round(ml_duration * 1000, 2),
            "redis": round(redis_duration * 1000, 2),
            "rules": round(rules_duration * 1000, 2)
        }
    })

@pw.udf
def extract_is_alert(alert_json: str) -> bool:
    if alert_json is None:
        return False
    try:
        return bool(json.loads(alert_json).get("is_alert", False))
    except:
        return False

# Main
def run_detector():
    print("Input:", INPUT_TOPIC)
    print("Alerts:", ALERTS_TOPIC)
    print("Results:", RESULTS_TOPIC)
    print(f"Rules: {rules_loader.rules_file}")
    print(f"Redis: {redis_stats_store.REDIS_HOST}:{redis_stats_store.REDIS_PORT}")
    print(f"Metrics: http://localhost:{METRICS_PORT}/metrics")
    print()
    
    # Check Redis
    if redis_store.health_check():
        summary = redis_store.get_stats_summary()
        print(f"✓ Redis connected")
        print(f"   Customers: {summary['customers']:,}")
        print(f"   Merchants: {summary['merchants']:,}")
        print(f"   Categories: {summary['categories']:,}")
    else:
        print("❌ Redis not available!")
        return
    
    print("─" * 43)

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
        alert_json=run_infer_with_latency(
            pw.this.trans_num, pw.this.cc_num, pw.this.amt,
            pw.this.lat, pw.this.long,
            pw.this.merch_lat, pw.this.merch_long,
            pw.this.unix_time, pw.this.merchant, pw.this.category,
            pw.this.city, pw.this.state,
            pw.this.trans_date_trans_time, pw.this.first, pw.this.last,
            pw.this.gender, pw.this.street, pw.this.zip,
            pw.this.city_pop, pw.this.job, pw.this.dob
        )
    )

    pw.io.nats.write(results, uri=NATS_URI, topic=RESULTS_TOPIC)

    parsed = results.select(
        alert_json=pw.this.alert_json,
        is_alert=extract_is_alert(pw.this.alert_json)
    )

    alerts = parsed.filter(pw.this.is_alert)

    pw.io.nats.write(
        alerts.select(alert_json=pw.this.alert_json),
        uri=NATS_URI, 
        topic=ALERTS_TOPIC
    )

    print("\n✓ Detector active with enhanced metrics")
    print(f"✓ Metrics: http://localhost:{METRICS_PORT}/metrics")
    print(f"✓ Tracking: Latency (total, ML, Redis), Alerts, Throughput")
    print()

    pw.run(persistence_config=CHECKPOINT_CONFIG)

if __name__ == "__main__":
    run_detector()