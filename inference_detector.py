"""
Inference-Only Fraud Detector - FIXED
Properly publishes individual transactions to NATS for dashboard
"""

import pathway as pw
import json
import pickle
import math
import threading
import time
import asyncio
from datetime import datetime
from pathlib import Path
from river import tree, preprocessing, compose
import nats

# ============================================================================
# CONFIGURATION
# ============================================================================

NATS_URI = "nats://localhost:4222"
NATS_INPUT_TOPIC = "fraud.test_transactions"
NATS_INFERENCE_RESULTS = "fraud.inference_results"

PERSIST = Path("pathway_persistence")
MODEL_PATH = PERSIST / "ml_models.pkl"
STATS_PATH = PERSIST / "stats.json"

# ============================================================================
# NATS PUBLISHER (for individual messages)
# ============================================================================

class NATSPublisher:
    """Publishes individual transaction results to NATS"""
    
    def __init__(self):
        self.nc = None
        self.loop = None
        self.thread = None
        self.message_queue = []
        self.lock = threading.Lock()
        self.start()
    
    def start(self):
        """Start NATS publisher in background thread"""
        self.thread = threading.Thread(target=self._run_async_loop, daemon=True)
        self.thread.start()
        time.sleep(1)  # Give it time to connect
    
    def _run_async_loop(self):
        """Run asyncio event loop in thread"""
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(self._connect())
        self.loop.run_until_complete(self._publish_loop())
    
    async def _connect(self):
        """Connect to NATS"""
        try:
            self.nc = await nats.connect(NATS_URI)
            print(f"✓ NATS publisher connected to {NATS_URI}")
        except Exception as e:
            print(f"❌ NATS connection failed: {e}")
    
    async def _publish_loop(self):
        """Continuously publish queued messages"""
        while True:
            try:
                if self.message_queue:
                    with self.lock:
                        messages = self.message_queue.copy()
                        self.message_queue.clear()
                    
                    for msg in messages:
                        await self.nc.publish(NATS_INFERENCE_RESULTS, msg.encode())
                
                await asyncio.sleep(0.1)
            except Exception as e:
                print(f"Publish error: {e}")
    
    def publish(self, message_dict):
        """Queue a message for publishing"""
        with self.lock:
            self.message_queue.append(json.dumps(message_dict))

# Global publisher
nats_publisher = NATSPublisher()

# ============================================================================
# SCHEMA (WITHOUT is_fraud label)
# ============================================================================

class UnlabeledTransactionSchema(pw.Schema):
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

# ============================================================================
# SHARED MODEL LOADER
# ============================================================================

class SharedModelReader:
    """Loads trained model from shared storage"""
    
    def __init__(self):
        self.load()
        t = threading.Thread(target=self.autoreload, daemon=True)
        t.start()
    
    def load(self):
        try:
            if MODEL_PATH.exists():
                with open(MODEL_PATH, "rb") as f:
                    saved = pickle.load(f)
                    self.model_main = saved["model_main"]
                    self.model_validator = saved["model_validator"]
                print("✓ Loaded trained model from shared storage")
            else:
                print("⚠️ No trained model found - using default")
                self.model_main = compose.Pipeline(
                    preprocessing.StandardScaler(),
                    tree.HoeffdingAdaptiveTreeClassifier(
                        grace_period=200, delta=1e-5, seed=42))
                self.model_validator = tree.HoeffdingAdaptiveTreeClassifier(
                    grace_period=150, delta=1e-4, seed=123)
            
            if STATS_PATH.exists():
                self.stats = json.load(open(STATS_PATH))
            else:
                self.stats = {"total": 0}
        except Exception as e:
            print(f"❌ Model load failed: {e}")
    
    def autoreload(self):
        while True:
            time.sleep(10)
            try:
                old_total = self.stats.get('total', 0)
                self.load()
                new_total = self.stats.get('total', 0)
                if new_total > old_total:
                    print(f"🔄 Model updated (trained on {new_total:,} transactions)")
            except:
                pass

shared_model = SharedModelReader()

# ============================================================================
# HELPER UDFs
# ============================================================================

@pw.udf
def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
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
def return_zero(x):
    return 0

# ============================================================================
# INFERENCE + PUBLISHING
# ============================================================================

@pw.udf
def inference_and_publish(
    trans_num, cc_num, merchant, category, amt,
    lat, long, merch_lat, merch_long, unix_time,
    city, state, city_pop,
    avg_amt, std_amt, txn_n, fraud_hist,
    avg_dist, std_dist, merch_rate, merch_total,
    cat_rate, distance, hour, online, late,
    first, last, trans_date_trans_time):
    """
    Performs inference AND publishes to NATS directly
    Returns a simple status string
    """
    
    try:
        # Feature engineering
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
        
        # ML prediction
        try:
            m1 = shared_model.model_main.predict_proba_one(feats).get(1, 0)*100
        except: 
            m1 = 0
        
        try:
            m2 = shared_model.model_validator.predict_proba_one(feats).get(1, 0)*100
        except: 
            m2 = 0
        
        ml_score = (m1 + m2) / 2
        agree = abs(m1 - m2) < 20
        
        # Rule-based detection
        suspicious = False
        tier = 0
        reasons = []
        
        # TIER 1: Extreme signals
        extreme_signals = []
        if z_amt > 4.5:
            extreme_signals.append(f"MASSIVE_AMT(Z={z_amt:.1f})")
        elif z_amt > 3.8 and amt > 500:
            extreme_signals.append(f"HUGE_AMT(Z={z_amt:.1f})")
        
        if z_dist > 4.0:
            extreme_signals.append(f"EXTREME_DIST(Z={z_dist:.1f})")
        elif z_dist > 3.5 and distance > 100:
            extreme_signals.append(f"VERY_FAR({distance:.0f}km)")
        
        if merch_rate > 0.4 and merch_total > 50:
            extreme_signals.append(f"FRAUD_MERCHANT({merch_rate*100:.0f}%)")
        
        if fraud_hist >= 3:
            extreme_signals.append(f"FRAUD_HISTORY({fraud_hist})")
        
        if len(extreme_signals) >= 2:
            suspicious = True
            tier = 1
            reasons = extreme_signals[:3]
        elif len(extreme_signals) >= 1 and ml_score >= 80 and agree:
            suspicious = True
            tier = 1
            reasons = extreme_signals + [f"ML{ml_score:.0f}"]
        
        # TIER 2: Score-based
        if not suspicious:
            score = 0
            tier2_reasons = []
            
            if z_amt > 3.5:
                score += 40
                tier2_reasons.append(f"VeryHighAmt(Z={z_amt:.1f})")
            elif z_amt > 3.0:
                score += 30
                tier2_reasons.append(f"HighAmt(Z={z_amt:.1f})")
            
            if z_dist > 3.5:
                score += 35
                tier2_reasons.append(f"VeryFar(Z={z_dist:.1f})")
            
            if merch_rate > 0.3 and merch_total > 40:
                score += 35
                tier2_reasons.append(f"RiskyMerch({merch_rate*100:.0f}%)")
            
            if online and late and amt > 400:
                score += 25
                tier2_reasons.append("LateOnline")
            
            if fraud_hist >= 2:
                score += 30
                tier2_reasons.append(f"PrevFraud({fraud_hist})")
            
            if ml_score >= 80 and agree:
                score += 25
                tier2_reasons.append(f"ML{ml_score:.0f}")
            elif ml_score >= 70:
                score += 15
            
            if score >= 75:
                suspicious = True
                tier = 2
                reasons = tier2_reasons[:4]
        
        # TIER 3: ML-based
        if not suspicious and ml_score >= 82 and agree:
            support = 0
            tier3_reasons = [f"ML{ml_score:.0f}"]
            
            if z_amt > 2.0:
                support += 1
            if z_dist > 2.0:
                support += 1
            if merch_rate > 0.15:
                support += 1
            if cat_rate > 0.1:
                support += 1
            if fraud_hist >= 1:
                support += 1
            
            if support >= 2:
                suspicious = True
                tier = 3
                reasons = tier3_reasons
        
        # Publish to NATS if suspicious
        if suspicious:
            confidence = 95 if tier == 1 else (80 if tier == 2 else 75)
            
            result = {
                "requires_review": True,
                "trans_num": trans_num,
                "cc_num": int(cc_num),
                "customer_name": f"{first} {last}",
                "merchant": merchant,
                "category": category,
                "amt": float(amt),
                "location": f"{city}, {state}",
                "trans_date": trans_date_trans_time,
                "tier": tier,
                "ml_score": round(ml_score, 1),
                "confidence": confidence,
                "reasons": "|".join(reasons),
                "prediction": "FRAUD",
                "status": "PENDING_REVIEW",
                "reviewed": False,
                "feedback_label": None
            }
            
            # Publish directly to NATS
            nats_publisher.publish(result)
            
            return f"FLAGGED_TIER{tier}"
        
        return "LEGITIMATE"
        
    except Exception as e:
        print(f"[ERROR] Inference failed: {e}")
        import traceback
        traceback.print_exc()
        return f"ERROR: {str(e)}"

# ============================================================================
# MAIN PIPELINE
# ============================================================================

def run_inference_detector():
    print("═══════════════════════════════════════════════════════════")
    print("   INFERENCE DETECTOR (Fixed NATS Publishing)")
    print("═══════════════════════════════════════════════════════════")
    print()
    
    # Read unlabeled test transactions
    transactions = pw.io.nats.read(
        uri=NATS_URI,
        topic=NATS_INPUT_TOPIC,
        schema=UnlabeledTransactionSchema,
        format="json",
        name="inference_detector"
    )
    
    print(f"✓ Reading from: {NATS_INPUT_TOPIC}")
    print(f"✓ Publishing to: {NATS_INFERENCE_RESULTS}")
    print(f"✓ Using model: {MODEL_PATH}")
    print()
    
    # Enrich with computed fields
    enriched = transactions.select(
        *pw.this,
        hour=extract_hour(pw.this.unix_time),
        distance=haversine_distance(pw.this.lat, pw.this.long,
                                    pw.this.merch_lat, pw.this.merch_long),
        is_online=is_online_category(pw.this.category),
        is_late=is_late_night(extract_hour(pw.this.unix_time))
    )
    
    # Build statistics
    merchant_stats = enriched.groupby(pw.this.merchant).reduce(
        merchant=pw.this.merchant,
        total=pw.reducers.count()
    ).select(
        merchant=pw.this.merchant,
        total=pw.this.total,
        fraud_rate=pw.apply(lambda t: 0.1, pw.this.total)
    )
    
    category_stats = enriched.groupby(pw.this.category).reduce(
        category=pw.this.category,
        total=pw.reducers.count()
    ).select(
        category=pw.this.category,
        fraud_rate=pw.apply(lambda t: 0.05, pw.this.total)
    )
    
    customer_stats = enriched.groupby(pw.this.cc_num).reduce(
        cc_num=pw.this.cc_num,
        txn_count=pw.reducers.count(),
        avg_amt=pw.reducers.avg(pw.this.amt),
        amt_array=pw.reducers.ndarray(pw.this.amt),
        avg_dist=pw.reducers.avg(pw.this.distance),
        dist_array=pw.reducers.ndarray(pw.this.distance)
    ).select(
        cc_num=pw.this.cc_num,
        txn_count=pw.this.txn_count,
        avg_amt=pw.this.avg_amt,
        std_amt=calculate_std(pw.this.amt_array),
        avg_dist=pw.this.avg_dist,
        std_dist=calculate_std(pw.this.dist_array),
        fraud_history=return_zero(pw.this.cc_num)
    )
    
    # Join all statistics
    enriched = enriched.join_left(
        merchant_stats,
        enriched.merchant == merchant_stats.merchant
    ).select(
        *pw.left,
        merch_fraud_rate=pw.require(pw.right.fraud_rate, pw.right.fraud_rate, 0.1),
        merch_total=pw.require(pw.right.total, pw.right.total, 0)
    )
    
    enriched = enriched.join_left(
        category_stats,
        enriched.category == category_stats.category
    ).select(
        *pw.left,
        cat_fraud_rate=pw.require(pw.right.fraud_rate, pw.right.fraud_rate, 0.05)
    )
    
    enriched = enriched.join_left(
        customer_stats,
        enriched.cc_num == customer_stats.cc_num
    ).select(
        *pw.left,
        customer_avg_amt=pw.require(pw.right.avg_amt, pw.right.avg_amt, pw.left.amt),
        customer_std_amt=pw.require(pw.right.std_amt, pw.right.std_amt, 0.0),
        customer_txn_count=pw.require(pw.right.txn_count, pw.right.txn_count, 1),
        customer_fraud_history=pw.require(pw.right.fraud_history, pw.right.fraud_history, 0),
        customer_avg_dist=pw.require(pw.right.avg_dist, pw.right.avg_dist, 0.0),
        customer_std_dist=pw.require(pw.right.std_dist, pw.right.std_dist, 0.0)
    )
    
    # Run inference (publishes directly to NATS inside UDF)
    results = enriched.select(
        trans_num=pw.this.trans_num,
        status=inference_and_publish(
            pw.this.trans_num, pw.this.cc_num, pw.this.merchant, pw.this.category,
            pw.this.amt, pw.this.lat, pw.this.long, pw.this.merch_lat, pw.this.merch_long,
            pw.this.unix_time, pw.this.city, pw.this.state, pw.this.city_pop,
            pw.this.customer_avg_amt, pw.this.customer_std_amt, pw.this.customer_txn_count,
            pw.this.customer_fraud_history, pw.this.customer_avg_dist, pw.this.customer_std_dist,
            pw.this.merch_fraud_rate, pw.this.merch_total,
            pw.this.cat_fraud_rate,
            pw.this.distance, pw.this.hour, pw.this.is_online, pw.this.is_late,
            pw.this.first, pw.this.last, pw.this.trans_date_trans_time
        )
    )
    
    # Optional: Log results to console
    pw.io.null.write(results)
    
    print("🎯 Inference pipeline active...")
    print("   Publishing flagged transactions directly to NATS")
    print()
    
    pw.run(monitoring_level=pw.MonitoringLevel.NONE)

if __name__ == "__main__":
    run_inference_detector()