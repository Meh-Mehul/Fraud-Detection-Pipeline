"""
Real-time fraud detection with NATS messaging
"""
## This is a simpler detector model we have made till now.
## For now, it checks some (assumed) bank-rules, as well as trains an ML-model (via ground truths from the datasets, with pathway persistence)

import pathway as pw
import math
import json
import pickle
from datetime import datetime
from pathlib import Path
from river import tree, preprocessing, compose


NATS_URI = "nats://localhost:4222"
NATS_INPUT_TOPIC = "fraud.transactions"
NATS_ALERTS_TOPIC = "fraud.alerts"
NATS_RESULTS_TOPIC = "fraud.results"

# ============================================================================
# PERSISTENCE CONFIGURATION
# ============================================================================
PERSISTENCE_DIR = Path("pathway_persistence")
PERSISTENCE_DIR.mkdir(exist_ok=True)

CHECKPOINT_CONFIG = pw.persistence.Config.simple_config(
    pw.persistence.Backend.filesystem(str(PERSISTENCE_DIR / "checkpoints")),
    snapshot_interval_ms=10000
)


# ============================================================================
# PATHWAY SCHEMA WITH EXPLICIT TYPES
# ============================================================================
## This is same as the synthetic dataset we are testing on, but it may be configured according to bank usage as well
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


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

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


@pw.udf
def is_online_category(category: str) -> int:
    """Check if category is online"""
    online_cats = ['shopping_net', 'misc_net', 'grocery_net']
    return 1 if category in online_cats else 0


@pw.udf
def is_late_night(hour: int) -> int:
    """Check if transaction is late night"""
    return 1 if 1 <= hour <= 5 else 0


@pw.udf
def calculate_std(arr) -> float:
    """Calculate standard deviation from array"""
    try:
        return float(arr.std()) if len(arr) > 1 else 0.0
    except:
        return 0.0


@pw.udf
def parse_and_filter_alert(alert_json: str):
    """Parse JSON and return alert_json if it's an alert"""
    try:
        data = json.loads(alert_json)
        if data.get('is_alert', False):
            return alert_json
        return None
    except:
        return None


# ============================================================================
# ML MODEL MANAGEMENT WITH DEDUPLICATION TRACKING
# ============================================================================
class MLModelState:
    """ML models with persistence and deduplication support"""
    
    def __init__(self):
        self.model_path = PERSISTENCE_DIR / "ml_models.pkl"
        self.stats_path = PERSISTENCE_DIR / "stats.json"
        
        # CRITICAL: Track processed transactions to prevent duplicates
        self.processed_transactions = set()
        self.processed_path = PERSISTENCE_DIR / "processed_trans.json"
        
        # Try to load existing models
        if self.model_path.exists():
            print("🔄 Loading saved ML models...")
            try:
                with open(self.model_path, 'rb') as f:
                    saved = pickle.load(f)
                    self.model_main = saved['model_main']
                    self.model_validator = saved['model_validator']
                print("✓ ML models restored from disk")
            except Exception as e:
                print(f"⚠️  Could not load models: {e}, creating new ones")
                self._init_new_models()
        else:
            self._init_new_models()
        
        # Load stats
        if self.stats_path.exists():
            with open(self.stats_path, 'r') as f:
                self.stats = json.load(f)
            print(f"✓ Stats restored: {self.stats['total']:,} transactions processed")
        else:
            self.stats = {'total': 0, 'alerts': 0, 'tier1': 0, 'tier2': 0, 'tier3': 0}
        
        # Load processed transactions
        if self.processed_path.exists():
            with open(self.processed_path, 'r') as f:
                self.processed_transactions = set(json.load(f))
            print(f"✓ Loaded {len(self.processed_transactions):,} processed transaction IDs")
        
        self.last_save_time = datetime.now()
        self.save_interval = 60
    
    def _init_new_models(self):
        """Initialize new ML models"""
        self.model_main = compose.Pipeline(
            preprocessing.StandardScaler(),
            tree.HoeffdingAdaptiveTreeClassifier(
                grace_period=200,
                delta=0.00001,
                seed=42
            )
        )
        
        self.model_validator = tree.HoeffdingAdaptiveTreeClassifier(
            grace_period=150,
            delta=0.0001,
            seed=123
        )
        print("✓ New ML models initialized")
    
    def is_already_processed(self, trans_num: str) -> bool:
        """Check if transaction already processed"""
        return trans_num in self.processed_transactions
    
    def mark_processed(self, trans_num: str):
        """Mark transaction as processed"""
        self.processed_transactions.add(trans_num)
    
    def save_models(self, force=False):
        """Periodically save models to disk"""
        now = datetime.now()
        if force or (now - self.last_save_time).total_seconds() >= self.save_interval:
            try:
                # Save models
                with open(self.model_path, 'wb') as f:
                    pickle.dump({
                        'model_main': self.model_main,
                        'model_validator': self.model_validator
                    }, f)
                
                # Save stats
                with open(self.stats_path, 'w') as f:
                    json.dump(self.stats, f)
                
                # Save processed transactions (keep only last 100k to prevent unbounded growth)
                processed_list = list(self.processed_transactions)
                if len(processed_list) > 100000:
                    processed_list = processed_list[-100000:]
                    self.processed_transactions = set(processed_list)
                
                with open(self.processed_path, 'w') as f:
                    json.dump(processed_list, f)
                
                self.last_save_time = now
                
                if self.stats['total'] % 50000 == 0 and self.stats['total'] > 0:
                    print(f"💾 Models and stats saved (Total: {self.stats['total']:,})")
            except Exception as e:
                print(f"⚠️  Could not save models: {e}")


ml_state = MLModelState()
@pw.udf
def comprehensive_fraud_detection(
    trans_num: str, cc_num: int, merchant: str, category: str,
    amt: float, lat: float, long: float, merch_lat: float, merch_long: float,
    unix_time: int, city: str, state: str, is_fraud: int, city_pop: int,
    customer_avg_amt: float, customer_std_amt: float, customer_txn_count: int,
    customer_fraud_history: int, customer_avg_dist: float, customer_std_dist: float,
    merch_fraud_rate: float, merch_total: int,
    cat_fraud_rate: float,
    distance: float, hour: int, is_online: int, is_late: int
) -> str:
    """Comprehensive fraud detection with deduplication"""
    
    try:
        # CRITICAL: Check if already processed (prevents duplicates from re-evaluations)
        if ml_state.is_already_processed(trans_num):
            return json.dumps({'is_alert': False, 'duplicate': True})
        
        # Mark as processed immediately
        ml_state.mark_processed(trans_num)
        ml_state.stats['total'] += 1
        
        # Training phase
        if customer_txn_count < 20:
            feats = {
                'amt': float(amt), 'dist': float(distance), 'hr': float(hour),
                'n': float(customer_txn_count)
            }
            try:
                ml_state.model_main.learn_one(feats, is_fraud)
                ml_state.model_validator.learn_one(feats, is_fraud)
            except:
                pass
            
            if ml_state.stats['total'] % 10000 == 0:
                print(f"[TRAIN] {ml_state.stats['total']:,}")
                ml_state.save_models()
            
            return json.dumps({'is_alert': False, 'training': True})
        
        # Feature engineering
        # Some z-Score tests
        z_amt = (amt - customer_avg_amt) / customer_std_amt if customer_std_amt > 0 else 0
        amt_ratio = amt / customer_avg_amt if customer_avg_amt > 0 else 1
        z_dist = (distance - customer_avg_dist) / customer_std_dist if customer_std_dist > 0 else 0
        
        # ML prediction
        feats = {
            'amt': float(amt), 'z_amt': float(z_amt), 'amt_ratio': float(amt_ratio),
            'dist': float(distance), 'z_dist': float(z_dist),
            'hr': float(hour), 'merch_risk': float(merch_fraud_rate),
            'cat_risk': float(cat_fraud_rate), 'online': float(is_online),
            'late_night': float(is_late), 'fraud_history': float(customer_fraud_history),
            'n': float(min(customer_txn_count, 1000))
        }
        
        try:
            ml_prob1 = ml_state.model_main.predict_proba_one(feats)
            ml_score1 = ml_prob1.get(1, 0.0) * 100
        except:
            ml_score1 = 0.0
        
        try:
            ml_prob2 = ml_state.model_validator.predict_proba_one(feats)
            ml_score2 = ml_prob2.get(1, 0.0) * 100
        except:
            ml_score2 = 0.0
        
        ml_score = (ml_score1 + ml_score2) / 2
        ml_agreement = abs(ml_score1 - ml_score2) < 20
        

        ################## Some Bank-set rules ############
        ## For now, we have set them here, but in our final pipeline, they may be configurable as well.        
        # Tier-based detection
        is_suspicious = False
        tier = 0
        reasons = []
        confidence = 0
        
        # TIER 1
        extreme_signals = []
        if z_amt > 4.5:
            extreme_signals.append(f"MASSIVE_AMT(Z={z_amt:.1f})")
        elif z_amt > 3.8 and amt > 500:
            extreme_signals.append(f"HUGE_AMT(Z={z_amt:.1f},${amt:.0f})")
        
        if z_dist > 4:
            extreme_signals.append(f"EXTREME_DIST(Z={z_dist:.1f})")
        elif z_dist > 3.2 and distance > 100:
            extreme_signals.append(f"VERY_FAR({distance:.0f}km)")
        
        if merch_fraud_rate > 0.4 and merch_total > 50:
            extreme_signals.append(f"FRAUD_MERCHANT({merch_fraud_rate*100:.0f}%)")
        
        if customer_fraud_history >= 3:
            extreme_signals.append(f"FRAUD_HISTORY({customer_fraud_history})")
        
        if len(extreme_signals) >= 2:
            is_suspicious = True
            tier = 1
            confidence = 95
            ml_state.stats['tier1'] += 1
            reasons = extreme_signals[:3]
        elif len(extreme_signals) >= 1 and ml_score >= 80 and ml_agreement:
            is_suspicious = True
            tier = 1
            confidence = 90
            ml_state.stats['tier1'] += 1
            reasons = extreme_signals + [f"ML{ml_score:.0f}"]
        
        # TIER 2
        if not is_suspicious:
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
            
            if merch_fraud_rate > 0.3 and merch_total > 40:
                tier2_score += 35
                tier2_reasons.append(f"RiskyMerch({merch_fraud_rate*100:.0f}%)")
            
            if is_online and is_late and amt > 400:
                tier2_score += 25
                tier2_reasons.append(f"LateOnline")
            
            if customer_fraud_history >= 2:
                tier2_score += 30
                tier2_reasons.append(f"PrevFraud({customer_fraud_history})")
            
            if z_amt > 2.5 and z_dist > 2.5:
                tier2_score += 25
                tier2_reasons.append("Amt+Dist")
            
            if ml_score >= 80 and ml_agreement:
                tier2_score += 25
                tier2_reasons.append(f"ML{ml_score:.0f}")
            elif ml_score >= 70:
                tier2_score += 15
            
            if tier2_score >= 75:
                is_suspicious = True
                tier = 2
                confidence = 80
                ml_state.stats['tier2'] += 1
                reasons = tier2_reasons[:4]
        
        # TIER 3
        if not is_suspicious:
            if ml_score >= 82 and ml_agreement:
                support_count = sum([
                    z_amt > 2, z_dist > 2, merch_fraud_rate > 0.15,
                    cat_fraud_rate > 0.1, customer_fraud_history >= 1
                ])
                
                if support_count >= 2:
                    is_suspicious = True
                    tier = 3
                    confidence = 75
                    ml_state.stats['tier3'] += 1
                    reasons.append(f"ML{ml_score:.0f}")
                    if z_amt > 2.5: reasons.append(f"HighAmt")
                    if z_dist > 2.5: reasons.append(f"FarLoc")
        
        reason_str = "|".join(reasons[:4]) if reasons else f"T{tier}"
        
        # Continue learning
        try:
            ml_state.model_main.learn_one(feats, is_fraud)
            ml_state.model_validator.learn_one(feats, is_fraud)
        except:
            pass
        
        # Periodic save and progress
        if ml_state.stats['total'] % 10000 == 0:
            print(f"[PROGRESS] {ml_state.stats['total']:,} | Alerts: {ml_state.stats['alerts']:,}")
            ml_state.save_models()
        
        # Publish alert
        if is_suspicious:
            ml_state.stats['alerts'] += 1
            
            if ml_state.stats['alerts'] % 50 == 0:
                print(f"[ALERT] #{ml_state.stats['alerts']:>6} | T{tier} | {reason_str[:35]}")
            
            return json.dumps({
                'is_alert': True,
                'trans_num': str(trans_num),
                'cc_num': int(cc_num),
                'merchant': merchant,
                'category': category,
                'amt': float(amt),
                'location': f"{city}, {state}",
                'risk_score': confidence,
                'reasons': reason_str,
                'confidence': confidence,
                'actual_fraud': is_fraud,
                'tier': tier,
                'ml_score': round(ml_score, 1)
            })
        
        return json.dumps({'is_alert': False})
    
    except Exception as e:
        print(f"[ERROR] {e}")
        return json.dumps({'is_alert': False, 'error': str(e)})


# ============================================================================
# MAIN DETECTOR PIPELINE
# ============================================================================

def run_detector():
    """Pathway-native fraud detection with NATS messaging"""
    
    print("═══════════════════════════════════════════════════════════")
    print("  PATHWAY-NATIVE FRAUD DETECTOR v9.1 - NATS FIXED")
    print("═══════════════════════════════════════════════════════════")
    print()
    print("  ✓ State persistence enabled")
    print("  ✓ Deduplication active")
    print("  ✓ ML model checkpointing")
    print("  ✓ Crash recovery support")
    print()
    print(f"  NATS URI: {NATS_URI}")
    print(f"  Input: {NATS_INPUT_TOPIC}")
    print(f"  Alerts: {NATS_ALERTS_TOPIC}")
    print(f"  Results: {NATS_RESULTS_TOPIC}")
    print()
    
    # Read transactions from NATS with persistence - FIXED parameters
    transactions = pw.io.nats.read(
        uri=NATS_URI,
        topic=NATS_INPUT_TOPIC,
        schema=TransactionSchema,
        format='json',
        persistent_id="transactions_input"
    )
    
    print("✓ Connected to NATS transaction stream (persistent)")
    print(f"✓ Checkpoint dir: {PERSISTENCE_DIR / 'checkpoints'}")
    print(f"✓ Processed tracking: {len(ml_state.processed_transactions):,} IDs")
    print()
    
    # Add computed fields
    transactions = transactions.select(
        *pw.this,
        hour=extract_hour(pw.this.unix_time),
        distance=haversine_distance(pw.this.lat, pw.this.long, 
                                    pw.this.merch_lat, pw.this.merch_long),
        is_online=is_online_category(pw.this.category),
        is_late=is_late_night(extract_hour(pw.this.unix_time))
    )
    
    # Merchant statistics
    merchant_stats = transactions.groupby(pw.this.merchant).reduce(
        merchant=pw.this.merchant,
        total=pw.reducers.count(),
        fraud_count=pw.reducers.sum(pw.this.is_fraud),
        amt_sum=pw.reducers.sum(pw.this.amt)
    ).select(
        merchant=pw.this.merchant,
        total=pw.this.total,
        fraud_rate=pw.apply(lambda f, t: f / t if t > 30 else 0.0,
                           pw.this.fraud_count, pw.this.total)
    )
    
    # Category statistics
    category_stats = transactions.groupby(pw.this.category).reduce(
        category=pw.this.category,
        total=pw.reducers.count(),
        fraud_count=pw.reducers.sum(pw.this.is_fraud)
    ).select(
        category=pw.this.category,
        fraud_rate=pw.apply(lambda f, t: f / t if t > 100 else 0.0,
                           pw.this.fraud_count, pw.this.total)
    )
    
    # Customer profiles
    customer_stats = transactions.groupby(pw.this.cc_num).reduce(
        cc_num=pw.this.cc_num,
        txn_count=pw.reducers.count(),
        avg_amt=pw.reducers.avg(pw.this.amt),
        amt_array=pw.reducers.ndarray(pw.this.amt),
        total_spent=pw.reducers.sum(pw.this.amt),
        avg_dist=pw.reducers.avg(pw.this.distance),
        dist_array=pw.reducers.ndarray(pw.this.distance),
        fraud_history=pw.reducers.sum(pw.this.is_fraud)
    ).select(
        cc_num=pw.this.cc_num,
        txn_count=pw.this.txn_count,
        avg_amt=pw.this.avg_amt,
        std_amt=calculate_std(pw.this.amt_array),
        total_spent=pw.this.total_spent,
        avg_dist=pw.this.avg_dist,
        std_dist=calculate_std(pw.this.dist_array),
        fraud_history=pw.this.fraud_history
    )
    
    # Join everything
    enriched = transactions
    
    enriched = enriched.join_left(
        merchant_stats,
        enriched.merchant == merchant_stats.merchant
    ).select(
        *pw.left,
        merch_fraud_rate=pw.require(pw.right.fraud_rate, pw.right.fraud_rate, 0.0),
        merch_total=pw.require(pw.right.total, pw.right.total, 0)
    )
    
    enriched = enriched.join_left(
        category_stats,
        enriched.category == category_stats.category
    ).select(
        *pw.left,
        cat_fraud_rate=pw.require(pw.right.fraud_rate, pw.right.fraud_rate, 0.0)
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
    
    # Fraud detection
    results = enriched.select(
        alert_json=comprehensive_fraud_detection(
            pw.this.trans_num, pw.this.cc_num, pw.this.merchant, pw.this.category,
            pw.this.amt, pw.this.lat, pw.this.long, pw.this.merch_lat, pw.this.merch_long,
            pw.this.unix_time, pw.this.city, pw.this.state, pw.this.is_fraud, pw.this.city_pop,
            pw.this.customer_avg_amt, pw.this.customer_std_amt, pw.this.customer_txn_count,
            pw.this.customer_fraud_history, pw.this.customer_avg_dist, pw.this.customer_std_dist,
            pw.this.merch_fraud_rate, pw.this.merch_total,
            pw.this.cat_fraud_rate,
            pw.this.distance, pw.this.hour, pw.this.is_online, pw.this.is_late
        )
    )
    
    # Output all results to NATS - FIXED parameters
    pw.io.nats.write(
        results,
        uri=NATS_URI,
        topic=NATS_RESULTS_TOPIC
    )
    
    # Filter and publish alerts
    alerts_filtered = results.select(
        alert_json=parse_and_filter_alert(pw.this.alert_json)
    )
    
    alerts = alerts_filtered.filter(pw.this.alert_json.is_not_none())
    
    pw.io.nats.write(
        alerts,
        uri=NATS_URI,
        topic=NATS_ALERTS_TOPIC
    )
    
    print("🎯 Pathway pipeline active with NATS messaging...")
    print(f"   Alerts: nats://{NATS_URI}/{NATS_ALERTS_TOPIC}")
    print(f"   Debug: nats://{NATS_URI}/{NATS_RESULTS_TOPIC}")
    print("   Press Ctrl+C to stop")
    print()
    
    # Run with persistence config
    pw.run(
        persistence_config=CHECKPOINT_CONFIG,
        monitoring_level=pw.MonitoringLevel.NONE
    )

