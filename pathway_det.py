"""
PATHWAY-NATIVE FRAUD DETECTOR v7.0 - FIXED VERSION
Leverages Pathway's incremental computation and joins
"""

import pathway as pw
import math
import json
from datetime import datetime
from river import tree, preprocessing, compose


# ============================================================================
# PATHWAY SCHEMA WITH EXPLICIT TYPES
# ============================================================================

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
    """Parse JSON and return alert_json if it's an alert, otherwise return None"""
    try:
        data = json.loads(alert_json)
        if data.get('is_alert', False):
            return alert_json
        return None
    except:
        return None


# ============================================================================
# ML MODEL MANAGEMENT
# ============================================================================

class MLModelState:
    """Global ML models"""
    def __init__(self):
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
        
        self.stats = {'total': 0, 'alerts': 0, 'tier1': 0, 'tier2': 0, 'tier3': 0}

ml_state = MLModelState()


@pw.udf
def comprehensive_fraud_detection(
    trans_num: str, cc_num: int, merchant: str, category: str,
    amt: float, lat: float, long: float, merch_lat: float, merch_long: float,
    unix_time: int, city: str, state: str, is_fraud: int, city_pop: int,
    # Pathway-computed features
    customer_avg_amt: float, customer_std_amt: float, customer_txn_count: int,
    customer_fraud_history: int, customer_avg_dist: float, customer_std_dist: float,
    merch_fraud_rate: float, merch_total: int,
    cat_fraud_rate: float,
    distance: float, hour: int, is_online: int, is_late: int
) -> str:
    """Comprehensive fraud detection with Pathway-enriched features"""
    
    try:
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
            
            return json.dumps({'is_alert': False, 'training': True})
        
        # ===== FEATURE ENGINEERING =====
        z_amt = (amt - customer_avg_amt) / customer_std_amt if customer_std_amt > 0 else 0
        amt_ratio = amt / customer_avg_amt if customer_avg_amt > 0 else 1
        
        z_dist = (distance - customer_avg_dist) / customer_std_dist if customer_std_dist > 0 else 0
        
        # ===== ML PREDICTION =====
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
        
        # ===== TIER-BASED DETECTION =====
        is_suspicious = False
        tier = 0
        reasons = []
        confidence = 0
        
        # TIER 1: Absolute certainty
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
        
        # Tier 1 decision
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
        
        # TIER 2: Strong evidence
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
        
        # TIER 3: ML-based
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
        
        # Progress reporting
        if ml_state.stats['total'] % 10000 == 0:
            print(f"[PROGRESS] Processed: {ml_state.stats['total']:,} | Alerts: {ml_state.stats['alerts']:,} | T1:{ml_state.stats['tier1']} T2:{ml_state.stats['tier2']} T3:{ml_state.stats['tier3']}")
        
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
    """Pathway-native fraud detection"""
    
    print("═══════════════════════════════════════════════════════════")
    print("  PATHWAY-NATIVE FRAUD DETECTOR v7.0 - FIXED")
    print("═══════════════════════════════════════════════════════════")
    print()
    print("  ✓ Incremental merchant/customer profiling")
    print("  ✓ Declarative joins for enrichment")
    print("  ✓ Dual Hoeffding Adaptive Trees")
    print("  ✓ Fixed alert filtering")
    print()
    
    # ========== READ TRANSACTIONS ==========
    transactions = pw.io.jsonlines.read(
        'pathway_streams/transactions.jsonl',
        schema=TransactionSchema,
        mode='streaming'
    )
    
    print("✓ Connected to transaction stream")
    print("✓ Building incremental profiles...")
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
    
    # ========== MERCHANT STATISTICS (Incremental with Pathway) ==========
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
    
    # ========== CATEGORY STATISTICS (Incremental) ==========
    category_stats = transactions.groupby(pw.this.category).reduce(
        category=pw.this.category,
        total=pw.reducers.count(),
        fraud_count=pw.reducers.sum(pw.this.is_fraud)
    ).select(
        category=pw.this.category,
        fraud_rate=pw.apply(lambda f, t: f / t if t > 100 else 0.0,
                           pw.this.fraud_count, pw.this.total)
    )
    
    # ========== CUSTOMER PROFILES (Incremental) ==========
    customer_stats = transactions.groupby(pw.this.cc_num).reduce(
        cc_num=pw.this.cc_num,
        txn_count=pw.reducers.count(),
        avg_amt=pw.reducers.avg(pw.this.amt),
        amt_array=pw.reducers.ndarray(pw.this.amt),
        total_spent=pw.reducers.sum(pw.this.amt),
        avg_dist=pw.reducers.avg(pw.this.distance),
        dist_array=pw.reducers.ndarray(pw.this.distance),
        fraud_history=pw.reducers.sum(pw.this.is_fraud)
    )
    
    # Calculate std from arrays
    customer_stats = customer_stats.select(
        cc_num=pw.this.cc_num,
        txn_count=pw.this.txn_count,
        avg_amt=pw.this.avg_amt,
        std_amt=calculate_std(pw.this.amt_array),
        total_spent=pw.this.total_spent,
        avg_dist=pw.this.avg_dist,
        std_dist=calculate_std(pw.this.dist_array),
        fraud_history=pw.this.fraud_history
    )
    
    # ========== JOIN EVERYTHING (Pathway's Strength!) ==========
    enriched = transactions
    
    # Join merchant stats
    enriched = enriched.join_left(
        merchant_stats,
        enriched.merchant == merchant_stats.merchant
    ).select(
        *pw.left,
        merch_fraud_rate=pw.require(pw.right.fraud_rate, pw.right.fraud_rate, 0.0),
        merch_total=pw.require(pw.right.total, pw.right.total, 0)
    )
    
    # Join category stats
    enriched = enriched.join_left(
        category_stats,
        enriched.category == category_stats.category
    ).select(
        *pw.left,
        cat_fraud_rate=pw.require(pw.right.fraud_rate, pw.right.fraud_rate, 0.0)
    )
    
    # Join customer stats
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
    
    # ========== FRAUD DETECTION ==========
    # Apply fraud detection to all transactions
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
    
    # ========== OUTPUT ==========
    # Write all results for debugging
    pw.io.jsonlines.write(results, 'pathway_streams/all_results.jsonl')
    
    # Filter to only alerts - return None for non-alerts, JSON string for alerts
    alerts_filtered = results.select(
        alert_json=parse_and_filter_alert(pw.this.alert_json)
    )
    
    # Filter out None values using is_not_none() like the working example
    alerts = alerts_filtered.filter(pw.this.alert_json.is_not_none())
    
    # Write only alerts
    pw.io.jsonlines.write(alerts, 'pathway_streams/fraud_alerts.jsonl')
    
    print("🎯 Pathway-native pipeline active...")
    print("   Alerts Output: pathway_streams/fraud_alerts.jsonl")
    print("   Debug Output: pathway_streams/all_results.jsonl")
    print("   Press Ctrl+C to stop")
    print()
    
    # Run pipeline
    pw.run()


if __name__ == "__main__":
    import os
    import signal
    import sys
    
    os.makedirs('pathway_streams', exist_ok=True)
    
    def signal_handler(sig, frame):
        print("\n\n═══════════════════════════════════════════════════════════")
        print("    DETECTOR SHUTDOWN")
        print("═══════════════════════════════════════════════════════════")
        print(f"Total Processed: {ml_state.stats['total']:,}")
        print(f"Alerts Generated: {ml_state.stats['alerts']:,}")
        print(f"  - Tier 1: {ml_state.stats['tier1']:,}")
        print(f"  - Tier 2: {ml_state.stats['tier2']:,}")
        print(f"  - Tier 3: {ml_state.stats['tier3']:,}")
        print("═══════════════════════════════════════════════════════════")
        sys.exit(0)
    
    # Register signal handler for Ctrl+C
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        run_detector()
    except KeyboardInterrupt:
        signal_handler(None, None)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()