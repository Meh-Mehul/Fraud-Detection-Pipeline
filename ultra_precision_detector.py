import asyncio
import json
from nats.aio.client import Client as NATS
import math
from datetime import datetime
from collections import defaultdict, deque

from river import tree, preprocessing, compose


class UltraPrecisionDetector:
    def __init__(self):
        # Use multiple trees with different parameters
        self.model_main = compose.Pipeline(
            preprocessing.StandardScaler(),
            tree.HoeffdingAdaptiveTreeClassifier(
                grace_period=200,
                delta=0.00001,
                seed=42
            )
        )
        
        # Second model for validation
        self.model_validator = tree.HoeffdingAdaptiveTreeClassifier(
            grace_period=150,
            delta=0.0001,
            seed=123
        )
        
        self.merchant_stats = defaultdict(lambda: {
            'total': 0, 'fraud': 0, 'risk': 0.0,
            'high_amt_fraud': 0,
            'avg_amt': 0.0,
            'amt_sum': 0.0
        })
        
        self.category_stats = defaultdict(lambda: {
            'total': 0, 'fraud': 0, 'risk': 0.0
        })
        
        # Location-based stats (city_pop bins)
        self.location_stats = defaultdict(lambda: {'total': 0, 'fraud': 0, 'risk': 0.0})
        
        self.stats = {
            'total': 0, 'training': 0, 'detection': 0, 'alerts': 0,
            'tp': 0, 'fp': 0, 'tn': 0, 'fn': 0,
            'tier1': 0, 'tier2': 0, 'tier3': 0
        }
        
        print("  ✓ Dual Hoeffding Adaptive Trees")
        print("  ✓ Ultra-conservative three-tier logic")


class CustomerProfile:
    def __init__(self):
        self.profiles = defaultdict(lambda: {
            'n': 0,
            'sum_amt': 0.0, 'sq_amt': 0.0, 'max_amt': 0.0, 'min_amt': 999999.0,
            'mean_amt': 0.0, 'std_amt': 0.0,
            'home_lat': 0.0, 'home_long': 0.0,
            'sum_dist': 0.0, 'sq_dist': 0.0, 'mean_dist': 0.0, 'std_dist': 0.0, 'max_dist': 0.0,
            'categories': defaultdict(int),
            'merchants': defaultdict(int),
            'hours': defaultdict(int),
            'txn_times': deque(maxlen=30),
            'amounts': deque(maxlen=30),
            'distances': deque(maxlen=30),
            'last_time': 0,
            'fraud_count': 0,
            'confirmed_frauds': 0,
            'total_spent': 0.0,
            'avg_daily_txns': 0.0,
            'days_active': set()
        })
    
    def update(self, cc, amt, dist, hr, cat, merch, ut, fraud):
        p = self.profiles[cc]
        p['n'] += 1
        n = p['n']
        
        # Amount stats (Welford's algorithm)
        delta = amt - p['mean_amt']
        p['mean_amt'] += delta / n
        p['sum_amt'] += amt
        p['sq_amt'] += amt * amt
        p['max_amt'] = max(p['max_amt'], amt)
        p['min_amt'] = min(p['min_amt'], amt)
        p['total_spent'] += amt
        
        if n > 1:
            var = (p['sq_amt'] - (p['sum_amt'] ** 2 / n)) / (n - 1)
            p['std_amt'] = math.sqrt(max(0, var))
        
        # Distance stats
        p['sum_dist'] += dist
        p['sq_dist'] += dist * dist
        p['mean_dist'] = p['sum_dist'] / n
        p['max_dist'] = max(p['max_dist'], dist)
        
        if n > 1:
            dist_var = (p['sq_dist'] - (p['sum_dist'] ** 2 / n)) / (n - 1)
            p['std_dist'] = math.sqrt(max(0, dist_var))
        
        # Patterns
        p['categories'][cat] += 1
        p['merchants'][merch] += 1
        p['hours'][hr] += 1
        
        # Temporal
        p['txn_times'].append(ut)
        p['amounts'].append(amt)
        p['distances'].append(dist)
        p['last_time'] = ut
        
        # Track active days
        day = datetime.fromtimestamp(ut).date()
        p['days_active'].add(day)
        p['avg_daily_txns'] = n / len(p['days_active']) if p['days_active'] else 0
        
        if fraud == 1:
            p['fraud_count'] += 1
            p['confirmed_frauds'] += 1


detector = UltraPrecisionDetector()
profiler = CustomerProfile()


def haversine(lat1, lon1, lat2, lon2):
    d = math.radians(lat2-lat1), math.radians(lon2-lon1)
    a = math.sin(d[0]/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(d[1]/2)**2
    return 6371 * 2 * math.asin(math.sqrt(a))


async def detect_fraud_ultra(data, pub_nc):
    try:
        detector.stats['total'] += 1
        
        cc = data['cc_num']
        amt = data['amt']
        lat, lng = data['lat'], data['long']
        mlat, mlng = data['merch_lat'], data['merch_long']
        ut = data['unix_time']
        cat, merch = data['category'], data['merchant']
        cp = data['city_pop']
        fraud = data['is_fraud']
        
        p = profiler.profiles[cc]
        n = p['n']
        
        if p['home_lat'] == 0.0:
            p['home_lat'], p['home_long'] = lat, lng
            dist = 0.0
        else:
            dist = haversine(p['home_lat'], p['home_long'], mlat, mlng)
        
        hr = datetime.fromtimestamp(ut).hour
        
        # Update merchant stats
        ms = detector.merchant_stats[merch]
        ms['total'] += 1
        ms['amt_sum'] += amt
        ms['avg_amt'] = ms['amt_sum'] / ms['total']
        if fraud == 1:
            ms['fraud'] += 1
            if amt > 300:
                ms['high_amt_fraud'] += 1
        if ms['total'] > 30:
            ms['risk'] = ms['fraud'] / ms['total']
        
        # Update category stats
        cs = detector.category_stats[cat]
        cs['total'] += 1
        if fraud == 1:
            cs['fraud'] += 1
        if cs['total'] > 100:
            cs['risk'] = cs['fraud'] / cs['total']
        
        # Update location stats (bin by city_pop)
        pop_bin = 'urban' if cp > 100000 else 'suburban' if cp > 20000 else 'rural'
        ls = detector.location_stats[pop_bin]
        ls['total'] += 1
        if fraud == 1:
            ls['fraud'] += 1
        if ls['total'] > 100:
            ls['risk'] = ls['fraud'] / ls['total']
        
        # Training phase (20 transactions)
        if n < 20:
            detector.stats['training'] += 1
            feats = {
                'amt': float(amt), 'dist': float(dist), 'hr': float(hr),
                'cp': float(min(cp, 1e6)), 'n': float(n)
            }
            try:
                detector.model_main.learn_one(feats, fraud)
                detector.model_validator.learn_one(feats, fraud)
            except:
                pass
            profiler.update(cc, amt, dist, hr, cat, merch, ut, fraud)
            if detector.stats['total'] % 10000 == 0:
                print(f"[TRAIN] {detector.stats['total']:,}")
            return
        
        detector.stats['detection'] += 1
        
        # ===== FEATURE ENGINEERING (Sparkov-aware) =====
        
        # Amount features (key Sparkov fraud indicator)
        z_amt = (amt - p['mean_amt']) / p['std_amt'] if p['std_amt'] > 0 else 0
        amt_ratio = amt / p['mean_amt'] if p['mean_amt'] > 0 else 1
        amt_vs_max = amt / p['max_amt'] if p['max_amt'] > 0 else 1
        amt_percentile = sum(1 for a in p['amounts'] if a < amt) / len(p['amounts']) if p['amounts'] else 0.5
        
        # Distance features (key Sparkov fraud indicator)
        z_dist = (dist - p['mean_dist']) / p['std_dist'] if p['std_dist'] > 0 else 0
        dist_vs_max = dist / p['max_dist'] if p['max_dist'] > 0 else 1
        dist_percentile = sum(1 for d in p['distances'] if d < dist) / len(p['distances']) if p['distances'] else 0.5
        
        # Velocity features (critical for Sparkov fraud detection)
        times = list(p['txn_times'])
        velocity_5m = sum(1 for t in times if ut - t <= 300)
        velocity_10m = sum(1 for t in times if ut - t <= 600)
        velocity_15m = sum(1 for t in times if ut - t <= 900)
        velocity_1h = sum(1 for t in times if ut - t <= 3600)
        velocity_24h = sum(1 for t in times if ut - t <= 86400)
        
        gaps = [times[i+1] - times[i] for i in range(len(times)-1)] if len(times) > 1 else [3600]
        min_gap = min(gaps)
        avg_gap = sum(gaps) / len(gaps)
        
        # Pattern consistency
        cat_total = sum(p['categories'].values())
        cat_freq = p['categories'][cat] / cat_total if cat_total > 0 else 0
        
        hr_total = sum(p['hours'].values())
        hr_freq = p['hours'][hr] / hr_total if hr_total > 0 else 0
        
        merch_total = sum(p['merchants'].values())
        merch_freq = p['merchants'][merch] / merch_total if merch_total > 0 else 0
        is_new_merchant = merch_freq == 0
        
        # Time-based features
        online = cat in ['shopping_net', 'misc_net', 'grocery_net']
        late_night = 1 <= hr <= 5
        business_hours = 9 <= hr <= 17
        
        # Behavioral anomalies
        daily_velocity = velocity_24h / p['avg_daily_txns'] if p['avg_daily_txns'] > 0 else 1
        
        # Recent trends
        if len(p['amounts']) >= 5:
            recent_avg = sum(list(p['amounts'])[-5:]) / 5
            amt_trend = (amt - recent_avg) / max(recent_avg, 1)
        else:
            amt_trend = 0
        
        feats = {
            'amt': float(amt),
            'z_amt': float(z_amt),
            'amt_ratio': float(amt_ratio),
            'amt_percentile': float(amt_percentile),
            'dist': float(dist),
            'z_dist': float(z_dist),
            'dist_percentile': float(dist_percentile),
            'hr': float(hr),
            'velocity_5m': float(velocity_5m),
            'velocity_10m': float(velocity_10m),
            'velocity_1h': float(velocity_1h),
            'min_gap': float(min_gap),
            'merch_risk': float(ms['risk']),
            'cat_risk': float(cs['risk']),
            'cat_freq': float(cat_freq),
            'hr_freq': float(hr_freq),
            'merch_freq': float(merch_freq),
            'online': float(online),
            'late_night': float(late_night),
            'fraud_history': float(p['confirmed_frauds']),
            'daily_velocity': float(daily_velocity),
            'n': float(min(n, 1000))
        }
        
        # ===== DUAL ML PREDICTION =====
        try:
            ml_prob1 = detector.model_main.predict_proba_one(feats)
            ml_score1 = ml_prob1.get(1, 0.0) * 100
        except:
            ml_score1 = 0.0
        
        try:
            ml_prob2 = detector.model_validator.predict_proba_one(feats)
            ml_score2 = ml_prob2.get(1, 0.0) * 100
        except:
            ml_score2 = 0.0
        
        # Ensemble ML score (both models must agree)
        ml_score = (ml_score1 + ml_score2) / 2
        ml_agreement = abs(ml_score1 - ml_score2) < 20  # Models agree if within 20%
        
        # ===== ULTRA-SELECTIVE DECISION LOGIC =====
        # Sparkov frauds typically have multiple anomalies
        
        is_suspicious = False
        tier = 0
        reasons = []
        confidence = 0
        
        # TIER 1: Absolute certainty (2+ extreme signals + ML agreement)
        extreme_signals = []
        
        if velocity_5m >= 4:
            extreme_signals.append(f"EXTREME_BURST({velocity_5m}/5m)")
        elif velocity_10m >= 5:  # Add more ways to trigger Tier 1
            extreme_signals.append(f"MAJOR_BURST({velocity_10m}/10m)")
        
        if z_amt > 4.5 or (z_amt > 4 and amt_percentile > 0.95):
            extreme_signals.append(f"MASSIVE_AMT(Z={z_amt:.1f})")
        elif z_amt > 3.8 and amt > 500:  # Additional trigger
            extreme_signals.append(f"HUGE_AMT(Z={z_amt:.1f},${amt:.0f})")
        
        if z_dist > 4 or (z_dist > 3.5 and dist_percentile > 0.95):
            extreme_signals.append(f"EXTREME_DIST(Z={z_dist:.1f})")
        elif z_dist > 3.2 and dist > 100:  # Very far transactions
            extreme_signals.append(f"VERY_FAR({dist:.0f}km)")
        
        if ms['risk'] > 0.4 and ms['total'] > 50:
            extreme_signals.append(f"FRAUD_MERCHANT({ms['risk']*100:.0f}%)")
        elif ms['risk'] > 0.35 and ms['total'] > 40:  # Slightly relaxed
            extreme_signals.append(f"BAD_MERCHANT({ms['risk']*100:.0f}%)")
        
        if p['confirmed_frauds'] >= 3:
            extreme_signals.append(f"FRAUD_HISTORY({p['confirmed_frauds']})")
        elif p['confirmed_frauds'] >= 2 and z_amt > 2.5:  # History + amount
            extreme_signals.append(f"REPEAT_FRAUD({p['confirmed_frauds']})")
        
        # Tier 1: 2+ extreme signals OR 1 extreme + very high ML (relaxed from 85 to 80)
        if len(extreme_signals) >= 2:
            is_suspicious = True
            tier = 1
            confidence = 95
            detector.stats['tier1'] += 1
            reasons = extreme_signals[:3]
        
        elif len(extreme_signals) >= 1 and ml_score >= 80 and ml_agreement:
            is_suspicious = True
            tier = 1
            confidence = 90
            detector.stats['tier1'] += 1
            reasons = extreme_signals + [f"ML{ml_score:.0f}"]
        
        # TIER 2: Strong evidence (score-based with high threshold)
        elif not is_suspicious:
            tier2_score = 0
            tier2_reasons = []
            
            # Velocity patterns (Sparkov key indicator)
            if velocity_5m >= 3:
                tier2_score += 45
                tier2_reasons.append(f"BURST({velocity_5m}/5m)")
            elif velocity_10m >= 4:
                tier2_score += 35
                tier2_reasons.append(f"FastBurst({velocity_10m}/10m)")
            elif velocity_15m >= 5:
                tier2_score += 25
                tier2_reasons.append(f"Rapid({velocity_15m}/15m)")
            
            # Amount anomalies
            if z_amt > 3.5:
                tier2_score += 40
                tier2_reasons.append(f"VeryHighAmt(Z={z_amt:.1f})")
            elif z_amt > 3:
                tier2_score += 30
                tier2_reasons.append(f"HighAmt(Z={z_amt:.1f})")
            elif z_amt > 2.5 and amt_percentile > 0.9:
                tier2_score += 20
                tier2_reasons.append(f"UnusualAmt")

            # Distance anomalies
            if z_dist > 3.5:
                tier2_score += 35
                tier2_reasons.append(f"VeryFar(Z={z_dist:.1f})")
            elif z_dist > 3:
                tier2_score += 25
                tier2_reasons.append(f"Far(Z={z_dist:.1f})")
            elif z_dist > 2.5 and dist_percentile > 0.9:
                tier2_score += 15
                tier2_reasons.append(f"UnusualDist")
            
            # Merchant risk
            if ms['risk'] > 0.3 and ms['total'] > 40:
                tier2_score += 35
                tier2_reasons.append(f"RiskyMerch({ms['risk']*100:.0f}%)")
            elif ms['risk'] > 0.25 and ms['total'] > 30:
                tier2_score += 25
            
            # Pattern breaks
            if is_new_merchant and amt > 500:
                tier2_score += 30
                tier2_reasons.append(f"NewMerch+High")
            elif is_new_merchant and z_amt > 2:
                tier2_score += 20
            
            if cat_freq < 0.05 and amt > 600:
                tier2_score += 25
                tier2_reasons.append(f"RareCat+High")
            
            if hr_freq < 0.03 and amt > 400:
                tier2_score += 20
                tier2_reasons.append(f"UnusualHour({hr}h)")
            
            # Time-based suspicion
            if online and late_night and amt > 400:
                tier2_score += 25
                tier2_reasons.append(f"LateOnline")
            
            # Fraud history
            if p['confirmed_frauds'] >= 2:
                tier2_score += 30
                tier2_reasons.append(f"PrevFraud({p['confirmed_frauds']})")
            
            # Critical combinations (Sparkov typical patterns)
            if velocity_10m >= 3 and z_amt > 2.5:
                tier2_score += 25
                tier2_reasons.append("Burst+Amt")
            
            if z_amt > 2.5 and z_dist > 2.5:
                tier2_score += 25
                tier2_reasons.append("Amt+Dist")
            
            if is_new_merchant and z_dist > 2:
                tier2_score += 15
            
            # ML support (but not primary)
            if ml_score >= 80 and ml_agreement:
                tier2_score += 25
                tier2_reasons.append(f"ML{ml_score:.0f}")
            elif ml_score >= 70:
                tier2_score += 15
            
            # Balanced threshold: need 75+ points (was 85, too high)
            if tier2_score >= 75:
                is_suspicious = True
                tier = 2
                confidence = 80
                detector.stats['tier2'] += 1
                reasons = tier2_reasons[:4]
        
        # TIER 3: Strong ML with supporting evidence (relaxed from 88 to 82)
        elif not is_suspicious:
            if ml_score >= 82 and ml_agreement:
                support_count = sum([
                    z_amt > 2,
                    z_dist > 2,
                    velocity_10m >= 2,
                    ms['risk'] > 0.15,
                    is_new_merchant,
                    cat_freq < 0.1,
                    hr_freq < 0.1,
                    amt_percentile > 0.85
                ])
                
                if support_count >= 2:  # Was 3, now 2
                    is_suspicious = True
                    tier = 3
                    confidence = 75
                    detector.stats['tier3'] += 1
                    reasons.append(f"ML{ml_score:.0f}")
                    if z_amt > 2.5: reasons.append(f"HighAmt")
                    if z_dist > 2.5: reasons.append(f"FarLoc")
                    if velocity_10m >= 2: reasons.append(f"Fast")
        
        reason_str = "|".join(reasons[:4]) if reasons else f"T{tier}"
        
        # ===== UPDATE STATS =====
        if fraud == 1 and is_suspicious:
            detector.stats['tp'] += 1
        elif fraud == 0 and is_suspicious:
            detector.stats['fp'] += 1
        elif fraud == 0 and not is_suspicious:
            detector.stats['tn'] += 1
        elif fraud == 1 and not is_suspicious:
            detector.stats['fn'] += 1
        
        # Learn continuously
        try:
            detector.model_main.learn_one(feats, fraud)
            detector.model_validator.learn_one(feats, fraud)
        except:
            pass
        
        profiler.update(cc, amt, dist, hr, cat, merch, ut, fraud)
        
        # Publish alert
        if is_suspicious:
            detector.stats['alerts'] += 1
            
            alert = {
                'trans_num': data['trans_num'],
                'cc_num': cc,
                'merchant': merch,
                'category': cat,
                'amt': amt,
                'location': f"{data['city']}, {data['state']}",
                'risk_score': confidence,
                'reasons': reason_str,
                'confidence': confidence,
                'actual_fraud': fraud,
                'tier': tier,
                'ml_score': round(ml_score, 1)
            }
            
            await pub_nc.publish("fraud.alerts", json.dumps(alert).encode())
            
            if detector.stats['alerts'] % 50 == 0:
                tp, fp = detector.stats['tp'], detector.stats['fp']
                prec = tp / max(1, tp + fp) * 100
                print(f"[ALERT] #{detector.stats['alerts']:>6}"
                      f"T{tier} | {reason_str[:35]}")
    
    except Exception as e:
        print(f"[ERROR] {e}")


async def main():
    print("═══════════════════════════════════════════════════════════")
    print("  ULTRA-PRECISION FRAUD DETECTOR v6.1")
    print("═══════════════════════════════════════════════════════════")
    print()
    
    nc = NATS()
    try:
        await nc.connect(servers=["nats://localhost:4222"], max_reconnect_attempts=60)
        print("✓ Connected to NATS")
    except Exception as e:
        print(f"❌ Failed: {e}")
        return
    
    print("✓ Subscribed: fraud.transactions")
    print("✓ Publishing: fraud.alerts")
    print()
    print("🎯 Ultra-Precision Pipeline Active...\n")
    
    async def handler(msg):
        try:
            data = json.loads(msg.data.decode())
            await detect_fraud_ultra(data, nc)
        except:
            pass
    
    await nc.subscribe("fraud.transactions", cb=handler)
    
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("\n\n═══════════════════════════════════════════════════════════")
        print("    FINAL RESULTS")
        print("═══════════════════════════════════════════════════════════")
    finally:
        try:
            await nc.close()
        except:
            pass

if __name__ == "__main__":
    asyncio.run(main())

    print(f"Total Transactions:    {detector.stats['total']:>10,}")
    print(f"Alerts Published:      {detector.stats['alerts']:>10,}")
    print(f"  - Tier 1 (Certain):  {detector.stats['tier1']:>10,}")
    print(f"  - Tier 2 (Strong):   {detector.stats['tier2']:>10,}")
    print(f"  - Tier 3 (ML-based): {detector.stats['tier3']:>10,}")
    print()
    
    print("═══════════════════════════════════════════════════════════")