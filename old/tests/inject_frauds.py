"""
Fraud Injection Test - ALL FRAUD PATTERNS SUPPORTED
====================================================
Generates injections for all fraud indicators from report generator
"""

import time
import json
import csv
import random
import os
import threading
import re
from pathlib import Path
from datetime import datetime, timedelta
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import pathway as pw

# ============================================================================
# PATHWAY SCHEMA (Shared with Publisher)
# ============================================================================

class TransactionSchema(pw.Schema):
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
    trans_num: str = pw.column_definition(dtype=str)
    unix_time: int = pw.column_definition(dtype=int)
    merch_lat: float = pw.column_definition(dtype=float)
    merch_long: float = pw.column_definition(dtype=float)
    is_fraud: int = pw.column_definition(dtype=int)

# ============================================================================
# CONFIGURATION
# ============================================================================

BASE_DIR = Path(__file__).resolve().parent.parent
REPORTS_DIR = BASE_DIR / "fraud_reports"
STREAM_CSV = BASE_DIR / "fraud_stream.csv"
INJECTION_LOG = BASE_DIR / "injection_log.json"

MAX_INJECTIONS = 20  # Increased for diverse patterns

# Ensure directories exist
REPORTS_DIR.mkdir(exist_ok=True)
STREAM_CSV.parent.mkdir(exist_ok=True)
INJECTION_LOG.parent.mkdir(exist_ok=True)

# Global State
injection_tracker = {
    'injections': [],
    'statistics': {
        'total_injected': 0,
        'total_detected': 0,
        'self_triggered_reports_ignored': 0,
        'avg_detection_time_ms': 0,
        'min_detection_time_ms': float('inf'),
        'max_detection_time_ms': 0,
        'patterns_injected': {}
    }
}

tracker_lock = threading.Lock()

# ============================================================================
# COMPREHENSIVE FRAUD PATTERN LIBRARY
# ============================================================================

FRAUD_PATTERNS = {
    # Velocity/Burst Patterns
    'EXTREME_BURST': {
        'description': 'Extreme Transaction Burst - 4+ transactions in 5 minutes',
        'amt_multiplier': 1.8,
        'burst_count': 4,
        'burst_window_seconds': 300,
        'distance_multiplier': 1.2,
        'time_variance_seconds': 60
    },
    'MAJOR_BURST': {
        'description': 'Major Transaction Burst - 5+ transactions in 10 minutes',
        'amt_multiplier': 2.0,
        'burst_count': 5,
        'burst_window_seconds': 600,
        'distance_multiplier': 1.0
    },
    'BURST': {
        'description': 'Transaction Burst - 3+ transactions in 5 minutes',
        'amt_multiplier': 1.5,
        'burst_count': 3,
        'burst_window_seconds': 300,
        'distance_multiplier': 1.0
    },
    'FastBurst': {
        'description': 'Fast Burst - 4+ transactions in 10 minutes',
        'amt_multiplier': 1.6,
        'burst_count': 4,
        'burst_window_seconds': 600
    },
    'Rapid': {
        'description': 'Rapid Transactions - 5+ in 15 minutes',
        'amt_multiplier': 1.4,
        'burst_count': 5,
        'burst_window_seconds': 900
    },
    
    # Amount Anomalies
    'MASSIVE_AMT': {
        'description': 'Massive Amount Spike - 4.5+ std deviations',
        'amt_multiplier': 6.0,
        'distance_multiplier': 1.0
    },
    'HUGE_AMT': {
        'description': 'Huge Amount Anomaly - 3.8+ std dev, over $500',
        'amt_multiplier': 5.0,
        'min_amount': 500,
        'distance_multiplier': 1.0
    },
    'VeryHighAmt': {
        'description': 'Very High Amount - 3.5+ std dev',
        'amt_multiplier': 4.5,
        'distance_multiplier': 1.2
    },
    'HighAmt': {
        'description': 'High Amount - 3+ std dev',
        'amt_multiplier': 3.5,
        'distance_multiplier': 1.0
    },
    'UnusualAmt': {
        'description': 'Unusual Amount - 2.5+ std dev, top 10%',
        'amt_multiplier': 2.8,
        'distance_multiplier': 1.0
    },
    
    # Distance/Location Anomalies
    'EXTREME_DIST': {
        'description': 'Extreme Distance - 4+ std dev from home',
        'amt_multiplier': 2.0,
        'distance_multiplier': 5.0,
        'min_distance_km': 300
    },
    'VERY_FAR': {
        'description': 'Very Far Location - 3.5+ std dev, over 100km',
        'amt_multiplier': 2.5,
        'distance_multiplier': 4.5,
        'min_distance_km': 100
    },
    'VeryFar': {
        'description': 'Very Far Distance - 3.5+ std dev',
        'amt_multiplier': 1.8,
        'distance_multiplier': 4.0
    },
    'Far': {
        'description': 'Far Location - 3+ std dev',
        'amt_multiplier': 1.5,
        'distance_multiplier': 3.5
    },
    'UnusualDist': {
        'description': 'Unusual Distance - 2.5+ std dev',
        'amt_multiplier': 1.3,
        'distance_multiplier': 3.0
    },
    'FarLoc': {
        'description': 'Far Location Transaction',
        'amt_multiplier': 1.2,
        'distance_multiplier': 2.5
    },
    
    # Merchant Risk
    'FRAUD_MERCHANT': {
        'description': 'High-Fraud Merchant - 40%+ fraud rate',
        'amt_multiplier': 2.5,
        'risky_merchant': True,
        'merchant_prefix': 'FRAUD_',
        'suspicious_categories': ['misc_net', 'shopping_net', 'entertainment']
    },
    'BAD_MERCHANT': {
        'description': 'Risky Merchant - 35%+ fraud rate',
        'amt_multiplier': 2.2,
        'risky_merchant': True,
        'merchant_prefix': 'RISKY_',
        'suspicious_categories': ['shopping_net', 'gas_transport']
    },
    'RiskyMerch': {
        'description': 'Risky Merchant Category - 30%+ fraud rate',
        'amt_multiplier': 2.0,
        'risky_merchant': True,
        'merchant_prefix': 'SUSPECT_'
    },
    
    # Combination Patterns
    'NewMerch+High': {
        'description': 'New Merchant + High Amount (over $500)',
        'amt_multiplier': 3.0,
        'min_amount': 500,
        'force_new_merchant': True,
        'merchant_prefix': 'NEW_'
    },
    'RareCat+High': {
        'description': 'Rare Category + High Amount (over $600)',
        'amt_multiplier': 3.2,
        'min_amount': 600,
        'category_change': True,
        'rare_categories': ['personal_care', 'health_fitness', 'home']
    },
    'Burst+Amt': {
        'description': 'Burst + Amount Spike',
        'amt_multiplier': 3.0,
        'burst_count': 3,
        'burst_window_seconds': 600,
        'distance_multiplier': 1.5
    },
    'Amt+Dist': {
        'description': 'Amount + Distance Anomaly',
        'amt_multiplier': 3.5,
        'distance_multiplier': 3.5
    },
    
    # Pattern Breaks
    'NewMerch': {
        'description': 'New Merchant - First transaction',
        'amt_multiplier': 1.8,
        'force_new_merchant': True,
        'merchant_prefix': 'FIRST_'
    },
    'RareCat': {
        'description': 'Rare Category - < 5% of history',
        'amt_multiplier': 1.5,
        'category_change': True,
        'rare_categories': ['kids_pets', 'personal_care', 'home']
    },
    'UnusualHour': {
        'description': 'Unusual Hour - < 3% of history',
        'amt_multiplier': 1.6,
        'hour_shift': random.choice([2, 3, 4]),  # 2-4 AM
        'unusual_hours': [2, 3, 4]
    },
    'LateOnline': {
        'description': 'Late Night Online Purchase (1-5 AM, over $400)',
        'amt_multiplier': 2.5,
        'min_amount': 400,
        'hour_shift': random.choice([1, 2, 3, 4]),
        'force_online': True,
        'online_categories': ['shopping_net', 'misc_net']
    },
    
    # Customer History (Simulated)
    'FRAUD_HISTORY': {
        'description': 'Multiple Fraud History - 3+ previous frauds',
        'amt_multiplier': 2.8,
        'distance_multiplier': 2.0,
        'simulate_history': True
    },
    'REPEAT_FRAUD': {
        'description': 'Repeat Fraud Pattern - 2+ previous + high amount',
        'amt_multiplier': 3.5,
        'distance_multiplier': 1.5,
        'simulate_history': True
    },
    'PrevFraud': {
        'description': 'Previous Fraud - 2+ confirmed incidents',
        'amt_multiplier': 2.5,
        'distance_multiplier': 1.3,
        'simulate_history': True
    },
    
    # ML Detection Pattern
    'ML': {
        'description': 'ML Detection - Complex anomaly pattern',
        'amt_multiplier': 2.5,
        'distance_multiplier': 2.5,
        'category_change': True,
        'hour_shift': random.choice([0, 2, 3]),
        'ml_signature': True
    }
}

# ============================================================================
# SCHEMA UTILITIES
# ============================================================================

def get_schema_field_order():
    return list(TransactionSchema.keys())

def validate_transaction(txn_dict):
    validated = {}
    for field_name in TransactionSchema.keys():
        col_def = TransactionSchema[field_name]
        dtype = col_def.dtype
        value = txn_dict.get(field_name)
        
        if value is None:
            if field_name == 'is_fraud':
                value = 0
            else:
                raise ValueError(f"Missing required field: {field_name}")
        
        try:
            if dtype == int:
                validated[field_name] = int(float(str(value)))
            elif dtype == float:
                validated[field_name] = float(value)
            else:
                validated[field_name] = str(value)
        except (ValueError, TypeError) as e:
            raise TypeError(f"Field '{field_name}' error converting {value} to {dtype.__name__}: {e}")
    
    return validated

# ============================================================================
# PDF REPORT PARSER (Enhanced)
# ============================================================================

class ReportParser:
    @staticmethod
    def parse_report_filename(filename):
        pattern = r'FRAUD_(\d{8}_\d{6})_([^_]+)_CC(\d{4})\.pdf'
        match = re.match(pattern, filename)
        
        if match:
            timestamp_str, reason, cc_last4 = match.groups()
            try:
                timestamp = datetime.strptime(timestamp_str, '%Y%m%d_%H%M%S')
                return {
                    'timestamp': timestamp,
                    'reason_signature': reason,
                    'cc_last4': cc_last4
                }
            except ValueError:
                return None
        return None
    
    @staticmethod
    def extract_pattern_from_report(report_path):
        filename = Path(report_path).name
        parsed = ReportParser.parse_report_filename(filename)
        
        if not parsed:
            return None
        
        reason = parsed['reason_signature']
        
        # Try to match exact pattern
        pattern_key = 'ML'  # Default fallback
        for key in FRAUD_PATTERNS.keys():
            if key.lower() in reason.lower():
                pattern_key = key
                break
        
        return {
            'report_path': str(report_path),
            'report_time': parsed['timestamp'],
            'cc_last4': parsed['cc_last4'],
            'reason_signature': reason,
            'pattern_template': FRAUD_PATTERNS[pattern_key].copy(),
            'pattern_type': pattern_key
        }

# ============================================================================
# ENHANCED PATHWAY FRAUD INJECTOR
# ============================================================================

class PathwaySchemaInjector:
    def __init__(self, stream_csv_path):
        self.stream_csv = Path(stream_csv_path)
        self.last_transactions = {}
        self.injection_count = 0
        self.field_order = get_schema_field_order()
        self._load_customer_baselines()
    
    def _load_customer_baselines(self):
        baseline_file = BASE_DIR / 'fraudTrain.csv'
        if not baseline_file.exists():
            baseline_file = Path('fraudTrain.csv')

        try:
            if baseline_file.exists():
                with open(baseline_file, 'r', encoding='utf-8-sig') as f:
                    reader = csv.DictReader(f)
                    for i, row in enumerate(reader):
                        if i > 5000:
                            break
                        try:
                            cc_raw = row.get('cc_num', 0)
                            cc_num = int(float(cc_raw))
                            
                            row['cc_num'] = cc_num
                            if 'zip' in row: row['zip'] = int(float(row['zip']))
                            if 'city_pop' in row: row['city_pop'] = int(float(row['city_pop']))
                            if 'is_fraud' in row: row['is_fraud'] = int(float(row['is_fraud']))

                            self.last_transactions[cc_num] = row
                        except (ValueError, TypeError):
                            continue
                            
                print(f"✓ Loaded baselines for {len(self.last_transactions):,} customers")
            else:
                print("⚠️ fraudTrain.csv not found. Using fallback baselines.")
        except Exception as e:
            print(f"⚠️ Error loading baselines: {e}")
    
    def generate_fraudulent_transaction(self, pattern_data):
        """Generate fraud transaction with comprehensive pattern support"""
        cc_last4 = pattern_data['cc_last4']
        matching = [d for cc, d in self.last_transactions.items() if str(cc)[-4:] == cc_last4]
        
        if matching:
            base_txn = matching[0]
            cc_num = int(base_txn['cc_num'])
        else:
            cc_num = int(f"400000000000{cc_last4}")
            base_txn = {
                'amt': '50.00', 'lat': '40.0', 'long': '-74.0',
                'merch_lat': '40.0', 'merch_long': '-74.0',
                'category': 'misc_net', 'merchant': 'unknown_merchant',
                'first': 'John', 'last': 'Doe', 'gender': 'M',
                'street': '123 Fake St', 'city': 'Nowhere', 'state': 'NY',
                'zip': '10001', 'city_pop': '10000', 'job': 'Tester', 'dob': '1990-01-01'
            }
        
        template = pattern_data['pattern_template']
        current_time = datetime.now()
        
        # Amount transformation
        base_amt = float(base_txn['amt'])
        new_amt = base_amt * template.get('amt_multiplier', 1.0)
        
        # Apply minimum amount if specified
        if 'min_amount' in template:
            new_amt = max(new_amt, template['min_amount'])
        
        # Location transformation
        base_lat, base_long = float(base_txn['lat']), float(base_txn['long'])
        dist_mult = template.get('distance_multiplier', 0)
        
        if dist_mult > 1:
            # Scale distance by multiplier
            lat_shift = (random.random() - 0.5) * 10 * dist_mult
            long_shift = (random.random() - 0.5) * 10 * dist_mult
            merch_lat = base_lat + lat_shift
            merch_long = base_long + long_shift
        else:
            merch_lat = float(base_txn.get('merch_lat', base_lat))
            merch_long = float(base_txn.get('merch_long', base_long))
        
        # Time transformation
        if 'hour_shift' in template:
            hour = template['hour_shift']
            current_time = current_time.replace(hour=hour, minute=random.randint(0, 59))
        elif 'unusual_hours' in template:
            hour = random.choice(template['unusual_hours'])
            current_time = current_time.replace(hour=hour, minute=random.randint(0, 59))
        
        # Category transformation
        category = base_txn['category']
        if template.get('force_online', False):
            category = random.choice(['shopping_net', 'misc_net'])
        elif template.get('category_change', False):
            if 'rare_categories' in template:
                category = random.choice(template['rare_categories'])
            else:
                category = random.choice(['entertainment', 'personal_care', 'home'])
        elif 'online_categories' in template:
            category = random.choice(template['online_categories'])
        elif 'suspicious_categories' in template:
            category = random.choice(template['suspicious_categories'])
        
        # Merchant transformation
        merchant = base_txn['merchant']
        if template.get('risky_merchant', False):
            prefix = template.get('merchant_prefix', 'FRAUD_')
            merchant = f"{prefix}{random.randint(100, 999)}"
        elif template.get('force_new_merchant', False):
            prefix = template.get('merchant_prefix', 'NEW_')
            merchant = f"{prefix}merchant_{random.randint(1000, 9999)}"
        
        self.injection_count += 1
        trans_num = f"INJECT_{current_time.strftime('%H%M%S')}_{self.injection_count:04d}"
        
        # Build transaction
        transaction = {
            'trans_num': trans_num,
            'trans_date_trans_time': current_time.strftime('%Y-%m-%d %H:%M:%S'),
            'cc_num': cc_num,
            'merchant': merchant,
            'category': category,
            'amt': round(new_amt, 2),
            'first': base_txn['first'],
            'last': base_txn['last'],
            'gender': base_txn['gender'],
            'street': base_txn['street'],
            'city': base_txn['city'],
            'state': base_txn['state'],
            'zip': base_txn['zip'],
            'lat': base_lat,
            'long': base_long,
            'city_pop': base_txn['city_pop'],
            'job': base_txn['job'],
            'dob': base_txn['dob'],
            'unix_time': int(current_time.timestamp()),
            'merch_lat': round(merch_lat, 6),
            'merch_long': round(merch_long, 6),
            'is_fraud': 1
        }
        
        # Handle burst patterns (inject multiple transactions)
        if 'burst_count' in template:
            return self._generate_burst_transactions(transaction, template)
        
        return validate_transaction(transaction)
    
    def _generate_burst_transactions(self, base_txn, template):
        """Generate multiple transactions for burst patterns"""
        burst_count = template.get('burst_count', 3)
        window_seconds = template.get('burst_window_seconds', 300)
        
        transactions = []
        base_time = datetime.fromisoformat(base_txn['trans_date_trans_time'])
        
        for i in range(burst_count):
            # Spread transactions within the window
            offset_seconds = random.randint(0, window_seconds)
            txn_time = base_time + timedelta(seconds=offset_seconds)
            
            txn = base_txn.copy()
            txn['trans_date_trans_time'] = txn_time.strftime('%Y-%m-%d %H:%M:%S')
            txn['unix_time'] = int(txn_time.timestamp())
            txn['trans_num'] = f"{base_txn['trans_num']}_BURST{i+1}"
            
            # Vary amount slightly
            variance = template.get('time_variance_seconds', 0.1)
            txn['amt'] = round(base_txn['amt'] * (1 + random.uniform(-variance, variance)), 2)
            
            transactions.append(validate_transaction(txn))
        
        return transactions

    def inject_to_stream(self, transaction_or_list):
        """Inject single transaction or burst (list of transactions)"""
        transactions = transaction_or_list if isinstance(transaction_or_list, list) else [transaction_or_list]
        
        records = []
        for transaction in transactions:
            if not self.stream_csv.exists():
                print("❌ Stream file disappeared!")
                return None
            
            injection_timestamp = time.time()
            row_values = [transaction[field] for field in self.field_order]
            
            import io
            output = io.StringIO()
            writer = csv.writer(output, lineterminator='')
            writer.writerow(row_values)
            csv_line = output.getvalue() + '\n'
            
            max_retries = 10
            for attempt in range(max_retries):
                try:
                    with open(self.stream_csv, 'a') as f:
                        f.write(csv_line)
                        f.flush()
                        os.fsync(f.fileno())
                    break
                except (PermissionError, IOError) as e:
                    if attempt < max_retries - 1:
                        time.sleep(0.3 + (attempt * 0.1))
                    else:
                        print(f"❌ Failed to write after {max_retries} attempts: {e}")
                        return None
            
            # Ensure amt is float for record storage and display
            amt_value = float(transaction['amt']) if isinstance(transaction['amt'], str) else transaction['amt']
            
            record = {
                'trans_num': transaction['trans_num'],
                'injection_timestamp': injection_timestamp,
                'injection_time': datetime.fromtimestamp(injection_timestamp).isoformat(),
                'cc_num': transaction['cc_num'],
                'amt': amt_value,
                'detected': False
            }
            
            records.append(record)
            
            with tracker_lock:
                injection_tracker['injections'].append(record)
                injection_tracker['statistics']['total_injected'] += 1
            
            print(f"💉 Injected: {transaction['trans_num']} | ${amt_value:.2f}")
        
        return records

# ============================================================================
# REPORT MONITOR (Enhanced)
# ============================================================================

class ReportMonitor(FileSystemEventHandler):
    def __init__(self, injector):
        self.injector = injector
        self.parser = ReportParser()
    
    def _wait_for_file_stable(self, filepath, timeout=5):
        start = time.time()
        last_size = -1
        while time.time() - start < timeout:
            try:
                size = filepath.stat().st_size
                if size > 0 and size == last_size:
                    return True
                last_size = size
                time.sleep(0.5)
            except OSError:
                pass
        return False

    def on_created(self, event):
        if event.is_directory or not event.src_path.endswith('.pdf'):
            return

        detection_timestamp = time.time()
        filepath = Path(event.src_path)
        
        if not self._wait_for_file_stable(filepath):
            return

        print(f"\n📄 Report: {filepath.name}")
        
        parsed = self.parser.parse_report_filename(filepath.name)
        if not parsed:
            return

        if self._is_self_triggered(parsed['cc_last4'], detection_timestamp):
            print(f"   🚫 Self-triggered (skipping)")
            with tracker_lock:
                injection_tracker['statistics']['self_triggered_reports_ignored'] += 1
            return

        with tracker_lock:
            if injection_tracker['statistics']['total_injected'] >= MAX_INJECTIONS:
                print(f"   ⛔ Limit reached ({MAX_INJECTIONS})")
                return

        try:
            pattern = self.parser.extract_pattern_from_report(event.src_path)
            pattern_type = pattern['pattern_type']
            
            print(f"   🎯 Pattern: {pattern_type}")
            print(f"   📝 {FRAUD_PATTERNS[pattern_type]['description']}")
            
            # Track pattern usage
            with tracker_lock:
                patterns_used = injection_tracker['statistics']['patterns_injected']
                patterns_used[pattern_type] = patterns_used.get(pattern_type, 0) + 1
            
            fraud_txn = self.injector.generate_fraudulent_transaction(pattern)
            records = self.injector.inject_to_stream(fraud_txn)
            
            if records:
                print(f"   ✅ Injected {len(records) if isinstance(records, list) else 1} transaction(s)")
                
        except Exception as e:
            print(f"   ❌ Error: {e}")
            import traceback
            traceback.print_exc()

    def _is_self_triggered(self, report_cc_last4, detection_timestamp):
        cutoff_time = time.time() - 300
        
        with tracker_lock:
            for inj in injection_tracker['injections']:
                inj_time = inj['injection_timestamp']
                inj_cc_last4 = str(inj['cc_num'])[-4:]
                
                if inj_time > cutoff_time and inj_cc_last4 == report_cc_last4:
                    if not inj['detected']:
                        self._mark_detected(inj, detection_timestamp)
                    return True
        return False

    def _mark_detected(self, record, detection_timestamp):
        injection_time = record['injection_timestamp']
        latency_ms = (detection_timestamp - injection_time) * 1000
        
        record['detected'] = True
        record['detection_timestamp'] = detection_timestamp
        record['latency_ms'] = latency_ms
        
        stats = injection_tracker['statistics']
        stats['total_detected'] += 1
        stats['min_detection_time_ms'] = min(stats['min_detection_time_ms'], latency_ms)
        stats['max_detection_time_ms'] = max(stats['max_detection_time_ms'], latency_ms)
        
        detected = [i['latency_ms'] for i in injection_tracker['injections'] if i.get('detected')]
        if detected:
            stats['avg_detection_time_ms'] = sum(detected) / len(detected)
            
        print(f"   ✅ DETECTED in {latency_ms:.2f}ms")

# ============================================================================
# RUNNER
# ============================================================================

def run_injection_test():
    print("═" * 70)
    print("  COMPREHENSIVE FRAUD PATTERN INJECTOR")
    print("═" * 70)
    print(f"✓ Stream: {STREAM_CSV}")
    print(f"✓ {len(FRAUD_PATTERNS)} pattern types supported")
    print(f"✓ Max injections: {MAX_INJECTIONS}")
    print()
    
    injector = PathwaySchemaInjector(STREAM_CSV)
    handler = ReportMonitor(injector)
    
    observer = Observer()
    observer.schedule(handler, str(REPORTS_DIR), recursive=False)
    observer.start()
    
    print(f"👀 Monitoring: {REPORTS_DIR}")
    print("\nPress Ctrl+C to stop.\n")
    
    try:
        while True:
            time.sleep(1)
            
            with tracker_lock:
                if (injection_tracker['statistics']['total_injected'] >= MAX_INJECTIONS and
                    injection_tracker['statistics']['total_detected'] >= MAX_INJECTIONS):
                    print("\n🎉 All injections detected!")
                    time.sleep(5)
                    break
                    
    except KeyboardInterrupt:
        pass
    
    observer.stop()
    print("\n" + "═" * 70)
    print("  FINAL STATISTICS")
    print("═" * 70)
    
    stats = injection_tracker['statistics'].copy()
    if stats['min_detection_time_ms'] == float('inf'):
        stats['min_detection_time_ms'] = 0
    
    print(f"\nTotal Injected: {stats['total_injected']}")
    print(f"Total Detected: {stats['total_detected']}")
    print(f"Detection Rate: {(stats['total_detected']/stats['total_injected']*100):.1f}%")
    print(f"Self-Triggered Reports Ignored: {stats['self_triggered_reports_ignored']}")
    print(f"\nDetection Latency:")
    print(f"  Average: {stats['avg_detection_time_ms']:.2f}ms")
    print(f"  Min: {stats['min_detection_time_ms']:.2f}ms")
    print(f"  Max: {stats['max_detection_time_ms']:.2f}ms")
    
    print(f"\n{'─' * 70}")
    print("  PATTERNS INJECTED")
    print("─" * 70)
    for pattern, count in sorted(stats['patterns_injected'].items(), key=lambda x: x[1], reverse=True):
        desc = FRAUD_PATTERNS.get(pattern, {}).get('description', 'Unknown')
        print(f"{pattern:20s} | {count:3d}x | {desc}")
    
    print("\n" + "─" * 70)
    print("  INDIVIDUAL INJECTION RESULTS")
    print("─" * 70)
    
    for i, inj in enumerate(injection_tracker['injections'], 1):
        status = "✅ Detected" if inj.get('detected') else "⏰ Pending"
        latency = f"{inj.get('latency_ms', 0):.2f}ms" if inj.get('detected') else "N/A"
        amt = f"${inj['amt']:.2f}"
        print(f"{i:3d}. {inj['trans_num']:30s} | {status:12s} | {amt:10s} | Latency: {latency}")
    
    # Save detailed log
    with open(INJECTION_LOG, 'w') as f:
        json.dump(injection_tracker, f, indent=2, default=str)
    
    print(f"\n💾 Full log: {INJECTION_LOG}")
    print("\n" + "═" * 70)
    print("  Pattern Coverage Summary")
    print("═" * 70)
    
    # Show which patterns were tested
    tested_patterns = set(stats['patterns_injected'].keys())
    all_patterns = set(FRAUD_PATTERNS.keys())
    untested = all_patterns - tested_patterns
    
    print(f"✓ Tested: {len(tested_patterns)}/{len(all_patterns)} patterns")
    if untested:
        print(f"\n⚠️  Untested patterns (no reports triggered these):")
        for pattern in sorted(untested):
            print(f"   - {pattern}: {FRAUD_PATTERNS[pattern]['description']}")
    
    observer.join()


if __name__ == "__main__":
    run_injection_test()