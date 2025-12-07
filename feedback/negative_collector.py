"""
Negative Results Collector
Subscribes to fraud.results and stores latest 500 non-fraud transactions
for false negative review in the frontend
"""

import pathway as pw
import json
import time
import os
from pathlib import Path
from collections import deque
import threading

# Configuration
NATS_URI = os.environ.get("NATS_URI", "nats://localhost:4222")
RESULTS_TOPIC = "fraud.results"
OUTPUT_FILE = Path("./negative_transactions.json")
MAX_TRANSACTIONS = 500

# Thread-safe storage for negative transactions
negative_buffer = deque(maxlen=MAX_TRANSACTIONS)
buffer_lock = threading.Lock()

# Persistence config
PERSIST_DIR = Path("./pathway_persistence")
CHECKPOINT_CONFIG = pw.persistence.Config.simple_config(
    pw.persistence.Backend.filesystem(str(PERSIST_DIR / "checkpoints_negative")),
    snapshot_interval_ms=10000
)

# Schema for reading results
class ResultSchema(pw.Schema):
    alert_json: str = pw.column_definition(dtype=str)


def save_buffer_to_file():
    """Save current buffer to JSON file"""
    with buffer_lock:
        transactions = list(negative_buffer)
    
    with open(OUTPUT_FILE, 'w') as f:
        json.dump({
            'count': len(transactions),
            'updated_at': time.strftime('%Y-%m-%d %H:%M:%S'),
            'transactions': transactions
        }, f, indent=2)


@pw.udf
def process_result(alert_json: str) -> str:
    """Process transaction result and store if not an alert (potential false negative)"""
    try:
        data = json.loads(alert_json)
        
        # Only store non-alert transactions (potential false negatives)
        is_alert = data.get('is_alert', False)
        
        if not is_alert:
            # Extract relevant fields for review
            transaction = {
                'trans_num': data.get('trans_num', ''),
                'cc_num': str(data.get('cc_num', '')),
                'amt': data.get('amt', 0),
                'merchant': data.get('merchant', ''),
                'category': data.get('category', ''),
                'location': data.get('location', ''),
                'city': data.get('city', ''),
                'state': data.get('state', ''),
                'first': data.get('first', ''),
                'last': data.get('last', ''),
                'ml_score': data.get('ml_score', 0),
                'risk_score': data.get('risk_score', 0),
                'reasons': data.get('reasons', ''),
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
                'trans_date_trans_time': data.get('trans_date_trans_time', ''),
            }
            
            with buffer_lock:
                negative_buffer.append(transaction)
            
            # Save to file every 50 transactions
            if len(negative_buffer) % 50 == 0:
                save_buffer_to_file()
                print(f"   [SAVE] Saved {len(negative_buffer)} negative transactions")
        
        return json.dumps({'processed': True, 'is_alert': is_alert})
        
    except Exception as e:
        return json.dumps({'error': str(e)})


# Background saver thread
def periodic_save():
    """Periodically save buffer to file"""
    while True:
        time.sleep(10)  # Save every 10 seconds
        save_buffer_to_file()


def run_negative_collector():
    """Main collector function"""
    
    print("╔═══════════════════════════════════════════╗")
    print("║  NEGATIVE RESULTS COLLECTOR               ║")
    print("║  (False Negative Detection Support)       ║")
    print("╚═══════════════════════════════════════════╝")
    print(f"Input: {RESULTS_TOPIC}")
    print(f"Output: {OUTPUT_FILE}")
    print(f"Buffer size: {MAX_TRANSACTIONS}")
    print()
    
    # Start background saver thread
    save_thread = threading.Thread(target=periodic_save, daemon=True)
    save_thread.start()
    
    # Read from fraud.results
    results = pw.io.nats.read(
        uri=NATS_URI,
        topic=RESULTS_TOPIC,
        schema=ResultSchema,
        format="json",
        persistent_id="negative_collector_reader"
    )
    
    # Process each result
    processed = results.select(
        result=process_result(pw.this.alert_json)
    )
    
    # Null writer just to keep the pipeline running
    pw.io.null.write(processed)
    
    print("[OK] Negative collector active")
    print(f"[OK] Storing non-alert transactions to: {OUTPUT_FILE}")
    print()
    
    pw.run(persistence_config=CHECKPOINT_CONFIG)


if __name__ == "__main__":
    run_negative_collector()

