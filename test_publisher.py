"""
Test Data Publisher
Streams FraudTest.csv (WITHOUT labels) to inference detector
"""

import pathway as pw
import time
import threading

# ============================================================================
# CONFIGURATION
# ============================================================================

NATS_URI = "nats://localhost:4222"
NATS_TOPIC = "fraud.test_transactions"

TEST_FILE = "fraudTest.csv"
TEMP_STREAM_FILE = "fraud_test_stream.csv"

TARGET_TPS = 20  # Lower rate for test data review

# ============================================================================
# SCHEMA (NO is_fraud label)
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
    # NOTE: is_fraud column is REMOVED - this is unlabeled test data

# ============================================================================
# STREAMING WRITER
# ============================================================================

def append_to_stream_file(tps):
    """Stream test data row by row"""
    print("⏳ Loading test file...")
    
    try:
        with open(TEST_FILE, "r") as f:
            lines = f.readlines()
    except FileNotFoundError:
        print(f"❌ Test file not found: {TEST_FILE}")
        print("   Please ensure fraudTest.csv exists")
        return
    
    header = lines[0]
    rows = lines[1:]
    total_rows = len(rows)
    
    # Remove is_fraud column from header if present
    header_cols = header.strip().split(',')
    if 'is_fraud' in header_cols:
        fraud_idx = header_cols.index('is_fraud')
        header_cols.pop(fraud_idx)
        header = ','.join(header_cols) + '\n'
        print("✓ Removed 'is_fraud' label from test data")
    
    # Reset file
    with open(TEMP_STREAM_FILE, "w") as f:
        f.write(header)
    
    interval = 1.0 / tps
    
    print(f"🚀 Starting test data stream at {tps} TPS")
    print(f"ℹ️  Total test transactions: {total_rows:,}")
    print()
    
    idx = 0
    
    while idx < total_rows:  # Stream once through test file
        loop_start = time.time()
        
        # Remove is_fraud column from row if present
        current_row = rows[idx].strip().split(',')
        if len(current_row) > len(header_cols):
            current_row.pop(fraud_idx)
        
        current_row_str = ','.join(current_row) + '\n'
        
        with open(TEMP_STREAM_FILE, "a") as f:
            f.write(current_row_str)
        
        idx += 1
        
        elapsed = time.time() - loop_start
        sleep_time = interval - elapsed
        
        if sleep_time > 0:
            time.sleep(sleep_time)
        
        if idx % 100 == 0:
            print(f"→ Test transaction {idx}/{total_rows} ({idx*100//total_rows}%) sent", end='\r')
    
    print(f"\n✅ All {total_rows:,} test transactions sent")
    print("   Inference detector will process and send to dashboard")

# ============================================================================
# PATHWAY PUBLISHER
# ============================================================================

def run_test_publisher():
    print("═══════════════════════════════════════════════════════════")
    print("    TEST DATA PUBLISHER (Unlabeled)")
    print("    Streams FraudTest.csv → Inference → Dashboard")
    print("═══════════════════════════════════════════════════════════")
    print(f"Test File      : {TEST_FILE}")
    print(f"Stream File    : {TEMP_STREAM_FILE}")
    print(f"Target TPS     : {TARGET_TPS}")
    print(f"Output Topic   : {NATS_TOPIC}")
    print()
    
    # Start writer thread
    t = threading.Thread(target=append_to_stream_file, args=(TARGET_TPS,), daemon=True)
    t.start()
    
    # Pathway reads and publishes
    transactions = pw.io.csv.read(
        TEMP_STREAM_FILE,
        schema=UnlabeledTransactionSchema,
        mode='streaming',
        autocommit_duration_ms=100
    )
    
    pw.io.nats.write(transactions, uri=NATS_URI, topic=NATS_TOPIC)
    
    print("✓ Pathway streaming active")
    print()
    
    pw.run()

if __name__ == "__main__":
    run_test_publisher()