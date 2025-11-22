import pathway as pw
import time
import threading

# ───────────────────────────────────────────────────────────────
# CONFIGURATION
# ───────────────────────────────────────────────────────────────

NATS_URI = "nats://localhost:4222"
NATS_TOPIC = "fraud.transactions"

ORIGINAL_FILE = "fraudTrain.csv"
TEMP_STREAM_FILE = "fraud_stream.csv"

# SET YOUR SPEED HERE (Keep < 100 for pure file streaming)
TARGET_TPS = 50 

# ───────────────────────────────────────────────────────────────
# SCHEMA
# ───────────────────────────────────────────────────────────────
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

# ───────────────────────────────────────────────────────────────
# 1. PURE STREAMING WRITER (Row-by-Row)
# ───────────────────────────────────────────────────────────────

def append_to_stream_file(tps):
    print("⏳ Loading source file...")
    with open(ORIGINAL_FILE, "r") as f:
        lines = f.readlines()

    header = lines[0]
    rows = lines[1:]
    total_rows = len(rows)

    # Reset file
    with open(TEMP_STREAM_FILE, "w") as f:
        f.write(header)

    # Calculate precise interval per transaction
    interval = 1.0 / tps

    print(f"🚀 Starting PURE streaming at {tps} TPS")
    print(f"ℹ️  1 Transaction every {interval:.4f} seconds")

    idx = 0
    
    while True:
        loop_start = time.time()

        # 1. Write exactly ONE row
        current_row = rows[idx]
        
        with open(TEMP_STREAM_FILE, "a") as f:
            f.write(current_row)
        
        # 2. Prepare next index
        idx = (idx + 1) % total_rows
        
        # 3. Accurate Sleep (Drift Correction)
        # We calculate how long the write took, and sleep only the remaining time
        elapsed = time.time() - loop_start
        sleep_time = interval - elapsed
        
        if sleep_time > 0:
            time.sleep(sleep_time)
            
        # Optional: Print every 10th row just to keep console clean
        if idx % 10 == 0:
            print(f"→ Transaction {idx} sent.", end='\r')


# ───────────────────────────────────────────────────────────────
# 2. PATHWAY STREAMER
# ───────────────────────────────────────────────────────────────

def run_publisher():
    print("═══════════════════════════════════════════════════════════")
    print("    PATHWAY BANK SIMULATION - PURE STREAMING")
    print("═══════════════════════════════════════════════════════════")
    print(f"Streaming File : {TEMP_STREAM_FILE}")
    print(f"Target TPS     : {TARGET_TPS}")
    print()

    # Start the writer thread
    t = threading.Thread(target=append_to_stream_file, args=(TARGET_TPS,), daemon=True)
    t.start()

    # Pathway reads the stream
    transactions = pw.io.csv.read(
        TEMP_STREAM_FILE,
        schema=TransactionSchema,
        mode='streaming',
        autocommit_duration_ms=100 # Low latency commit
        
    )

    pw.io.nats.write(transactions, uri=NATS_URI, topic=NATS_TOPIC)

    pw.run()

if __name__ == "__main__":
    run_publisher()