import pathway as pw
from datetime import datetime
import time
import shutil
import threading

# NATS Configuration
NATS_URI = "nats://localhost:4222"
NATS_TOPIC = "fraud.transactions"

ORIGINAL_FILE = "fraudTrain.csv"
TEMP_STREAM_FILE = "fraud_stream.csv"

# Schema stays the same
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
# 1. STREAMING FILE WRITER
# ───────────────────────────────────────────────────────────────

def append_to_stream_file():
    """
    Continuously append rows from fraudTrain.csv to a temporary streaming file.
    Pathway watches this temp file and streams updates whenever new rows arrive.
    """
    print("⏳ Loading source file...")
    with open(ORIGINAL_FILE, "r") as f:
        lines = f.readlines()

    header = lines[0]
    rows = lines[1:]

    # Create / reset the temp stream file
    with open(TEMP_STREAM_FILE, "w") as f:
        f.write(header)

    print(f"📄 Temporary stream file: {TEMP_STREAM_FILE}")
    print("🔁 Starting periodic appending...")

    idx = 0
    total = len(rows)

    while True:
        # Append 1 line per second (or adjust rate)
        with open(TEMP_STREAM_FILE, "a") as f:
            f.write(rows[idx])

        print(f"→ Published row {idx+1}/{total}")

        idx = (idx + 1) % total  # loop forever

        time.sleep(0.2)  # adjust streaming speed here


# ───────────────────────────────────────────────────────────────
# 2. PATHWAY STREAMER (READS ONLY TEMP FILE)
# ───────────────────────────────────────────────────────────────

def run_publisher():
    print("═══════════════════════════════════════════════════════════")
    print("    PATHWAY TRANSACTION PUBLISHER v8.0 - TRUE STREAMING")
    print("═══════════════════════════════════════════════════════════")
    print()
    print(f"NATS URI  : {NATS_URI}")
    print(f"Topic     : {NATS_TOPIC}")
    print(f"Streaming : {TEMP_STREAM_FILE}")
    print()

    # Start background appending thread
    t = threading.Thread(target=append_to_stream_file, daemon=True)
    t.start()

    # Pathway watches only the temp file
    transactions = pw.io.csv.read(
        TEMP_STREAM_FILE,
        schema=TransactionSchema,
        mode='streaming',
        autocommit_duration_ms=200
    )

    print("✓ Pathway streaming initialized")
    print(f"✓ Output → NATS topic: {NATS_TOPIC}")
    print("🚀 Live streaming active. CTRL+C to stop.")
    print()

    pw.io.nats.write(transactions, uri=NATS_URI, topic=NATS_TOPIC)

    pw.run()


# Run if executed directly
if __name__ == "__main__":
    run_publisher()
