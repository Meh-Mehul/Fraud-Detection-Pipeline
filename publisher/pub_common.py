"""
Combined Publisher - Runs both feedback and detector publishers concurrently.

FEEDBACK PUBLISHER:
  - Publishes ENTIRE dataset (fraud + legitimate) to fraud.feedback
  - Includes is_fraud column (ground truth labels)
  - Used for model training

DETECTOR PUBLISHER:
  - Publishes transactions to fraud.transactions
  - REMOVES is_fraud column (detector doesn't have access to labels)
  - Used for real-time fraud detection
"""

import time
import threading
import os
from pathlib import Path
import pathway as pw
import sys
from multiprocessing import Process

# Add project root to python path
ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))

# Configuration
ORIGINAL_FILE = "fraudTrain.csv"
TEMP_FEEDBACK_FILE = "./publisher/temp_feed_stream.csv"
TEMP_DETECTOR_FILE = "./publisher/temp_det_stream.csv"
NATS_URI = os.environ.get("NATS_URI", "nats://localhost:4222")
FEEDBACK_TOPIC = "fraud.feedback"
DETECTOR_TOPIC = "fraud.transactions"
TARGET_TPS = 25

# ============================================================================
# SCHEMAS
# ============================================================================

class FeedBackSchema(pw.Schema):
    """Schema for feedback stream (includes is_fraud label)"""
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


class TransactionSchema(pw.Schema):
    """Schema for detector stream (NO is_fraud column)"""
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
# UTILITY FUNCTIONS
# ============================================================================

def verify_fraud_column():
    """Verify that is_fraud column exists and show class distribution."""
    with open(ORIGINAL_FILE, "r") as f:
        header = f.readline().strip().split(",")
        
        if "is_fraud" not in header:
            print("❌ ERROR: is_fraud column not found in CSV!")
            print(f"   Available columns: {header}")
            return False
        
        fraud_idx = header.index("is_fraud")
        print(f"✓ Found is_fraud column at index {fraud_idx}")
        
        # Analyze class distribution
        fraud_count = 0
        legit_count = 0
        
        for line in f:
            parts = line.strip().split(",")
            if len(parts) > fraud_idx:
                if parts[fraud_idx] == "1":
                    fraud_count += 1
                else:
                    legit_count += 1
        
        total = fraud_count + legit_count
        fraud_pct = (fraud_count / total * 100) if total > 0 else 0
        legit_pct = (legit_count / total * 100) if total > 0 else 0
        
        print(f"\n📊 Dataset Class Distribution:")
        print(f"   Total samples: {total:,}")
        print(f"   Fraudulent:    {fraud_count:,} ({fraud_pct:.2f}%)")
        print(f"   Legitimate:    {legit_count:,} ({legit_pct:.2f}%)")
        print(f"   Class ratio:   1:{legit_count/fraud_count:.1f}" if fraud_count > 0 else "")
        
        if fraud_count == 0:
            print("⚠️  WARNING: No fraud cases found in dataset!")
            return False
        
        if fraud_pct < 0.1:
            print(f"⚠️  WARNING: Very low fraud rate ({fraud_pct:.3f}%). Model may struggle.")
        
        return True


def strip_is_fraud(row: str) -> str:
    """Remove the last column (is_fraud) from a CSV row."""
    parts = row.rstrip().split(",")
    if len(parts) > 1:
        parts = parts[:-1]
    return ",".join(parts) + "\n"


# ============================================================================
# FEEDBACK PUBLISHER (WITH is_fraud)
# ============================================================================

def stream_feedback_data(tps):
    """Stream the entire dataset continuously (both fraud and legitimate)."""
    with open(ORIGINAL_FILE, "r") as f:
        lines = f.readlines()
    
    header = lines[0]
    rows = lines[1:]
    data = rows
    
    # Analyze what we're streaming
    fraud_idx = header.strip().split(",").index("is_fraud")
    fraud_count = sum(1 for row in data if row.strip().split(",")[fraud_idx] == "1")
    legit_count = len(data) - fraud_count
    
    print(f"\n📊 Feedback Stream Content:")
    print(f"   Total transactions: {len(data):,}")
    print(f"   Fraudulent:         {fraud_count:,} ({fraud_count/len(data)*100:.2f}%)")
    print(f"   Legitimate:         {legit_count:,} ({legit_count/len(data)*100:.2f}%)")
    print(f"\n   ℹ️  Both classes are necessary for training:")
    print(f"      • Fraud samples teach the model what fraud looks like")
    print(f"      • Legitimate samples teach what normal behavior is")
    print(f"      • The model learns to distinguish between them")

    # Prepare temp streaming file
    Path(TEMP_FEEDBACK_FILE).parent.mkdir(parents=True, exist_ok=True)
    with open(TEMP_FEEDBACK_FILE, "w") as f:
        f.write(header)

    interval = 1.0 / tps
    idx = 0
    sent_fraud = 0
    sent_legit = 0
    
    print(f"\n📤 [FEEDBACK] Streaming at {tps} TPS to topic: {FEEDBACK_TOPIC}")
    print("=" * 70)

    while True:
        t0 = time.time()
        
        # Track class distribution
        row_fraud_label = data[idx].strip().split(",")[fraud_idx]
        if row_fraud_label == "1":
            sent_fraud += 1
        else:
            sent_legit += 1
        
        # Append to streaming file
        with open(TEMP_FEEDBACK_FILE, "a") as f:
            f.write(data[idx])
        
        idx = (idx + 1) % len(data)
        
        # Regulate TPS
        elapsed = time.time() - t0
        sleep = interval - elapsed
        if sleep > 0:
            time.sleep(sleep)
        
        # Progress logging
        if (sent_fraud + sent_legit) % 100 == 0:
            total_sent = sent_fraud + sent_legit
            fraud_pct = (sent_fraud / total_sent * 100) if total_sent > 0 else 0
            print(f"[FEEDBACK] Sent: {total_sent:,} | Fraud: {sent_fraud} ({fraud_pct:.1f}%) | Legit: {sent_legit}", end="\r")


def run_feedback_publisher():
    """Run the feedback publisher process."""
    print("\n🔵 Starting FEEDBACK Publisher...")
    
    # Start streaming thread
    t = threading.Thread(target=stream_feedback_data, args=(TARGET_TPS,), daemon=True)
    t.start()

    # Pathway reads from temp file and publishes to NATS
    tx = pw.io.csv.read(
        TEMP_FEEDBACK_FILE,
        schema=FeedBackSchema,
        mode="streaming",
        autocommit_duration_ms=100,
    )
    pw.io.nats.write(tx, uri=NATS_URI, topic=FEEDBACK_TOPIC, format="json")
    
    print("✓ Feedback publisher running...\n")
    pw.run()


# ============================================================================
# DETECTOR PUBLISHER (WITHOUT is_fraud)
# ============================================================================

def stream_detector_data(tps):
    """Stream transactions without is_fraud column."""
    with open(ORIGINAL_FILE, "r") as f:
        lines = f.readlines()

    header = lines[0].rstrip().split(",")
    if header[-1] == "is_fraud":
        header = header[:-1]
    header = ",".join(header) + "\n"

    rows = lines[1:]
    data = rows

    # Ensure directory exists
    Path(TEMP_DETECTOR_FILE).parent.mkdir(parents=True, exist_ok=True)

    # Write header without is_fraud
    with open(TEMP_DETECTOR_FILE, "w") as f:
        f.write(header)

    interval = 1.0 / tps
    idx = 0

    print(f"\n📤 [DETECTOR] Streaming {len(data):,} transactions (no is_fraud column)")
    print(f"   Topic: {DETECTOR_TOPIC}")
    print("=" * 70)

    while True:
        t0 = time.time()

        # Remove the last column (is_fraud)
        cleaned = strip_is_fraud(data[idx])

        with open(TEMP_DETECTOR_FILE, "a") as f:
            f.write(cleaned)

        idx = (idx + 1) % len(data)

        # Regulate TPS
        elapsed = time.time() - t0
        sleep = interval - elapsed
        if sleep > 0:
            time.sleep(sleep)

        if idx % 100 == 0:
            print(f"[DETECTOR] Sent: {idx:,} transactions", end="\r")


def run_detector_publisher():
    """Run the detector publisher process."""
    print("\n🟢 Starting DETECTOR Publisher...")
    
    # Start streaming thread
    t = threading.Thread(target=stream_detector_data, args=(TARGET_TPS,), daemon=True)
    t.start()

    # Pathway reads from temp file and publishes to NATS
    tx = pw.io.csv.read(
        TEMP_DETECTOR_FILE,
        schema=TransactionSchema,
        mode="streaming",
        autocommit_duration_ms=100,
    )
    pw.io.nats.write(tx, uri=NATS_URI, topic=DETECTOR_TOPIC, format="json")
    
    print("✓ Detector publisher running...\n")
    pw.run()


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    print("═" * 70)
    print("   COMBINED PUBLISHER - Feedback + Detector")
    print("═" * 70)
    print("\n🎯 This script runs TWO publishers concurrently:")
    print("   1. FEEDBACK Publisher  → fraud.feedback (with is_fraud)")
    print("   2. DETECTOR Publisher  → fraud.transactions (without is_fraud)")
    print()
    
    # Verify data integrity
    if not verify_fraud_column():
        print("\n❌ Exiting due to data issues")
        return
    
    print("\n" + "─" * 70)
    print("   Starting both publishers in separate processes...")
    print("─" * 70)
    
    # Create separate processes for each publisher
    feedback_process = Process(target=run_feedback_publisher, name="FeedbackPublisher")
    detector_process = Process(target=run_detector_publisher, name="DetectorPublisher")
    
    # Start both processes
    feedback_process.start()
    time.sleep(2)  # Small delay to avoid startup conflicts
    detector_process.start()
    
    print("\n✅ Both publishers are now running!")
    print("   Press Ctrl+C to stop both publishers\n")
    
    try:
        # Keep main thread alive
        feedback_process.join()
        detector_process.join()
    except KeyboardInterrupt:
        print("\n\n⚠️  Shutting down publishers...")
        feedback_process.terminate()
        detector_process.terminate()
        feedback_process.join()
        detector_process.join()
        print("✓ Publishers stopped cleanly")


if __name__ == "__main__":
    main()