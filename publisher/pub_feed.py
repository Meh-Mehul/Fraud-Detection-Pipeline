# pipeline/publisher/pub_feedback.py
"""
Publish ENTIRE dataset (both fraud and non-fraud) to fraud.feedback.
This represents ground truth labels for training - includes both:
  - True frauds (is_fraud=1) 
  - True legitimate transactions (is_fraud=0)
  
The model learns from both classes to improve detection accuracy.
"""
import time, threading
from pathlib import Path
import pathway as pw
import sys

# Add project root to python path
ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))

ORIGINAL_FILE = "fraudTrain.csv"
TEMP_STREAM_FILE = "./publisher/temp_feed_stream.csv"
NATS_URI = "nats://localhost:4222"
NATS_TOPIC = "fraud.feedback"

TARGET_TPS = 20  # Transactions per second


def verify_fraud_column():
    """Verify that is_fraud column exists and show class distribution."""
    with open(ORIGINAL_FILE, "r") as f:
        header = f.readline().strip().split(",")
        
        if "is_fraud" not in header:
            print("[ERROR] ERROR: is_fraud column not found in CSV!")
            print(f"   Available columns: {header}")
            return False
        
        fraud_idx = header.index("is_fraud")
        print(f"[OK] Found is_fraud column at index {fraud_idx}")
        
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
        
        print(f"\n[INFO] Dataset Class Distribution:")
        print(f"   Total samples: {total:,}")
        print(f"   Fraudulent:    {fraud_count:,} ({fraud_pct:.2f}%)")
        print(f"   Legitimate:    {legit_count:,} ({legit_pct:.2f}%)")
        print(f"   Class ratio:   1:{legit_count/fraud_count:.1f}" if fraud_count > 0 else "")
        
        if fraud_count == 0:
            print("[WARN]  WARNING: No fraud cases found in dataset!")
            return False
        
        if fraud_pct < 0.1:
            print(f"[WARN]  WARNING: Very low fraud rate ({fraud_pct:.3f}%). Model may struggle.")
        
        return True


def stream_full_dataset(tps):
    """Stream the entire dataset continuously (both fraud and legitimate)."""
    with open(ORIGINAL_FILE, "r") as f:
        lines = f.readlines()
    
    header = lines[0]
    rows = lines[1:]
    
    # Use ALL rows for feedback (not just first half)
    data = rows
    
    # Analyze what we're about to stream
    fraud_idx = header.strip().split(",").index("is_fraud")
    fraud_count = sum(1 for row in data if row.strip().split(",")[fraud_idx] == "1")
    legit_count = len(data) - fraud_count
    
    print(f"\n[INFO] Feedback Stream Content:")
    print(f"   Total transactions: {len(data):,}")
    print(f"   Fraudulent:         {fraud_count:,} ({fraud_count/len(data)*100:.2f}%)")
    print(f"   Legitimate:         {legit_count:,} ({legit_count/len(data)*100:.2f}%)")
    print(f"\n     Both classes are necessary for training:")
    print(f"      • Fraud samples teach the model what fraud looks like")
    print(f"      • Legitimate samples teach what normal behavior is")
    print(f"      • The model learns to distinguish between them")

    # Prepare temp streaming file
    Path(TEMP_STREAM_FILE).parent.mkdir(parents=True, exist_ok=True)
    with open(TEMP_STREAM_FILE, "w") as f:
        f.write(header)

    interval = 1.0 / tps
    idx = 0
    sent_fraud = 0
    sent_legit = 0
    
    print(f"\n[SEND] Streaming at {tps} TPS to topic: {NATS_TOPIC}")
    print("=" * 70)

    while True:
        t0 = time.time()
        
        # Track class distribution as we stream
        row_fraud_label = data[idx].strip().split(",")[fraud_idx]
        if row_fraud_label == "1":
            sent_fraud += 1
        else:
            sent_legit += 1
        
        # Append to streaming file
        with open(TEMP_STREAM_FILE, "a") as f:
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


def run_pub():
    print("═══════════════════════════════════════════════════════════")
    print("   FEEDBACK PUBLISHER (Ground Truth Labels - Both Classes) ")
    print("═══════════════════════════════════════════════════════════")
    
    # Verify fraud column exists and show distribution
    if not verify_fraud_column():
        print("\n[ERROR] Exiting due to data issues")
        return
    
    print()
    
    # Start streaming thread
    t = threading.Thread(target=stream_full_dataset, args=(TARGET_TPS,), daemon=True)
    t.start()

    # Pathway reads from temp file and publishes to NATS
    from shared.schema import FeedBackSchema
    tx = pw.io.csv.read(
        TEMP_STREAM_FILE,
        schema=FeedBackSchema,
        mode="streaming",
        autocommit_duration_ms=100,
    )
    pw.io.nats.write(tx, uri=NATS_URI, topic=NATS_TOPIC, format="json")
    
    print("[OK] Pathway publisher running...\n")
    pw.run()


if __name__ == "__main__":
    run_pub()