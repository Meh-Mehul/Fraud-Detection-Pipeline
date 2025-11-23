# pipeline/publisher/pub_detector.py
"""
Publish second half of the CSV continuously to fraud.transactions.
"""
import time, threading
from pathlib import Path
import pathway as pw
import sys
from pathlib import Path

# Add project root to python path
ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))
from shared.config import NATS_URI, NATS_INPUT_TOPIC as NATS_TOPIC, TARGET_TPS, ORIGINAL_DATA_FILE, AUTOCOMMIT_DURATION_MS, PUBLISHER_DETECTOR_TEMP_FILE

ORIGINAL_FILE = ORIGINAL_DATA_FILE
TEMP_STREAM_FILE = PUBLISHER_DETECTOR_TEMP_FILE

def stream_second_half(tps):
    with open(ORIGINAL_FILE, "r") as f:
        lines = f.readlines()
    header = lines[0]
    rows = lines[1:]
    mid = len(rows) // 2
    data = rows[mid:]

    Path(TEMP_STREAM_FILE).parent.mkdir(parents=True, exist_ok=True)
    with open(TEMP_STREAM_FILE, "w") as f:
        f.write(header)

    interval = 1.0 / tps
    idx = 0
    print(f"📤 DETECTOR Publisher streaming {len(data)} rows (2nd half) at {tps} TPS")

    while True:
        t0 = time.time()
        with open(TEMP_STREAM_FILE, "a") as f:
            f.write(data[idx])
        idx = (idx + 1) % len(data)
        elapsed = time.time() - t0
        sleep = interval - elapsed
        if sleep > 0:
            time.sleep(sleep)
        if idx % 50 == 0:
            print(f"[DET] sent {idx}", end="\r")

def run_pub():
    t = threading.Thread(target=stream_second_half, args=(TARGET_TPS,), daemon=True)
    t.start()

    from shared.schema import TransactionSchema
    tx = pw.io.csv.read(
        TEMP_STREAM_FILE,
        schema=TransactionSchema,
        mode="streaming",
        autocommit_duration_ms=AUTOCOMMIT_DURATION_MS
    )
    pw.io.nats.write(tx, uri=NATS_URI, topic=NATS_TOPIC)
    pw.run()

if __name__ == "__main__":
    run_pub()
