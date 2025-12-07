# pipeline/publisher/pub_detector.py
"""
Publish second half of the CSV continuously to fraud.transactions,
but REMOVE the is_fraud column since the detector does not use it.
Adds publish_timestamp_ms for latency tracking.
"""

import time, threading
from pathlib import Path
import pathway as pw
import sys

# Add project root to Python path
ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))

from shared.metrics import get_timestamp_ms

ORIGINAL_FILE = "fraudTrain.csv"
TEMP_STREAM_FILE = "./publisher/temp_det_stream.csv"

NATS_URI = "nats://localhost:4222"
NATS_TOPIC = "fraud.transactions"

TARGET_TPS = 20

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


# UDF to add publish timestamp
@pw.udf
def add_publish_timestamp() -> int:
    """Return current timestamp in milliseconds for latency tracking"""
    return get_timestamp_ms()


def strip_is_fraud(row: str) -> str:
    """
    Removes the last column (is_fraud) from a CSV row.
    Assumes fraudTrain.csv format with is_fraud as the LAST column.
    """
    parts = row.rstrip().split(",")
    if len(parts) > 1:
        parts = parts[:-1]   # drop last column
    return ",".join(parts) + "\n"


def stream_second_half(tps):
    with open(ORIGINAL_FILE, "r") as f:
        lines = f.readlines()

    header = lines[0].rstrip().split(",")
    if header[-1] == "is_fraud":
        header = header[:-1]        # drop the last column
    header = ",".join(header) + "\n"

    rows = lines[1:]
    # mid = len(rows) // 2
    data = rows

    # ensure directory exists
    Path(TEMP_STREAM_FILE).parent.mkdir(parents=True, exist_ok=True)

    # write header without is_fraud
    with open(TEMP_STREAM_FILE, "w") as f:
        f.write(header)

    interval = 1.0 / tps
    idx = 0

    print(f"[SEND] DETECTOR Publisher streaming {len(data)} rows (2nd half, no is_fraud)")

    while True:
        t0 = time.time()

        # remove the last column
        cleaned = strip_is_fraud(data[idx])

        with open(TEMP_STREAM_FILE, "a") as f:
            f.write(cleaned)

        idx = (idx + 1) % len(data)

        # regulate TPS
        elapsed = time.time() - t0
        sleep = interval - elapsed
        if sleep > 0:
            time.sleep(sleep)

        if idx % 50 == 0:
            print(f"[DET] sent {idx}", end="\r")


def run_pub():
    t = threading.Thread(target=stream_second_half, args=(TARGET_TPS,), daemon=True)
    t.start()
    tx = pw.io.csv.read(
        TEMP_STREAM_FILE,
        schema=TransactionSchema,  
        mode="streaming",
        autocommit_duration_ms=100,
    )

    # Add publish timestamp for latency tracking
    tx_with_timestamp = tx.select(
        *pw.this,
        publish_timestamp_ms=add_publish_timestamp()
    )

    pw.io.nats.write(tx_with_timestamp, uri=NATS_URI, topic=NATS_TOPIC, format="json")
    pw.run()


if __name__ == "__main__":
    run_pub()
