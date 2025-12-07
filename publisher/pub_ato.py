import time
import threading
from pathlib import Path
import pathway as pw
import sys
from multiprocessing import Process

# Add project root to python path
ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))

from shared.config import (
    NATS_URI,
    ATO_LOGIN_ATTEMPTS_TOPIC,
    ATO_USER_PROFILES_TOPIC
)

# Configuration
ORIGINAL_FILE = "ato_data.csv"
TEMP_LOGIN_FILE = "./publisher/temp_ato_logins.csv"
TEMP_PROFILE_FILE = "./publisher/temp_ato_profiles.csv"
TARGET_TPS = 25

class TransactionSchema(pw.Schema):
    step: int = pw.column_definition(dtype=int)
    type: str = pw.column_definition(dtype=str)
    amount: float = pw.column_definition(dtype=float)
    nameOrig: str = pw.column_definition(dtype=str)
    oldbalanceOrg: float = pw.column_definition(dtype=float)
    newbalanceOrig: float = pw.column_definition(dtype=float)
    nameDest: str = pw.column_definition(dtype=str)
    oldbalanceDest: float = pw.column_definition(dtype=float)
    newbalanceDest: float = pw.column_definition(dtype=float)
    isFraud: int = pw.column_definition(dtype=int)
    isFlaggedFraud: int = pw.column_definition(dtype=int)

def verify_transaction_data():
    try:
        with open(ORIGINAL_FILE, "r") as f:
            header = f.readline().strip().split(",")
            fraud_count = 0
            total_count = 0
            for line in f:
                parts = line.strip().split(",")
                if len(parts) >= 10:
                    total_count += 1
                    if parts[9] == "1":
                        fraud_count += 1
            return total_count > 0
    except FileNotFoundError:
        return False
    except Exception:
        return False

def stream_login_data(tps):
    with open(ORIGINAL_FILE, "r") as f:
        lines = f.readlines()
    header = lines[0]
    rows = lines[1:]
    data = rows
    Path(TEMP_LOGIN_FILE).parent.mkdir(parents=True, exist_ok=True)
    with open(TEMP_LOGIN_FILE, "w") as f:
        f.write(header)
    interval = 1.0 / tps
    idx = 0
    while True:
        t0 = time.time()
        with open(TEMP_LOGIN_FILE, "a") as f:
            f.write(data[idx])
        idx = (idx + 1) % len(data)
        elapsed = time.time() - t0
        sleep = interval - elapsed
        if sleep > 0:
            time.sleep(sleep)

def run_login_attempts_publisher():
    t = threading.Thread(target=stream_login_data, args=(TARGET_TPS,), daemon=True)
    t.start()
    tx = pw.io.csv.read(
        TEMP_LOGIN_FILE,
        schema=TransactionSchema,
        mode="streaming",
        autocommit_duration_ms=100,
    )
    pw.io.nats.write(tx, uri=NATS_URI, topic=ATO_LOGIN_ATTEMPTS_TOPIC, format="json")
    pw.run()

def main():
    if not verify_transaction_data():
        return
    login_process = Process(target=run_login_attempts_publisher, name="ATOLoginPublisher")
    login_process.start()
    try:
        login_process.join()
    except KeyboardInterrupt:
        login_process.terminate()
        login_process.join()

if __name__ == "__main__":
    main()