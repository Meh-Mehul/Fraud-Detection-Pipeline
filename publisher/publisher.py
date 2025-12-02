"""
Streams the Kaggle Synthetic AML dataset over NATS on subject 'transactions'.
Matches schema expected by trainer and inference pipelines.

Usage:
    python publisher.py --csv synthetic_aml.csv --rate 300 --nats nats://127.0.0.1:4222
"""

import argparse
import asyncio
import json
import pandas as pd
import numpy as np
from nats.aio.client import Client as NATS
import time
from datetime import datetime


# -----------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------

def combine_timestamp(row):
    """Combine Time and Date columns into single timestamp."""
    t = str(row.get("Time", "")).strip()
    d = str(row.get("Date", "")).strip()
    if t and d:
        return f"{d} {t}"
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def safe_get_float(v):
    try:
        return float(v)
    except:
        return 0.0


def safe_get_int(v):
    try:
        return int(v)
    except:
        return 0


# -----------------------------------------------------------------------
# MAIN STREAMING LOGIC
# -----------------------------------------------------------------------

async def run(csv_path, rate, nats_url, shuffle=False):
    print(f"[PUBLISHER] Loading CSV: {csv_path}")

    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"[PUBLISHER] Could not load CSV: {e}")
        return

    # Combine date/time into timestamp
    df["Timestamp"] = df.apply(combine_timestamp, axis=1)

    # Shuffle if requested
    if shuffle:
        df = df.sample(frac=1).reset_index(drop=True)

    print(f"[PUBLISHER] Connecting to NATS at {nats_url}...")
    nc = NATS()

    try:
        await nc.connect(nats_url)
    except Exception as e:
        print(f"[PUBLISHER] Failed to connect to NATS: {e}")
        return

    total_rows = len(df)
    print(f"[PUBLISHER] Loaded {total_rows} rows.")
    print(f"[PUBLISHER] Streaming rate: {rate} tx/min ({rate/60:.2f} tx/sec).")

    interval = 60.0 / rate if rate > 0 else 0
    sent = 0
    failed = 0
    start_time = time.time()

    # -------------------------------------------------------------------
    # STREAM LOOP
    # -------------------------------------------------------------------
    for i, row in df.iterrows():

        msg = {
            # Account fields (lowercase for consistency)
            "src": str(row.get("Sender_account", "unknown")),
            "dst": str(row.get("Receiver_account", "unknown")),

            # Amount
            "amount": safe_get_float(row.get("Amount", 0)),

            # Timestamp
            "timestamp": str(row.get("Timestamp", "")),

            # Label (0=normal, 1=laundering)
            "label": safe_get_int(row.get("Is_laundering", 0)),

            # Categorical features (lowercase for consistency)
            "payment_type": str(row.get("Payment_type", "unknown")),
            "sender_bank_location": str(row.get("Sender_bank_location", "unknown")),
            "receiver_bank_location": str(row.get("Receiver_bank_location", "unknown")),
        }

        # Optional laundering type (for logging, not used by model)
        if "Laundering_type" in row and pd.notna(row["Laundering_type"]):
            msg["laundering_type"] = str(row["Laundering_type"])

        try:
            await nc.publish("transactions", json.dumps(msg).encode())
            sent += 1
        except Exception as e:
            failed += 1
            if failed <= 5:
                print(f"[PUBLISHER] Error sending row {i}: {e}")

        # Status every 200 rows
        if (i + 1) % 200 == 0:
            elapsed = time.time() - start_time
            tx_per_min = (sent / elapsed) * 60 if elapsed > 0 else 0
            print(f"[PUBLISHER] Progress {i+1}/{total_rows} | sent={sent}, failed={failed} | {tx_per_min:.1f} tx/min")

        # Throttle if rate limit set
        if interval > 0:
            await asyncio.sleep(interval)

    # -------------------------------------------------------------------
    # END STREAM
    # -------------------------------------------------------------------
    elapsed = time.time() - start_time
    final_rate = (sent / elapsed) * 60 if elapsed > 0 else 0

    print(f"\n[PUBLISHER] Stream completed in {elapsed:.1f}s")
    print(f"[PUBLISHER] Sent: {sent}, Failed: {failed}")
    print(f"[PUBLISHER] Final rate: {final_rate:.2f} tx/min")

    await nc.drain()
    await nc.close()


# -----------------------------------------------------------------------
# ENTRYPOINT
# -----------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Synthetic AML Publisher")
    parser.add_argument("--csv", required=True, help="Path to AML dataset CSV")
    parser.add_argument("--rate", type=float, default=300, help="tx/min (0 for max speed)")
    parser.add_argument("--shuffle", action="store_true", help="Randomize row order")
    parser.add_argument("--nats", default="nats://127.0.0.1:4222", help="NATS server URL")
    args = parser.parse_args()

    print("=" * 60)
    print(" AML Transaction Publisher ")
    print("=" * 60)
    print(f"CSV     : {args.csv}")
    print(f"Rate    : {args.rate} tx/min")
    print(f"Shuffle : {args.shuffle}")
    print(f"NATS    : {args.nats}")
    print("=" * 60 + "\n")

    asyncio.run(run(args.csv, args.rate, args.nats, args.shuffle))