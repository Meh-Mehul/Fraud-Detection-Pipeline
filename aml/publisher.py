# publisher.py
"""
Reads the Kaggle synthetic AML dataset and streams one transaction at a time
over NATS subject `transactions`.

Usage:
    python publisher.py --csv aml.csv --rate 300 --nats nats://127.0.0.1:4222
"""

import argparse
import asyncio
import json
import pandas as pd
from nats.aio.client import Client as NATS
import time


async def run(csv_path, rate, nats_url):
    """
    Stream transactions from CSV to NATS.
    
    Args:
        csv_path: Path to the CSV file
        rate: Transactions per minute (0 = no throttling)
        nats_url: NATS server URL
    """
    
    print(f"[PUBLISHER] Loading data from {csv_path}...")
    
    try:
        df = pd.read_csv(csv_path)
    except FileNotFoundError:
        print(f"[PUBLISHER] Error: File '{csv_path}' not found")
        return
    except Exception as e:
        print(f"[PUBLISHER] Error loading CSV: {e}")
        return

    # Combine Time + Date into a single timestamp string
    if "Date" in df.columns and "Time" in df.columns:
        df["Timestamp"] = df["Date"] + " " + df["Time"]
    elif "Timestamp" in df.columns:
        # Already has timestamp
        pass
    else:
        print("[PUBLISHER] Warning: No 'Date'/'Time' or 'Timestamp' column found, using current time")
        from datetime import datetime
        df["Timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print(f"[PUBLISHER] Connecting to NATS at {nats_url}...")
    nc = NATS()
    
    try:
        await nc.connect(nats_url)
    except Exception as e:
        print(f"[PUBLISHER] Failed to connect to NATS: {e}")
        return

    print(f"[PUBLISHER] Loaded {len(df)} rows. Streaming at {rate} tx/min...")
    print(f"[PUBLISHER] CSV columns: {list(df.columns)}")

    interval = 60.0 / rate if rate > 0 else 0
    
    start_time = time.time()
    successful = 0
    failed = 0

    for i, row in df.iterrows():
        try:
            # Build message with all available fields
            msg = {
                "src": str(row.get("Sender_account", row.get("From Account", "unknown"))),
                "dst": str(row.get("Receiver_account", row.get("To Account", "unknown"))),
                "amount": float(row.get("Amount", row.get("Amount Paid", 0))),
                "timestamp": str(row.get("Timestamp", "")),
                "label": int(row.get("Is_laundering", row.get("label", 0))),
            }
            
            # Add optional categorical fields if they exist
            if "Payment_type" in row:
                msg["payment_type"] = str(row["Payment_type"])
            elif "Payment Format" in row:
                msg["payment_type"] = str(row["Payment Format"])
            else:
                msg["payment_type"] = "unknown"
                
            if "Sender_bank_location" in row:
                msg["sender_bank_location"] = str(row["Sender_bank_location"])
            elif "From Bank" in row:
                msg["sender_bank_location"] = str(row["From Bank"])
            else:
                msg["sender_bank_location"] = "unknown"
                
            if "Receiver_bank_location" in row:
                msg["receiver_bank_location"] = str(row["Receiver_bank_location"])
            elif "To Bank" in row:
                msg["receiver_bank_location"] = str(row["To Bank"])
            else:
                msg["receiver_bank_location"] = "unknown"
            
            # Add laundering type if available
            if "Laundering_type" in row:
                msg["laundering_type"] = str(row.get("Laundering_type", ""))

            await nc.publish("transactions", json.dumps(msg).encode())
            successful += 1
            
            if (i + 1) % 100 == 0:
                elapsed = time.time() - start_time
                rate_actual = successful / (elapsed / 60) if elapsed > 0 else 0
                print(f"[PUBLISHER] Progress: {i+1}/{len(df)} ({successful} sent, {failed} failed) "
                      f"@ {rate_actual:.1f} tx/min")

        except Exception as e:
            failed += 1
            if failed <= 5:  # Only print first 5 errors
                print(f"[PUBLISHER] Error publishing row {i}: {e}")

        if interval > 0:
            await asyncio.sleep(interval)

    elapsed = time.time() - start_time
    print(f"[PUBLISHER] Completed streaming in {elapsed:.1f}s")
    print(f"[PUBLISHER] Successfully sent: {successful}, Failed: {failed}")
    
    await nc.drain()
    await nc.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AML Transaction Publisher")
    parser.add_argument("--csv", required=True, help="Path to the AML CSV dataset")
    parser.add_argument("--rate", type=float, default=300, 
                        help="Transactions per minute (0 = no throttling)")
    parser.add_argument("--nats", default="nats://127.0.0.1:4222", 
                        help="NATS server URL")
    args = parser.parse_args()

    print(f"[PUBLISHER] Starting with CSV={args.csv}, rate={args.rate} tx/min, NATS={args.nats}")
    asyncio.run(run(args.csv, args.rate, args.nats))