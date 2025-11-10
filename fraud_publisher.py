import asyncio
import json
import pandas as pd
from nats.aio.client import Client as NATS
from datetime import datetime
import time

async def main():
    # Configuration
    TARGET_TPS = 1000  # Transactions per second (realistic bank load)
    # Options: 100 (small bank), 1000 (medium bank), 5000 (large bank)
    
    # Load the fraud detection dataset
    print("═══════════════════════════════════════════════════════════")
    print("    GLOBAL BANK TRANSACTION PROCESSING SYSTEM v4.2.1")
    print("═══════════════════════════════════════════════════════════")
    print()
    
    print("⏳ Initializing transaction feed from fraudTrain.csv...")
    df = pd.read_csv('fraudTrain.csv')
    
    print(f"✓ Connected to transaction database")
    print(f"✓ {len(df):,} transactions queued for processing")
    print(f"✓ Target rate: {TARGET_TPS:,} transactions/second")
    print()
    
    # Connect to NATS
    nc = NATS()
    await nc.connect("nats://localhost:4222")
    print("✓ Connected to NATS message broker (nats://localhost:4222)")
    print("✓ Publishing to channel: fraud.transactions")
    print()
    print("─────────────────────────────────────────────────────────")
    print("          LIVE TRANSACTION STREAM ACTIVE")
    print("─────────────────────────────────────────────────────────")
    print()
    
    transaction_count = 0
    start_time = time.time()
    batch_start = time.time()
    
    # Calculate delay between transactions
    delay = 1.0 / TARGET_TPS if TARGET_TPS > 0 else 0
    
    # Stream each transaction with rate limiting
    for idx, row in df.iterrows():
        transaction_count += 1
        
        # Convert row to dictionary
        transaction = {
            "trans_num": str(row['trans_num']),
            "trans_date_time": str(row['trans_date_trans_time']),
            "cc_num": int(row['cc_num']),
            "merchant": str(row['merchant']),
            "category": str(row['category']),
            "amt": float(row['amt']),
            "first_name": str(row['first']),
            "last_name": str(row['last']),
            "gender": str(row['gender']),
            "street": str(row['street']),
            "city": str(row['city']),
            "state": str(row['state']),
            "zip": int(row['zip']),
            "lat": float(row['lat']),
            "long": float(row['long']),
            "city_pop": int(row['city_pop']),
            "job": str(row['job']),
            "dob": str(row['dob']),
            "unix_time": int(row['unix_time']),
            "merch_lat": float(row['merch_lat']),
            "merch_long": float(row['merch_long']),
            "is_fraud": int(row['is_fraud'])
        }
        
        # PUBLISH with rate control
        await nc.publish("fraud.transactions", json.dumps(transaction).encode())
        
        # Print every 100th transaction
        if transaction_count % 100 == 0:
            print(f"💳 TXN #{transaction_count:07d} | ${transaction['amt']:>8.2f} | {transaction['merchant'][:35]:<35} | {transaction['category']:<20} | {transaction['city']}, {transaction['state']}")
        
        # Show metrics every 1000 transactions
        if transaction_count % 1000 == 0:
            elapsed = time.time() - start_time
            rate = transaction_count / elapsed
            actual_rate = 1000 / (time.time() - batch_start)
            batch_start = time.time()
            
            print(f"📊 STATUS | Processed: {transaction_count:>7,} | Target: {TARGET_TPS:>5,} tps | Actual: {actual_rate:>6,.0f} tps | Uptime: {elapsed:.1f}s")
            print()
        
        # Rate limiting: Sleep to maintain target TPS
        if delay > 0:
            await asyncio.sleep(delay)
    
    elapsed = time.time() - start_time
    print()
    print("─────────────────────────────────────────────────────────")
    print("          TRANSACTION STREAM COMPLETED")
    print("─────────────────────────────────────────────────────────")
    print(f"Total Transactions:    {transaction_count:>10,}")
    print(f"Processing Time:       {elapsed:>13.2f}s")
    print(f"Target Rate:           {TARGET_TPS:>10,} txns/sec")
    print(f"Actual Rate:           {transaction_count/elapsed:>10,.0f} txns/sec")
    print(f"Expected Time:         {len(df)/TARGET_TPS:>13.2f}s")
    print("─────────────────────────────────────────────────────────")
    print()
    
    await nc.close()

if __name__ == "__main__":
    asyncio.run(main())