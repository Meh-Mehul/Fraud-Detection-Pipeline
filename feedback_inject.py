import asyncio
import nats
import json
import random
import time
from datetime import datetime

# CONFIG
NATS_URI = "nats://localhost:4222"
TOPIC = "fraud.test_transactions"

async def inject_scenarios():
    nc = await nats.connect(NATS_URI)
    print(f"💉 Connected to NATS. Injecting scenarios to '{TOPIC}'...")

    # Base profile (We need to use a CC_NUM that exists or create a new one)
    # We will use a fixed CC to build instant history then break it
    target_cc = 1234567890123456
    
    # ====================================================================
    # STEP 1: Build a "Normal" Baseline (So stats exist)
    # ====================================================================
    print("\n--- Step 1: Establishing Baseline (Normal Behavior) ---")
    for i in range(5):
        normal_tx = {
            "trans_num": f"SETUP_{i}",
            "trans_date_trans_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "cc_num": target_cc,
            "merchant": "Local Grocery",
            "category": "grocery_pos",
            "amt": random.uniform(40, 60),  # Always around $50
            "first": "Test", "last": "User", "gender": "M",
            "street": "123 Main St", "city": "New York", "state": "NY", "zip": 10001,
            "lat": 40.7128, "long": -74.0060, "city_pop": 8000000,
            "job": "Tester", "dob": "1990-01-01",
            "unix_time": int(time.time()),
            "merch_lat": 40.7130, "merch_long": -74.0065 # Very close
        }
        await nc.publish(TOPIC, json.dumps(normal_tx).encode())
        print(f"   Sent Normal: ${normal_tx['amt']:.2f}")
        await asyncio.sleep(0.1)

    print("   (Waiting 2s for Pathway to process stats...)")
    await asyncio.sleep(2)

    # ====================================================================
    # SCENARIO A: The "Massive Amount" Attack (Triggers Z-Score)
    # ====================================================================
    print("\n--- Step 2: Injecting MASSIVE AMOUNT Fraud ---")
    fraud_tx_1 = normal_tx.copy()
    fraud_tx_1["trans_num"] = "FRAUD_TEST_1"
    fraud_tx_1["amt"] = 5000.00  # Jump from $50 avg to $5000 (Z-Score ~100)
    fraud_tx_1["category"] = "electronics"
    fraud_tx_1["unix_time"] = int(time.time())
    
    await nc.publish(TOPIC, json.dumps(fraud_tx_1).encode())
    print(f"⚠️ SENT: $5000.00 transaction (Should trigger TIER 1)")

    # ====================================================================
    # SCENARIO B: The "Teleportation" Attack (Triggers Distance)
    # ====================================================================
    print("\n--- Step 3: Injecting LOCATION Fraud ---")
    fraud_tx_2 = normal_tx.copy()
    fraud_tx_2["trans_num"] = "FRAUD_TEST_2"
    fraud_tx_2["amt"] = 55.00
    fraud_tx_2["merch_lat"] = 51.5074   # London
    fraud_tx_2["merch_long"] = -0.1278
    fraud_tx_2["merchant"] = "London Pub"
    fraud_tx_2["unix_time"] = int(time.time())
    
    await nc.publish(TOPIC, json.dumps(fraud_tx_2).encode())
    print(f"⚠️ SENT: Transaction in London (Should trigger TIER 1 Distance)")

    # ====================================================================
    # SCENARIO C: The "Rapid Fire" Attack (Triggers Frequency/Rule)
    # ====================================================================
    print("\n--- Step 4: Injecting RAPID FIRE Fraud ---")
    for i in range(3):
        fraud_tx_3 = normal_tx.copy()
        fraud_tx_3["trans_num"] = f"FRAUD_TEST_3_{i}"
        fraud_tx_3["amt"] = 450.00 # High but not massive
        fraud_tx_3["category"] = "shopping_net" # Online
        fraud_tx_3["unix_time"] = int(time.time()) + (i*10) # 10 seconds apart
        
        # Hack: Force Late Night
        # (In UDF, hour is extracted from unix_time. We can't easily fake extraction 
        # without changing the UDF, but we can hit other rules)
        
        await nc.publish(TOPIC, json.dumps(fraud_tx_3).encode())
        print(f"⚠️ SENT: Rapid Online Transaction {i+1}")
        await asyncio.sleep(0.1)

    await nc.drain()
    print("\nDone! Check your Inference Detector logs.")

if __name__ == "__main__":
    asyncio.run(inject_scenarios())