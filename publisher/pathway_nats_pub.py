## This file contains the main publisher logic to stream the static csv dataset
## For now, we have just read it from a single file, in final prodcut we plan to make this publishing happen from a single main file running inside docker.


import pathway as pw
from datetime import datetime
from shared.config import NATS_URI, NATS_INPUT_TOPIC as NATS_TOPIC, AUTOCOMMIT_DURATION_MS, PUBLISHER_STREAM_FILE
from shared.schema import TransactionSchema


def run_publisher():
    """Publish transactions to NATS with Pathway streaming"""
    print("═══════════════════════════════════════════════════════════")
    print("    PATHWAY TRANSACTION PUBLISHER v7.1 - NATS FIXED")
    print("═══════════════════════════════════════════════════════════")
    print()
    print(f"  NATS URI: {NATS_URI}")
    print(f"  Topic: {NATS_TOPIC}")
    print()
    
    transactions = pw.io.csv.read(
        PUBLISHER_STREAM_FILE,
        schema=TransactionSchema,
        mode='streaming',
        autocommit_duration_ms=AUTOCOMMIT_DURATION_MS
    )
    
    print("✓ CSV streaming initialized")
    print(f"✓ Output: NATS topic '{NATS_TOPIC}'")
    print()
    print("🚀 Transaction stream active...")
    print("   Press Ctrl+C to stop")
    print()
    pw.io.nats.write(
        transactions,
        uri=NATS_URI,
        topic=NATS_TOPIC
    )
    
    # Run pipeline
    pw.run()

