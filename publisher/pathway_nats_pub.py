## This file contains the main publisher logic to stream the static csv dataset
## For now, we have just read it from a single file, in final prodcut we plan to make this publishing happen from a single main file running inside docker.


import pathway as pw
from datetime import datetime

# NATS Configuration
NATS_URI = "nats://localhost:4222"
NATS_TOPIC = "fraud.transactions"

## For now, the structure of this table is accroding to our dataset used, but this may be configured according to the usecase and data-availability of the bank.
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
    is_fraud: int = pw.column_definition(dtype=int)


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
        'fraudTrain.csv',
        schema=TransactionSchema,
        mode='streaming',
        autocommit_duration_ms=100
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

