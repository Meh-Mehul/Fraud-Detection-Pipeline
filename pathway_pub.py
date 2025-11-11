"""
PATHWAY TRANSACTION PUBLISHER - Enhanced
Streams transactions from CSV with realistic rate limiting
"""

import pathway as pw
import time
from datetime import datetime

# Configuration
BATCH_SIZE = 1000  # Process in batches

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
    """Publish transactions with Pathway streaming"""
    
    print("═══════════════════════════════════════════════════════════")
    print("    PATHWAY TRANSACTION PUBLISHER v6.0")
    print("═══════════════════════════════════════════════════════════")
    print()
    
    # Read CSV as streaming source
    transactions = pw.io.csv.read(
        'fraudTrain.csv',
        schema=TransactionSchema,
        mode='streaming',
        autocommit_duration_ms=100
    )
    
    print("✓ CSV streaming initialized")
    print("✓ Output: pathway_streams/transactions.jsonl")
    print()
    print("🚀 Transaction stream active...")
    print("   Press Ctrl+C to stop")
    print()
    
    # Write to JSONL for detector to consume
    pw.io.jsonlines.write(transactions, 'pathway_streams/transactions.jsonl')
    
    # Run pipeline
    pw.run()


if __name__ == "__main__":
    import os
    os.makedirs('pathway_streams', exist_ok=True)
    
    try:
        run_publisher()
    except KeyboardInterrupt:
        print("\n\n✓ Publisher stopped")