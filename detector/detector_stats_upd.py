# pipeline/stats/stats_updater.py
"""
Stats updater node: reads transactions and updates aggregated stats.
This ensures stats are always up-to-date for the detector.
"""
import pathway as pw
from pathlib import Path
import sys
import math
from datetime import datetime

# Add parent to path for imports
ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))

from shared.schema import TransactionSchema
from shared import stats_store

NATS_URI = "nats://localhost:4222"
INPUT_TOPIC = "fraud.transactions"

# Persistence config
PERSIST_DIR = Path("./pathway_persistence")
CHECKPOINT_CONFIG = pw.persistence.Config.simple_config(
    pw.persistence.Backend.filesystem(str(PERSIST_DIR / "checkpoints_stats")),
    snapshot_interval_ms=10000
)


@pw.udf
def update_stats(cc_num, amt, lat, lon, merch_lat, merch_long, category, merchant):
    """
    Update stats for customer, merchant, and category.
    Returns a status string to confirm execution.
    """
    # Compute distance
    try:
        d_lat = math.radians(float(merch_lat) - float(lat))
        d_lon = math.radians(float(merch_long) - float(lon))
        a = (
            math.sin(d_lat/2)**2 +
            math.cos(math.radians(float(lat))) *
            math.cos(math.radians(float(merch_lat))) *
            math.sin(d_lon/2)**2
        )
        distance = 6371 * 2 * math.asin(math.sqrt(a))
    except Exception as e:
        print(f"⚠️  Distance calculation error: {e}")
        distance = 0.0
    
    # Update stats (is_fraud=0 since this is just transaction data, not feedback)
    try:
        stats_store.update_customer(str(cc_num), float(amt), float(distance), 0)
        stats_store.update_merchant(str(merchant), float(amt), 0)
        stats_store.update_category(str(category), 0)
        return f"updated_{cc_num}"  # Return unique value to force execution
    except Exception as e:
        print(f"❌ Stats update error: {e}")
        return f"error_{cc_num}"


def run_stats_node():
    print("═══════════════════════════════════════════")
    print("      STATS UPDATER NODE                   ")
    print("═══════════════════════════════════════════")
    print(f"Input topic: {INPUT_TOPIC}")
    print(f"Stats file: {PERSIST_DIR / 'stats_store.json'}")
    print("───────────────────────────────────────────")
    
    # Read transactions
    tx = pw.io.nats.read(
        uri=NATS_URI,
        topic=INPUT_TOPIC,
        schema=TransactionSchema,
        format="json",
        persistent_id="stats_updater"
    )
    
    # Apply stats update UDF
    updated = tx.select(
        trans_num=pw.this.trans_num,
        cc_num=pw.this.cc_num,
        update_status=update_stats(
            pw.this.cc_num,
            pw.this.amt,
            pw.this.lat,
            pw.this.long,
            pw.this.merch_lat,
            pw.this.merch_long,
            pw.this.category,
            pw.this.merchant
        )
    )
    
    # CRITICAL: Write to a sink to force UDF execution
    # Option 1: Write to NATS (creates audit trail)
    pw.io.nats.write(
        updated,
        uri=NATS_URI,
        topic="stats.updates"
    )
    
    # Option 2: Also write to null sink if you don't need the audit trail
    # pw.io.null.write(updated)
    
    print("✓ Stats updater running...")
    pw.run(persistence_config=CHECKPOINT_CONFIG)


if __name__ == "__main__":
    run_stats_node()