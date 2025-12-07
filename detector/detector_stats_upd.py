# pipeline/stats/stats_updater_redis.py
"""
Stats updater node: reads transactions from NATS and updates Redis stats.
This runs FIRST to initialize Redis and keep stats updated.
"""
import pathway as pw
from pathlib import Path
import sys
import math
from shared.metrics import initialize_metrics, get_metrics_manager


METRICS_PORT = 8002


# Add parent to path for imports
ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))

from shared.schema import TransactionSchema
from shared import redis_stats_store

NATS_URI = "nats://localhost:4222"
INPUT_TOPIC = "fraud.transactions"

# Persistence config
PERSIST_DIR = Path("./pathway_persistence")
CHECKPOINT_CONFIG = pw.persistence.Config.simple_config(
    pw.persistence.Backend.filesystem(str(PERSIST_DIR / "checkpoints_stats")),
    snapshot_interval_ms=10000
)

# Global Redis store
redis_store = redis_stats_store.get_store()

# Counters
update_counter = {"total": 0}


@pw.udf
def update_stats_redis(trans_num, cc_num, amt, lat, lon, merch_lat, merch_long, category, merchant):
    """
    Update Redis stats for customer, merchant, and category.
    Returns a status string to force execution.
    """
    update_counter["total"] += 1
    
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
        distance = 0.0
    
    # Update Redis stats (is_fraud=0 since this is transaction data, not feedback)
    try:
        redis_store.update_customer(str(cc_num), float(amt), float(distance), 0)
        redis_store.update_merchant(str(merchant), float(amt), 0)
        redis_store.update_category(str(category), 0)
        
        # Log progress
        if update_counter["total"] % 1000 == 0:
            summary = redis_store.get_stats_summary()
            print(f"[STATS] Updated {update_counter['total']:,} txns | "
                  f"Redis: {summary['customers']:,} customers, "
                  f"{summary['merchants']:,} merchants")
        
        return f"ok_{trans_num}"
    except Exception as e:
        print(f"❌ Redis update error for {trans_num}: {e}")
        return f"error_{trans_num}"

metrics_manager = initialize_metrics("stats_updater", port=METRICS_PORT)

def run_stats_node():
    print("═══════════════════════════════════════════")
    print("   STATS UPDATER NODE (Redis)")
    print("═══════════════════════════════════════════")
    print(f"Input topic: {INPUT_TOPIC}")
    print(f"Redis: {redis_stats_store.REDIS_HOST}:{redis_stats_store.REDIS_PORT}")
    print()
    
    # Initialize Redis - load from JSON if exists
    json_path = PERSIST_DIR / "stats_store.json"
    if json_path.exists():
        print("📥 Loading initial stats from JSON into Redis...")
        loaded = redis_store.load_from_json(str(json_path))
        summary = redis_store.get_stats_summary()
        print(f"✓ Loaded {loaded} entities")
        print(f"   Customers: {summary['customers']:,}")
        print(f"   Merchants: {summary['merchants']:,}")
        print(f"   Categories: {summary['categories']:,}")
    else:
        print("⚠️  No existing stats.json - starting fresh")
    
    print()
    print("───────────────────────────────────────────")
    
    # Read transactions from NATS
    tx = pw.io.nats.read(
        uri=NATS_URI,
        topic=INPUT_TOPIC,
        schema=TransactionSchema,
        format="json",
        persistent_id="stats_updater"
    )
    
    # Apply Redis stats update UDF
    updated = tx.select(
        trans_num=pw.this.trans_num,
        cc_num=pw.this.cc_num,
        update_status=update_stats_redis(
            pw.this.trans_num,
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
    
    # Write to null sink to force execution
    pw.io.null.write(updated)
    
    print("✓ Stats updater running - updating Redis in real-time")
    print("✓ Detector and feedback nodes will read from Redis")
    print()
    
    pw.run(persistence_config=CHECKPOINT_CONFIG)


if __name__ == "__main__":
    run_stats_node()