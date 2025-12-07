# pipeline/shared/stats_store.py
"""
Thread-safe statistics store for customer, merchant, and category profiles.
Uses file-based persistence with atomic writes.
"""
import json
from pathlib import Path
import threading
import math

# ──────────────────────────────────────────────────────
# PATHS & LOCK
# ──────────────────────────────────────────────────────
PERSIST_DIR = Path("./pathway_persistence")
PERSIST_DIR.mkdir(parents=True, exist_ok=True)

_STATS_FILE = PERSIST_DIR / "stats_store.json"
_LOCK = threading.Lock()

# Initialize empty file if it doesn't exist
if not _STATS_FILE.exists():
    _STATS_FILE.write_text(json.dumps({
        "customers": {},
        "merchants": {},
        "categories": {}
    }))
    print(f"[OK] Initialized stats store at {_STATS_FILE}")


# ──────────────────────────────────────────────────────
# BASIC LOAD / SAVE (ALWAYS USE WITHIN LOCK)
# ──────────────────────────────────────────────────────
def _safe_load():
    """Load stats file. Should be called within lock."""
    if not _STATS_FILE.exists():
        return {"customers": {}, "merchants": {}, "categories": {}}
    try:
        with open(_STATS_FILE, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"[WARN]  Error loading stats: {e}")
        return {"customers": {}, "merchants": {}, "categories": {}}


def _safe_save(obj):
    """Save stats file atomically. Should be called within lock."""
    try:
        # Write to temp file first
        tmp = _STATS_FILE.with_suffix(".json.tmp")
        with open(tmp, 'w') as f:
            json.dump(obj, f, indent=2)
        # Atomic rename
        tmp.replace(_STATS_FILE)
        return True
    except Exception as e:
        print(f"[ERROR] Error saving stats: {e}")
        return False


# ──────────────────────────────────────────────────────
# UPDATE FUNCTIONS — ALL OPERATIONS WITHIN LOCK
# ──────────────────────────────────────────────────────
def update_customer(cc_num: str, amt: float, dist: float, is_fraud: int):
    """Update customer statistics with new transaction."""
    with _LOCK:
        obj = _safe_load()
        customers = obj.setdefault("customers", {})
        
        c = customers.setdefault(str(cc_num), {
            "count": 0,
            "sum_amt": 0.0,
            "sumsq_amt": 0.0,
            "count_dist": 0,
            "sum_dist": 0.0,
            "sumsq_dist": 0.0,
            "fraud_history": 0
        })

        # Update amounts
        c["count"] += 1
        c["sum_amt"] += float(amt)
        c["sumsq_amt"] += float(amt) ** 2

        # Update distances
        c["count_dist"] += 1
        c["sum_dist"] += float(dist)
        c["sumsq_dist"] += float(dist) ** 2

        # Update fraud history
        c["fraud_history"] += int(is_fraud)

        success = _safe_save(obj)
        return success


def update_merchant(merchant: str, amt: float, is_fraud: int):
    """Update merchant statistics with new transaction."""
    with _LOCK:
        obj = _safe_load()
        merchants = obj.setdefault("merchants", {})
        
        m = merchants.setdefault(str(merchant), {
            "count": 0,
            "sum_amt": 0.0,
            "fraud_count": 0
        })
        
        m["count"] += 1
        m["sum_amt"] += float(amt)
        m["fraud_count"] += int(is_fraud)

        success = _safe_save(obj)
        return success


def update_category(category: str, is_fraud: int):
    """Update category statistics with new transaction."""
    with _LOCK:
        obj = _safe_load()
        categories = obj.setdefault("categories", {})
        
        c = categories.setdefault(str(category), {
            "count": 0,
            "fraud_count": 0
        })
        
        c["count"] += 1
        c["fraud_count"] += int(is_fraud)

        success = _safe_save(obj)
        return success


# ──────────────────────────────────────────────────────
# READ HELPERS (LOCK FOR CONSISTENCY)
# ──────────────────────────────────────────────────────
def get_customer_profile(cc_num: str):
    """Get computed customer profile with averages and std devs."""
    with _LOCK:
        obj = _safe_load()
        c = obj.get("customers", {}).get(str(cc_num))

    if not c:
        return {
            "txn_count": 0,
            "avg_amt": 0.0,
            "std_amt": 0.0,
            "avg_dist": 0.0,
            "std_dist": 0.0,
            "fraud_history": 0
        }

    txn_count = c["count"]
    avg_amt = c["sum_amt"] / txn_count if txn_count > 0 else 0.0
    var_amt = (c["sumsq_amt"] / txn_count) - (avg_amt ** 2) if txn_count > 0 else 0.0
    std_amt = math.sqrt(max(0.0, var_amt))

    dist_count = c["count_dist"]
    avg_dist = c["sum_dist"] / dist_count if dist_count > 0 else 0.0
    var_dist = (c["sumsq_dist"] / dist_count) - (avg_dist ** 2) if dist_count > 0 else 0.0
    std_dist = math.sqrt(max(0.0, var_dist))

    return {
        "txn_count": txn_count,
        "avg_amt": avg_amt,
        "std_amt": std_amt,
        "avg_dist": avg_dist,
        "std_dist": std_dist,
        "fraud_history": c["fraud_history"]
    }


def get_merchant_profile(merchant: str):
    """Get merchant fraud rate and transaction count."""
    with _LOCK:
        obj = _safe_load()
        m = obj.get("merchants", {}).get(str(merchant))

    if not m:
        return {"total": 0, "fraud_count": 0, "fraud_rate": 0.0}

    total = m["count"]
    fraud_count = m["fraud_count"]
    fraud_rate = fraud_count / total if total > 0 else 0.0

    return {
        "total": total,
        "fraud_count": fraud_count,
        "fraud_rate": fraud_rate
    }


def get_category_profile(category: str):
    """Get category fraud rate and transaction count."""
    with _LOCK:
        obj = _safe_load()
        c = obj.get("categories", {}).get(str(category))

    if not c:
        return {"total": 0, "fraud_count": 0, "fraud_rate": 0.0}

    total = c["count"]
    fraud_count = c["fraud_count"]
    fraud_rate = fraud_count / total if total > 0 else 0.0

    return {
        "total": total,
        "fraud_count": fraud_count,
        "fraud_rate": fraud_rate
    }


# ──────────────────────────────────────────────────────
# UTILITY FUNCTIONS
# ──────────────────────────────────────────────────────
def get_stats_summary():
    """Get summary of all stats for debugging."""
    with _LOCK:
        obj = _safe_load()
    
    return {
        "customers": len(obj.get("customers", {})),
        "merchants": len(obj.get("merchants", {})),
        "categories": len(obj.get("categories", {}))
    }


def reset_stats():
    """Reset all stats (use with caution)."""
    with _LOCK:
        obj = {"customers": {}, "merchants": {}, "categories": {}}
        _safe_save(obj)
        print("[OK] Stats reset")