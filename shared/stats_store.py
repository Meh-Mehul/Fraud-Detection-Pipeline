# pipeline/shared/stats_store.py
import json
from pathlib import Path
import threading
import math

PERSIST_DIR = Path("pathway_persistence")
PERSIST_DIR.mkdir(parents=True, exist_ok=True)

_STATS_FILE = PERSIST_DIR / "stats_store.json"
_lock = threading.Lock()

def _safe_load():
    if not _STATS_FILE.exists():
        return {"customers": {}, "merchants": {}, "categories": {}}
    try:
        return json.loads(_STATS_FILE.read_text())
    except Exception:
        return {"customers": {}, "merchants": {}, "categories": {}}

def _safe_save(obj):
    with _lock:
        tmp = _STATS_FILE.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(obj))
        tmp.replace(_STATS_FILE)

def update_customer(cc_num: str, amt: float, dist: float, is_fraud: int):
    """
    Maintain incremental stats per customer:
      - count, sum_amt, sumsq_amt, avg_amt, std_amt
      - count_dist, sum_dist, sumsq_dist, avg_dist, std_dist
      - fraud_history (sum)
    """
    with _lock:
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
        # amounts
        c["count"] += 1
        c["sum_amt"] += float(amt)
        c["sumsq_amt"] += float(amt) * float(amt)
        # dist
        c["count_dist"] += 1
        c["sum_dist"] += float(dist)
        c["sumsq_dist"] += float(dist) * float(dist)
        # fraud
        c["fraud_history"] += int(is_fraud)
        _safe_save(obj)

def update_merchant(merchant: str, amt: float, is_fraud: int):
    with _lock:
        obj = _safe_load()
        merchants = obj.setdefault("merchants", {})
        m = merchants.setdefault(str(merchant), {"count": 0, "sum_amt": 0.0, "fraud_count": 0})
        m["count"] += 1
        m["sum_amt"] += float(amt)
        m["fraud_count"] += int(is_fraud)
        _safe_save(obj)

def update_category(category: str, is_fraud: int):
    with _lock:
        obj = _safe_load()
        cats = obj.setdefault("categories", {})
        c = cats.setdefault(str(category), {"count": 0, "fraud_count": 0})
        c["count"] += 1
        c["fraud_count"] += int(is_fraud)
        _safe_save(obj)

def get_customer_profile(cc_num: str):
    obj = _safe_load()
    c = obj.get("customers", {}).get(str(cc_num))
    if not c:
        return {
            "txn_count": 0, "avg_amt": 0.0, "std_amt": 0.0,
            "avg_dist": 0.0, "std_dist": 0.0, "fraud_history": 0
        }
    # compute mean/std safely
    txn_count = c.get("count", 0)
    avg_amt = (c["sum_amt"] / txn_count) if txn_count > 0 else 0.0
    std_amt = math.sqrt(max(0.0, (c["sumsq_amt"] / txn_count) - (avg_amt ** 2))) if txn_count > 0 else 0.0

    cnt_dist = c.get("count_dist", 0)
    avg_dist = (c["sum_dist"] / cnt_dist) if cnt_dist > 0 else 0.0
    std_dist = math.sqrt(max(0.0, (c["sumsq_dist"] / cnt_dist) - (avg_dist ** 2))) if cnt_dist > 0 else 0.0

    return {
        "txn_count": txn_count,
        "avg_amt": avg_amt,
        "std_amt": std_amt,
        "avg_dist": avg_dist,
        "std_dist": std_dist,
        "fraud_history": c.get("fraud_history", 0)
    }

def get_merchant_profile(merchant: str):
    obj = _safe_load()
    m = obj.get("merchants", {}).get(str(merchant))
    if not m:
        return {"total": 0, "fraud_count": 0, "fraud_rate": 0.0}
    total = m.get("count", 0)
    fraud_count = m.get("fraud_count", 0)
    fraud_rate = (fraud_count / total) if total > 0 else 0.0
    return {"total": total, "fraud_count": fraud_count, "fraud_rate": fraud_rate}

def get_category_profile(category: str):
    obj = _safe_load()
    c = obj.get("categories", {}).get(str(category))
    if not c:
        return {"total": 0, "fraud_count": 0, "fraud_rate": 0.0}
    total = c.get("count", 0)
    fraud_count = c.get("fraud_count", 0)
    fraud_rate = (fraud_count / total) if total > 0 else 0.0
    return {"total": total, "fraud_count": fraud_count, "fraud_rate": fraud_rate}
