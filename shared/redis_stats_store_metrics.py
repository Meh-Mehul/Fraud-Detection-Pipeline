# shared/redis_stats_store_metrics.py
"""
Redis-based stats store with Prometheus instrumentation.
Tracks all Redis operations for monitoring.
"""
import redis
import json
import time
from pathlib import Path
from typing import Dict, Any

# Import metrics
try:
    from .metrics import (
        record_redis_operation,
        update_redis_entity_counts,
        set_redis_connection_status,
        MetricsTimer
    )
    METRICS_ENABLED = True
except ImportError:
    METRICS_ENABLED = False
    print("⚠️  Metrics module not available for Redis stats store")

# Redis connection configuration
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0

# Key prefixes
CUSTOMER_PREFIX = "customer:"
MERCHANT_PREFIX = "merchant:"
CATEGORY_PREFIX = "category:"


class RedisStatsStore:
    """Thread-safe Redis-based statistics storage with metrics"""
    
    def __init__(self, host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB):
        """Initialize Redis connection"""
        self.redis_client = redis.Redis(
            host=host,
            port=port,
            db=db,
            decode_responses=True,
            socket_keepalive=True,
            socket_connect_timeout=5
        )
        
        # Test connection
        try:
            self.redis_client.ping()
            print(f"✓ Redis connected: {host}:{port} (DB {db})")
            if METRICS_ENABLED:
                set_redis_connection_status(True)
        except redis.ConnectionError as e:
            print(f"❌ Redis connection failed: {e}")
            if METRICS_ENABLED:
                set_redis_connection_status(False)
            raise
    
    def _record_operation(self, operation: str, entity_type: str, duration: float, success: bool = True):
        """Record Redis operation metrics"""
        if METRICS_ENABLED:
            record_redis_operation(operation, entity_type, duration, success)
    
    # ========================================================================
    # CUSTOMER OPERATIONS (Instrumented)
    # ========================================================================
    
    def _get_customer_key(self, cc_num: str) -> str:
        return f"{CUSTOMER_PREFIX}{cc_num}"
    
    def _set_customer_stats(self, cc_num: str, stats: Dict[str, Any]):
        """Set customer stats with timing"""
        start = time.time()
        try:
            key = self._get_customer_key(cc_num)
            self.redis_client.hset(key, mapping={
                'txn_count': stats.get('txn_count', 0),
                'fraud_history': stats.get('fraud_history', 0),
                'avg_amt': stats.get('avg_amt', 0.0),
                'std_amt': stats.get('std_amt', 0.0),
                'avg_dist': stats.get('avg_dist', 0.0),
                'std_dist': stats.get('std_dist', 0.0),
                'total_amt': stats.get('total_amt', 0.0),
                'total_dist': stats.get('total_dist', 0.0),
                'amt_squared': stats.get('amt_squared', 0.0),
                'dist_squared': stats.get('dist_squared', 0.0)
            })
            duration = time.time() - start
            self._record_operation('set', 'customer', duration, True)
        except Exception as e:
            duration = time.time() - start
            self._record_operation('set', 'customer', duration, False)
            raise
    
    def get_customer_profile(self, cc_num: int) -> Dict[str, float]:
        """Get customer statistics with timing"""
        start = time.time()
        try:
            key = self._get_customer_key(str(cc_num))
            data = self.redis_client.hgetall(key)
            
            duration = time.time() - start
            self._record_operation('get', 'customer', duration, True)
            
            if not data:
                return {
                    'txn_count': 0,
                    'fraud_history': 0,
                    'avg_amt': 0.0,
                    'std_amt': 0.0,
                    'avg_dist': 0.0,
                    'std_dist': 0.0
                }
            
            return {
                'txn_count': int(data.get('txn_count', 0)),
                'fraud_history': int(data.get('fraud_history', 0)),
                'avg_amt': float(data.get('avg_amt', 0.0)),
                'std_amt': float(data.get('std_amt', 0.0)),
                'avg_dist': float(data.get('avg_dist', 0.0)),
                'std_dist': float(data.get('std_dist', 0.0))
            }
        except Exception as e:
            duration = time.time() - start
            self._record_operation('get', 'customer', duration, False)
            raise
    
    def update_customer(self, cc_num: str, amt: float, distance: float, is_fraud: int):
        """Update customer statistics atomically with timing"""
        start = time.time()
        try:
            key = self._get_customer_key(cc_num)
            
            # Use Redis pipeline
            pipe = self.redis_client.pipeline()
            pipe.hincrby(key, 'txn_count', 1)
            pipe.hincrbyfloat(key, 'total_amt', amt)
            pipe.hincrbyfloat(key, 'total_dist', distance)
            pipe.hincrbyfloat(key, 'amt_squared', amt * amt)
            pipe.hincrbyfloat(key, 'dist_squared', distance * distance)
            
            if is_fraud:
                pipe.hincrby(key, 'fraud_history', 1)
            
            pipe.hgetall(key)
            results = pipe.execute()
            updated_data = results[-1]
            
            # Calculate running statistics
            n = int(updated_data.get('txn_count', 1))
            
            if n > 0:
                total_amt = float(updated_data.get('total_amt', 0.0))
                total_dist = float(updated_data.get('total_dist', 0.0))
                amt_sq = float(updated_data.get('amt_squared', 0.0))
                dist_sq = float(updated_data.get('dist_squared', 0.0))
                
                avg_amt = total_amt / n
                avg_dist = total_dist / n
                
                std_amt = ((amt_sq / n) - (avg_amt ** 2)) ** 0.5 if n > 1 else 0.0
                std_dist = ((dist_sq / n) - (avg_dist ** 2)) ** 0.5 if n > 1 else 0.0
                
                pipe2 = self.redis_client.pipeline()
                pipe2.hset(key, 'avg_amt', avg_amt)
                pipe2.hset(key, 'std_amt', max(std_amt, 0.01))
                pipe2.hset(key, 'avg_dist', avg_dist)
                pipe2.hset(key, 'std_dist', max(std_dist, 0.01))
                pipe2.execute()
            
            duration = time.time() - start
            self._record_operation('update', 'customer', duration, True)
            
        except Exception as e:
            duration = time.time() - start
            self._record_operation('update', 'customer', duration, False)
            raise
    
    # ========================================================================
    # MERCHANT OPERATIONS (Instrumented)
    # ========================================================================
    
    def _get_merchant_key(self, merchant: str) -> str:
        return f"{MERCHANT_PREFIX}{merchant}"
    
    def _set_merchant_stats(self, merchant: str, stats: Dict[str, Any]):
        """Set merchant stats with timing"""
        start = time.time()
        try:
            key = self._get_merchant_key(merchant)
            self.redis_client.hset(key, mapping={
                'total': stats.get('total', 0),
                'fraud_count': stats.get('fraud_count', 0),
                'fraud_rate': stats.get('fraud_rate', 0.0)
            })
            duration = time.time() - start
            self._record_operation('set', 'merchant', duration, True)
        except Exception as e:
            duration = time.time() - start
            self._record_operation('set', 'merchant', duration, False)
            raise
    
    def get_merchant_profile(self, merchant: str) -> Dict[str, float]:
        """Get merchant fraud statistics with timing"""
        start = time.time()
        try:
            key = self._get_merchant_key(merchant)
            data = self.redis_client.hgetall(key)
            
            duration = time.time() - start
            self._record_operation('get', 'merchant', duration, True)
            
            if not data:
                return {
                    'total': 0,
                    'fraud_count': 0,
                    'fraud_rate': 0.0
                }
            
            return {
                'total': int(data.get('total', 0)),
                'fraud_count': int(data.get('fraud_count', 0)),
                'fraud_rate': float(data.get('fraud_rate', 0.0))
            }
        except Exception as e:
            duration = time.time() - start
            self._record_operation('get', 'merchant', duration, False)
            raise
    
    def update_merchant(self, merchant: str, amt: float, is_fraud: int):
        """Update merchant statistics atomically with timing"""
        start = time.time()
        try:
            key = self._get_merchant_key(merchant)
            
            pipe = self.redis_client.pipeline()
            pipe.hincrby(key, 'total', 1)
            
            if is_fraud:
                pipe.hincrby(key, 'fraud_count', 1)
            
            pipe.hgetall(key)
            results = pipe.execute()
            updated_data = results[-1]
            
            # Calculate fraud rate
            total = int(updated_data.get('total', 1))
            fraud_count = int(updated_data.get('fraud_count', 0))
            
            if total >= 30:
                fraud_rate = fraud_count / total
            else:
                fraud_rate = 0.0
            
            self.redis_client.hset(key, 'fraud_rate', fraud_rate)
            
            duration = time.time() - start
            self._record_operation('update', 'merchant', duration, True)
            
        except Exception as e:
            duration = time.time() - start
            self._record_operation('update', 'merchant', duration, False)
            raise
    
    # ========================================================================
    # CATEGORY OPERATIONS (Instrumented)
    # ========================================================================
    
    def _get_category_key(self, category: str) -> str:
        return f"{CATEGORY_PREFIX}{category}"
    
    def _set_category_stats(self, category: str, stats: Dict[str, Any]):
        """Set category stats with timing"""
        start = time.time()
        try:
            key = self._get_category_key(category)
            self.redis_client.hset(key, mapping={
                'total': stats.get('total', 0),
                'fraud_count': stats.get('fraud_count', 0),
                'fraud_rate': stats.get('fraud_rate', 0.0)
            })
            duration = time.time() - start
            self._record_operation('set', 'category', duration, True)
        except Exception as e:
            duration = time.time() - start
            self._record_operation('set', 'category', duration, False)
            raise
    
    def get_category_profile(self, category: str) -> Dict[str, float]:
        """Get category fraud statistics with timing"""
        start = time.time()
        try:
            key = self._get_category_key(category)
            data = self.redis_client.hgetall(key)
            
            duration = time.time() - start
            self._record_operation('get', 'category', duration, True)
            
            if not data:
                return {
                    'total': 0,
                    'fraud_count': 0,
                    'fraud_rate': 0.0
                }
            
            return {
                'total': int(data.get('total', 0)),
                'fraud_count': int(data.get('fraud_count', 0)),
                'fraud_rate': float(data.get('fraud_rate', 0.0))
            }
        except Exception as e:
            duration = time.time() - start
            self._record_operation('get', 'category', duration, False)
            raise
    
    def update_category(self, category: str, is_fraud: int):
        """Update category statistics atomically with timing"""
        start = time.time()
        try:
            key = self._get_category_key(category)
            
            pipe = self.redis_client.pipeline()
            pipe.hincrby(key, 'total', 1)
            
            if is_fraud:
                pipe.hincrby(key, 'fraud_count', 1)
            
            pipe.hgetall(key)
            results = pipe.execute()
            updated_data = results[-1]
            
            # Calculate fraud rate
            total = int(updated_data.get('total', 1))
            fraud_count = int(updated_data.get('fraud_count', 0))
            
            if total >= 100:
                fraud_rate = fraud_count / total
            else:
                fraud_rate = 0.0
            
            self.redis_client.hset(key, 'fraud_rate', fraud_rate)
            
            duration = time.time() - start
            self._record_operation('update', 'category', duration, True)
            
        except Exception as e:
            duration = time.time() - start
            self._record_operation('update', 'category', duration, False)
            raise
    
    # ========================================================================
    # UTILITY METHODS (Instrumented)
    # ========================================================================
    
    def get_stats_summary(self) -> Dict[str, int]:
        """Get summary with metrics update"""
        customer_count = len(self.redis_client.keys(f"{CUSTOMER_PREFIX}*"))
        merchant_count = len(self.redis_client.keys(f"{MERCHANT_PREFIX}*"))
        category_count = len(self.redis_client.keys(f"{CATEGORY_PREFIX}*"))
        
        # Update metrics
        if METRICS_ENABLED:
            update_redis_entity_counts(customer_count, merchant_count, category_count)
        
        return {
            'customers': customer_count,
            'merchants': merchant_count,
            'categories': category_count,
            'total': customer_count + merchant_count + category_count
        }
    
    def health_check(self) -> bool:
        """Check if Redis is responsive and update metrics"""
        try:
            self.redis_client.ping()
            if METRICS_ENABLED:
                set_redis_connection_status(True)
            return True
        except:
            if METRICS_ENABLED:
                set_redis_connection_status(False)
            return False
    
    # ========================================================================
    # INITIALIZATION (Original methods preserved)
    # ========================================================================
    
    def load_from_json(self, json_path: str = "./pathway_persistence/stats_store.json"):
        """Load existing stats from JSON into Redis"""
        path = Path(json_path)
        if not path.exists():
            print(f"⚠️  No existing stats file at {json_path}")
            return 0
        
        print(f"📥 Loading stats from {json_path} into Redis...")
        
        with open(path, 'r') as f:
            data = json.load(f)
        
        loaded_count = 0
        
        if 'customers' in data:
            for cc_num, stats in data['customers'].items():
                self._set_customer_stats(cc_num, stats)
                loaded_count += 1
        
        if 'merchants' in data:
            for merchant, stats in data['merchants'].items():
                self._set_merchant_stats(merchant, stats)
                loaded_count += 1
        
        if 'categories' in data:
            for category, stats in data['categories'].items():
                self._set_category_stats(category, stats)
                loaded_count += 1
        
        print(f"✓ Loaded {loaded_count} entities into Redis")
        
        # Update entity counts in metrics
        if METRICS_ENABLED:
            summary = self.get_stats_summary()
            update_redis_entity_counts(
                summary['customers'],
                summary['merchants'],
                summary['categories']
            )
        
        return loaded_count
    
    def clear_all(self):
        """Clear all fraud detection stats from Redis"""
        patterns = [f"{CUSTOMER_PREFIX}*", f"{MERCHANT_PREFIX}*", f"{CATEGORY_PREFIX}*"]
        deleted = 0
        
        for pattern in patterns:
            keys = self.redis_client.keys(pattern)
            if keys:
                deleted += self.redis_client.delete(*keys)
        
        print(f"🗑️  Cleared {deleted} keys from Redis")
        
        # Update metrics
        if METRICS_ENABLED:
            update_redis_entity_counts(0, 0, 0)
        
        return deleted


# Global instance
_redis_store = None

def get_store() -> RedisStatsStore:
    """Get global Redis stats store instance (singleton)"""
    global _redis_store
    if _redis_store is None:
        _redis_store = RedisStatsStore()
    return _redis_store