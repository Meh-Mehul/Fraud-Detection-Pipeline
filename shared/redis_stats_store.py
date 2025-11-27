# shared/redis_stats_store.py
"""
Redis-based stats store for online fraud detection.
Replaces file-based stats_store with in-memory Redis storage.
"""
import redis
import json
from pathlib import Path
from typing import Dict, Any

# Redis connection configuration
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0

# Key prefixes for different entity types
CUSTOMER_PREFIX = "customer:"
MERCHANT_PREFIX = "merchant:"
CATEGORY_PREFIX = "category:"


class RedisStatsStore:
    """Thread-safe Redis-based statistics storage"""
    
    def __init__(self, host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB):
        """Initialize Redis connection"""
        self.redis_client = redis.Redis(
            host=host,
            port=port,
            db=db,
            decode_responses=True,  # Automatically decode bytes to strings
            socket_keepalive=True,
            socket_connect_timeout=5
        )
        
        # Test connection
        try:
            self.redis_client.ping()
            print(f"✓ Redis connected: {host}:{port} (DB {db})")
        except redis.ConnectionError as e:
            print(f"❌ Redis connection failed: {e}")
            raise
    
    # ============================================================================
    # INITIALIZATION - Load from JSON file into Redis
    # ============================================================================
    
    def load_from_json(self, json_path: str = "./pathway_persistence/stats_store.json"):
        """
        Load existing stats from JSON file into Redis.
        This is a one-time migration step.
        """
        path = Path(json_path)
        if not path.exists():
            print(f"⚠️  No existing stats file at {json_path}")
            return 0
        
        print(f"📥 Loading stats from {json_path} into Redis...")
        
        with open(path, 'r') as f:
            data = json.load(f)
        
        loaded_count = 0
        
        # Load customers
        if 'customers' in data:
            for cc_num, stats in data['customers'].items():
                self._set_customer_stats(cc_num, stats)
                loaded_count += 1
        
        # Load merchants
        if 'merchants' in data:
            for merchant, stats in data['merchants'].items():
                self._set_merchant_stats(merchant, stats)
                loaded_count += 1
        
        # Load categories
        if 'categories' in data:
            for category, stats in data['categories'].items():
                self._set_category_stats(category, stats)
                loaded_count += 1
        
        print(f"✓ Loaded {loaded_count} entities into Redis")
        return loaded_count
    
    def clear_all(self):
        """Clear all fraud detection stats from Redis (use with caution!)"""
        patterns = [f"{CUSTOMER_PREFIX}*", f"{MERCHANT_PREFIX}*", f"{CATEGORY_PREFIX}*"]
        deleted = 0
        
        for pattern in patterns:
            keys = self.redis_client.keys(pattern)
            if keys:
                deleted += self.redis_client.delete(*keys)
        
        print(f"🗑️  Cleared {deleted} keys from Redis")
        return deleted
    
    # ============================================================================
    # CUSTOMER OPERATIONS
    # ============================================================================
    
    def _get_customer_key(self, cc_num: str) -> str:
        """Get Redis key for customer"""
        return f"{CUSTOMER_PREFIX}{cc_num}"
    
    def _set_customer_stats(self, cc_num: str, stats: Dict[str, Any]):
        """Set customer stats in Redis"""
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
    
    def get_customer_profile(self, cc_num: int) -> Dict[str, float]:
        """
        Get customer statistics from Redis.
        Returns default values if customer not found.
        """
        key = self._get_customer_key(str(cc_num))
        data = self.redis_client.hgetall(key)
        
        if not data:
            # Return defaults for new customer
            return {
                'txn_count': 0,
                'fraud_history': 0,
                'avg_amt': 0.0,
                'std_amt': 0.0,
                'avg_dist': 0.0,
                'std_dist': 0.0
            }
        
        # Convert strings to appropriate types
        return {
            'txn_count': int(data.get('txn_count', 0)),
            'fraud_history': int(data.get('fraud_history', 0)),
            'avg_amt': float(data.get('avg_amt', 0.0)),
            'std_amt': float(data.get('std_amt', 0.0)),
            'avg_dist': float(data.get('avg_dist', 0.0)),
            'std_dist': float(data.get('std_dist', 0.0))
        }
    
    def update_customer(self, cc_num: str, amt: float, distance: float, is_fraud: int):
        """
        Update customer statistics using online (incremental) algorithm.
        This is atomic and thread-safe using Redis.
        """
        key = self._get_customer_key(cc_num)
        
        # Use Redis pipeline for atomic updates
        pipe = self.redis_client.pipeline()
        
        # Increment counters
        pipe.hincrby(key, 'txn_count', 1)
        pipe.hincrbyfloat(key, 'total_amt', amt)
        pipe.hincrbyfloat(key, 'total_dist', distance)
        pipe.hincrbyfloat(key, 'amt_squared', amt * amt)
        pipe.hincrbyfloat(key, 'dist_squared', distance * distance)
        
        if is_fraud:
            pipe.hincrby(key, 'fraud_history', 1)
        
        # Get updated values
        pipe.hgetall(key)
        
        results = pipe.execute()
        updated_data = results[-1]  # Last result is hgetall
        
        # Calculate running statistics
        n = int(updated_data.get('txn_count', 1))
        
        if n > 0:
            total_amt = float(updated_data.get('total_amt', 0.0))
            total_dist = float(updated_data.get('total_dist', 0.0))
            amt_sq = float(updated_data.get('amt_squared', 0.0))
            dist_sq = float(updated_data.get('dist_squared', 0.0))
            
            avg_amt = total_amt / n
            avg_dist = total_dist / n
            
            # Calculate standard deviation
            std_amt = ((amt_sq / n) - (avg_amt ** 2)) ** 0.5 if n > 1 else 0.0
            std_dist = ((dist_sq / n) - (avg_dist ** 2)) ** 0.5 if n > 1 else 0.0
            
            # Update computed fields
            pipe2 = self.redis_client.pipeline()
            pipe2.hset(key, 'avg_amt', avg_amt)
            pipe2.hset(key, 'std_amt', max(std_amt, 0.01))  # Prevent division by zero
            pipe2.hset(key, 'avg_dist', avg_dist)
            pipe2.hset(key, 'std_dist', max(std_dist, 0.01))
            pipe2.execute()
    
    # ============================================================================
    # MERCHANT OPERATIONS
    # ============================================================================
    
    def _get_merchant_key(self, merchant: str) -> str:
        """Get Redis key for merchant"""
        return f"{MERCHANT_PREFIX}{merchant}"
    
    def _set_merchant_stats(self, merchant: str, stats: Dict[str, Any]):
        """Set merchant stats in Redis"""
        key = self._get_merchant_key(merchant)
        self.redis_client.hset(key, mapping={
            'total': stats.get('total', 0),
            'fraud_count': stats.get('fraud_count', 0),
            'fraud_rate': stats.get('fraud_rate', 0.0)
        })
    
    def get_merchant_profile(self, merchant: str) -> Dict[str, float]:
        """Get merchant fraud statistics from Redis"""
        key = self._get_merchant_key(merchant)
        data = self.redis_client.hgetall(key)
        
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
    
    def update_merchant(self, merchant: str, amt: float, is_fraud: int):
        """Update merchant statistics atomically"""
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
        
        if total >= 30:  # Only calculate fraud rate after 30 transactions
            fraud_rate = fraud_count / total
        else:
            fraud_rate = 0.0
        
        self.redis_client.hset(key, 'fraud_rate', fraud_rate)
    
    # ============================================================================
    # CATEGORY OPERATIONS
    # ============================================================================
    
    def _get_category_key(self, category: str) -> str:
        """Get Redis key for category"""
        return f"{CATEGORY_PREFIX}{category}"
    
    def _set_category_stats(self, category: str, stats: Dict[str, Any]):
        """Set category stats in Redis"""
        key = self._get_category_key(category)
        self.redis_client.hset(key, mapping={
            'total': stats.get('total', 0),
            'fraud_count': stats.get('fraud_count', 0),
            'fraud_rate': stats.get('fraud_rate', 0.0)
        })
    
    def get_category_profile(self, category: str) -> Dict[str, float]:
        """Get category fraud statistics from Redis"""
        key = self._get_category_key(category)
        data = self.redis_client.hgetall(key)
        
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
    
    def update_category(self, category: str, is_fraud: int):
        """Update category statistics atomically"""
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
        
        if total >= 100:  # Only calculate fraud rate after 100 transactions
            fraud_rate = fraud_count / total
        else:
            fraud_rate = 0.0
        
        self.redis_client.hset(key, 'fraud_rate', fraud_rate)
    
    # ============================================================================
    # UTILITY METHODS
    # ============================================================================
    
    def get_stats_summary(self) -> Dict[str, int]:
        """Get summary of stored statistics"""
        customer_count = len(self.redis_client.keys(f"{CUSTOMER_PREFIX}*"))
        merchant_count = len(self.redis_client.keys(f"{MERCHANT_PREFIX}*"))
        category_count = len(self.redis_client.keys(f"{CATEGORY_PREFIX}*"))
        
        return {
            'customers': customer_count,
            'merchants': merchant_count,
            'categories': category_count,
            'total': customer_count + merchant_count + category_count
        }
    
    def health_check(self) -> bool:
        """Check if Redis is responsive"""
        try:
            self.redis_client.ping()
            return True
        except:
            return False


# Global instance
_redis_store = None

def get_store() -> RedisStatsStore:
    """Get global Redis stats store instance (singleton)"""
    global _redis_store
    if _redis_store is None:
        _redis_store = RedisStatsStore()
    return _redis_store


# Convenience functions for backward compatibility
def get_customer_profile(cc_num: int) -> Dict[str, float]:
    return get_store().get_customer_profile(cc_num)

def get_merchant_profile(merchant: str) -> Dict[str, float]:
    return get_store().get_merchant_profile(merchant)

def get_category_profile(category: str) -> Dict[str, float]:
    return get_store().get_category_profile(category)

def update_customer(cc_num: str, amt: float, distance: float, is_fraud: int):
    get_store().update_customer(cc_num, amt, distance, is_fraud)

def update_merchant(merchant: str, amt: float, is_fraud: int):
    get_store().update_merchant(merchant, amt, is_fraud)

def update_category(category: str, is_fraud: int):
    get_store().update_category(category, is_fraud)


if __name__ == "__main__":
    # Test/initialization script
    print("═══════════════════════════════════════════")
    print("   REDIS STATS STORE - TEST/INIT")
    print("═══════════════════════════════════════════")
    
    store = RedisStatsStore()
    
    # Load from existing JSON file
    loaded = store.load_from_json()
    
    # Show summary
    summary = store.get_stats_summary()
    print(f"\n📊 Redis Stats Summary:")
    print(f"   Customers: {summary['customers']:,}")
    print(f"   Merchants: {summary['merchants']:,}")
    print(f"   Categories: {summary['categories']:,}")
    print(f"   Total: {summary['total']:,}")
    
    # Test a few lookups
    print(f"\n🔍 Sample Lookups:")
    test_cc = 4761049645711555825
    cust = store.get_customer_profile(test_cc)
    print(f"   Customer {test_cc}:")
    print(f"      Transactions: {cust['txn_count']}")
    print(f"      Avg Amount: ${cust['avg_amt']:.2f}")
    print(f"      Fraud History: {cust['fraud_history']}")
    
    print(f"\n✓ Redis stats store ready!")