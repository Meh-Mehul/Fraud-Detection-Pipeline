#!/usr/bin/env python3
"""
redis_manager.py - Utility for managing Redis stats store

Usage:
    python redis_manager.py load        # Load stats from JSON
    python redis_manager.py clear       # Clear all stats
    python redis_manager.py stats       # Show summary
    python redis_manager.py inspect CC  # Inspect customer
    python redis_manager.py export      # Export to JSON
"""

import sys
import json
from pathlib import Path

# Add parent to path
ROOT = Path(__file__).resolve().parent
sys.path.append(str(ROOT))

from shared import redis_stats_store


def load_from_json():
    """Load stats from JSON file into Redis"""
    print("═══════════════════════════════════════════")
    print("   LOAD STATS FROM JSON → REDIS")
    print("═══════════════════════════════════════════")
    
    json_path = "./pathway_persistence/stats_store.json"
    
    if not Path(json_path).exists():
        print(f"❌ File not found: {json_path}")
        return
    
    store = redis_stats_store.RedisStatsStore()
    
    print(f"📥 Loading from {json_path}...")
    loaded = store.load_from_json(json_path)
    
    summary = store.get_stats_summary()
    print(f"\n✓ Successfully loaded {loaded} entities")
    print(f"   Customers:  {summary['customers']:,}")
    print(f"   Merchants:  {summary['merchants']:,}")
    print(f"   Categories: {summary['categories']:,}")


def clear_all():
    """Clear all fraud detection stats from Redis"""
    print("═══════════════════════════════════════════")
    print("   CLEAR ALL STATS FROM REDIS")
    print("═══════════════════════════════════════════")
    
    confirm = input("⚠️  This will delete ALL fraud detection data. Continue? (yes/no): ")
    
    if confirm.lower() != 'yes':
        print("❌ Cancelled")
        return
    
    store = redis_stats_store.RedisStatsStore()
    deleted = store.clear_all()
    
    print(f"✓ Deleted {deleted} keys from Redis")


def show_stats():
    """Show summary statistics"""
    print("═══════════════════════════════════════════")
    print("   REDIS STATS SUMMARY")
    print("═══════════════════════════════════════════")
    
    store = redis_stats_store.RedisStatsStore()
    
    # Connection health
    if store.health_check():
        print("✓ Redis connection: OK")
    else:
        print("❌ Redis connection: FAILED")
        return
    
    summary = store.get_stats_summary()
    
    print(f"\n📊 Entity Counts:")
    print(f"   Customers:  {summary['customers']:,}")
    print(f"   Merchants:  {summary['merchants']:,}")
    print(f"   Categories: {summary['categories']:,}")
    print(f"   Total:      {summary['total']:,}")
    
    # Sample some customers with fraud history
    print(f"\n🔍 Sample Customers with Fraud History:")
    
    redis_client = store.redis_client
    customer_keys = redis_client.keys("customer:*")
    
    fraud_customers = []
    for key in customer_keys[:100]:  # Check first 100
        fraud_hist = redis_client.hget(key, 'fraud_history')
        if fraud_hist and int(fraud_hist) > 0:
            cc_num = key.split(':')[1]
            fraud_customers.append((cc_num, int(fraud_hist)))
    
    fraud_customers.sort(key=lambda x: x[1], reverse=True)
    
    for cc_num, fraud_hist in fraud_customers[:5]:
        cust = store.get_customer_profile(int(cc_num))
        print(f"   CC {cc_num[-8:]}: {fraud_hist} frauds, "
              f"{cust['txn_count']} txns, avg ${cust['avg_amt']:.2f}")


def inspect_customer(cc_num):
    """Inspect a specific customer"""
    print("═══════════════════════════════════════════")
    print(f"   CUSTOMER PROFILE: {cc_num}")
    print("═══════════════════════════════════════════")
    
    store = redis_stats_store.RedisStatsStore()
    
    try:
        cust = store.get_customer_profile(int(cc_num))
        
        print(f"\n📈 Transaction Statistics:")
        print(f"   Total Transactions: {cust['txn_count']}")
        print(f"   Fraud History:      {cust['fraud_history']}")
        print(f"   Fraud Rate:         {(cust['fraud_history']/cust['txn_count']*100) if cust['txn_count'] > 0 else 0:.2f}%")
        
        print(f"\n💰 Amount Statistics:")
        print(f"   Average:            ${cust['avg_amt']:.2f}")
        print(f"   Std Deviation:      ${cust['std_amt']:.2f}")
        
        print(f"\n📍 Distance Statistics:")
        print(f"   Average:            {cust['avg_dist']:.2f} km")
        print(f"   Std Deviation:      {cust['std_dist']:.2f} km")
        
    except Exception as e:
        print(f"❌ Error: {e}")


def export_to_json():
    """Export Redis stats back to JSON"""
    print("═══════════════════════════════════════════")
    print("   EXPORT REDIS → JSON")
    print("═══════════════════════════════════════════")
    
    store = redis_stats_store.RedisStatsStore()
    redis_client = store.redis_client
    
    output = {
        'customers': {},
        'merchants': {},
        'categories': {}
    }
    
    print("📤 Exporting customers...")
    for key in redis_client.scan_iter(match="customer:*"):
        cc_num = key.split(':')[1]
        data = redis_client.hgetall(key)
        output['customers'][cc_num] = {
            'txn_count': int(data.get('txn_count', 0)),
            'fraud_history': int(data.get('fraud_history', 0)),
            'avg_amt': float(data.get('avg_amt', 0.0)),
            'std_amt': float(data.get('std_amt', 0.0)),
            'avg_dist': float(data.get('avg_dist', 0.0)),
            'std_dist': float(data.get('std_dist', 0.0))
        }
    
    print("📤 Exporting merchants...")
    for key in redis_client.scan_iter(match="merchant:*"):
        merchant = key.split(':', 1)[1]
        data = redis_client.hgetall(key)
        output['merchants'][merchant] = {
            'total': int(data.get('total', 0)),
            'fraud_count': int(data.get('fraud_count', 0)),
            'fraud_rate': float(data.get('fraud_rate', 0.0))
        }
    
    print("📤 Exporting categories...")
    for key in redis_client.scan_iter(match="category:*"):
        category = key.split(':', 1)[1]
        data = redis_client.hgetall(key)
        output['categories'][category] = {
            'total': int(data.get('total', 0)),
            'fraud_count': int(data.get('fraud_count', 0)),
            'fraud_rate': float(data.get('fraud_rate', 0.0))
        }
    
    output_path = "./redis_export.json"
    with open(output_path, 'w') as f:
        json.dump(output, f, indent=2)
    
    print(f"\n✓ Exported to {output_path}")
    print(f"   Customers:  {len(output['customers']):,}")
    print(f"   Merchants:  {len(output['merchants']):,}")
    print(f"   Categories: {len(output['categories']):,}")


def show_help():
    """Show usage information"""
    print("═══════════════════════════════════════════")
    print("   REDIS STATS MANAGER")
    print("═══════════════════════════════════════════")
    print()
    print("Usage:")
    print("  python redis_manager.py load              Load stats from JSON")
    print("  python redis_manager.py clear             Clear all stats")
    print("  python redis_manager.py stats             Show summary")
    print("  python redis_manager.py inspect <CC>      Inspect customer")
    print("  python redis_manager.py export            Export to JSON")
    print("  python redis_manager.py help              Show this help")
    print()
    print("Examples:")
    print("  python redis_manager.py load")
    print("  python redis_manager.py inspect 4761049645711555825")
    print("  python redis_manager.py export")


def main():
    if len(sys.argv) < 2:
        show_help()
        return
    
    command = sys.argv[1].lower()
    
    try:
        if command == 'load':
            load_from_json()
        elif command == 'clear':
            clear_all()
        elif command == 'stats':
            show_stats()
        elif command == 'inspect':
            if len(sys.argv) < 3:
                print("❌ Usage: python redis_manager.py inspect <CC_NUMBER>")
                return
            inspect_customer(sys.argv[2])
        elif command == 'export':
            export_to_json()
        elif command == 'help':
            show_help()
        else:
            print(f"❌ Unknown command: {command}")
            show_help()
    
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()