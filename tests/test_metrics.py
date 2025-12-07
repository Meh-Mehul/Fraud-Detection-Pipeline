#!/usr/bin/env python3
"""
Test script to verify Prometheus metrics are working
Run this before starting your full pipeline
"""
import time
import sys
from pathlib import Path

# Add shared to path
current_file = Path(__file__).resolve()
project_root = current_file.parent
shared_path = project_root / "shared"
sys.path.append(str(shared_path))

try:
    from metrics import (
        initialize_metrics,
        record_fraud_alert,
        record_latency,
        record_pipeline_latency,
        record_model_update,
        set_model_weight_delta,
        get_metrics_manager
    )
    print("✅ Successfully imported metrics module")
except ImportError as e:
    print(f"❌ Failed to import metrics: {e}")
    print(f"   Make sure 'shared/metrics.py' exists")
    print(f"   Install: pip install prometheus-client")
    sys.exit(1)

def test_metrics_server():
    """Test metrics server initialization"""
    print("\n" + "="*60)
    print("TESTING PROMETHEUS METRICS SERVER")
    print("="*60)
    
    # Test 1: Initialize metrics
    print("\n1️⃣  Initializing metrics server on port 8001...")
    try:
        manager = initialize_metrics("test_component", port=8001)
        print("   ✅ Metrics manager initialized")
    except Exception as e:
        print(f"   ❌ Failed to initialize: {e}")
        return False
    
    # Test 2: Record some metrics
    print("\n2️⃣  Recording test metrics...")
    try:
        # Pipeline latency (new)
        record_pipeline_latency("publisher_to_detector", 0.025)  # 25ms
        record_pipeline_latency("detector_to_report", 0.015)     # 15ms
        
        # Internal latency
        record_latency("detector_total", 0.020)
        record_latency("ml_inference", 0.010)
        
        # Alerts
        record_fraud_alert(tier="1", pattern="TEST_PATTERN", risk_score=85.5, component="test")
        record_fraud_alert(tier="2", pattern="ANOTHER_PATTERN", risk_score=72.0, component="test")
        
        # Model updates
        record_model_update()
        set_model_weight_delta(0.05)  # 5% change
        
        print("   ✅ Metrics recorded successfully")
    except Exception as e:
        print(f"   ❌ Failed to record metrics: {e}")
        return False
    
    # Test 3: Check metrics endpoint
    print("\n3️⃣  Checking metrics endpoint...")
    try:
        import urllib.request
        with urllib.request.urlopen('http://localhost:8001/metrics', timeout=5) as response:
            metrics_text = response.read().decode('utf-8')
            
            # Verify expected metrics exist
            expected_metrics = [
                'fraud_pipeline_latency_seconds',
                'fraud_latency_seconds',
                'fraud_alerts_total',
                'fraud_model_updates_total',
                'fraud_model_weight_delta',
            ]
            
            missing = []
            for metric in expected_metrics:
                if metric not in metrics_text:
                    missing.append(metric)
            
            if missing:
                print(f"   ⚠️  Missing metrics: {missing}")
            else:
                print("   ✅ All expected metrics found")
            
            # Show sample metrics
            print("\n   📊 Sample metrics output:")
            lines = metrics_text.split('\n')
            for line in lines[:25]:
                if line and not line.startswith('#'):
                    print(f"      {line}")
            
    except urllib.error.URLError as e:
        print(f"   ❌ Cannot connect to metrics endpoint: {e}")
        print("   This means the HTTP server didn't start properly")
        return False
    except Exception as e:
        print(f"   ❌ Error checking endpoint: {e}")
        return False
    
    return True


def test_prometheus_connection():
    """Test if Prometheus can connect"""
    print("\n" + "="*60)
    print("TESTING PROMETHEUS CONNECTION")
    print("="*60)
    
    print("\n4️⃣  Checking if Prometheus is running...")
    try:
        import urllib.request
        with urllib.request.urlopen('http://localhost:9090/-/healthy', timeout=5) as response:
            print("   ✅ Prometheus is running")
            
            # Check if our metrics endpoint is configured
            print("\n5️⃣  Checking Prometheus targets...")
            with urllib.request.urlopen('http://localhost:9090/api/v1/targets', timeout=5) as response:
                import json
                data = json.loads(response.read().decode('utf-8'))
                
                if data['status'] == 'success':
                    active_targets = data['data']['activeTargets']
                    print(f"   Found {len(active_targets)} targets:")
                    
                    for target in active_targets:
                        job = target['labels'].get('job', 'unknown')
                        health = target['health']
                        endpoint = target['scrapeUrl']
                        
                        status_symbol = "✅" if health == "up" else "❌"
                        print(f"   {status_symbol} {job}: {health} - {endpoint}")
                
            return True
            
    except urllib.error.URLError:
        print("   ⚠️  Prometheus not running or not accessible")
        print("   Start it with: docker-compose -f docker-compose-monitoring.yml up -d")
        return False
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False


def main():
    """Run all tests"""
    print("""
╔══════════════════════════════════════════════════════════╗
║         PROMETHEUS METRICS TEST SUITE                    ║
║         (Pipeline Latency Edition)                       ║
╚══════════════════════════════════════════════════════════╝
    """)
    
    # Run metrics server test
    metrics_ok = test_metrics_server()
    
    if not metrics_ok:
        print("\n" + "="*60)
        print("❌ METRICS TEST FAILED")
        print("="*60)
        print("\nTroubleshooting steps:")
        print("1. Install prometheus-client: pip install prometheus-client")
        print("2. Check if port 8001 is available: lsof -i :8001")
        print("3. Make sure shared/metrics.py exists with correct code")
        sys.exit(1)
    
    # Run Prometheus connection test
    prometheus_ok = test_prometheus_connection()
    
    # Final summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    print(f"Metrics Server: {'✅ PASS' if metrics_ok else '❌ FAIL'}")
    print(f"Prometheus:     {'✅ PASS' if prometheus_ok else '⚠️  NOT RUNNING'}")
    
    print("\n📊 Metrics being tracked:")
    print("   • fraud_pipeline_latency_seconds (publisher→detector, detector→report)")
    print("   • fraud_latency_seconds (detector_total, ml_inference)")
    print("   • fraud_alerts_total (by tier and pattern)")
    print("   • fraud_model_updates_total (update frequency)")
    print("   • fraud_model_weight_delta (weight changes)")
    
    if metrics_ok:
        print("\n✅ Metrics server is working correctly!")
        print("\n📊 Metrics endpoint: http://localhost:8001/metrics")
        print("🔍 Prometheus UI: http://localhost:9090")
        print("📈 Grafana: http://localhost:3000 (admin/admin)")
        
        print("\n🚀 Next steps:")
        if not prometheus_ok:
            print("1. Start Prometheus: docker-compose -f docker-compose-monitoring.yml up -d")
            print("2. Wait 30 seconds for scraping to begin")
            print("3. Check targets: http://localhost:9090/targets")
        else:
            print("1. Start your fraud detection pipeline")
            print("2. Check metrics are being collected in Prometheus")
            print("3. View dashboard in Grafana")
        
        print("\nKeeping test server running for 30 seconds...")
        print("Press Ctrl+C to stop early\n")
        
        try:
            for i in range(30):
                time.sleep(1)
                # Record some test metrics
                record_pipeline_latency("publisher_to_detector", 0.02 + (i % 5) * 0.01)
                record_pipeline_latency("detector_to_report", 0.01 + (i % 3) * 0.005)
                if i % 10 == 0:
                    record_fraud_alert(tier="1", pattern="TEST_ALERT", risk_score=85.0, component="test")
                print(f"⏱️  Running... {i+1}s / 30s", end='\r')
        except KeyboardInterrupt:
            print("\n\nℹ️  Stopped by user")
    else:
        print("\n❌ Please fix the issues above and try again")
        sys.exit(1)


if __name__ == "__main__":
    main()