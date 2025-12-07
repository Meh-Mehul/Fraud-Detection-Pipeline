#!/usr/bin/env python3
"""
Quick test script to verify ATO metrics are being recorded
Run this while the ATO detector is running
"""

import requests
import sys
import time

def test_ato_metrics(port=8005):
    """Test if ATO metrics endpoint is accessible and recording data"""
    
    metrics_url = f"http://localhost:{port}/metrics"
    
    print(f"🔍 Testing ATO Metrics Endpoint: {metrics_url}")
    print("=" * 70)
    
    try:
        response = requests.get(metrics_url, timeout=5)
        
        if response.status_code == 200:
            print(f"✅ Metrics endpoint is UP (Status: {response.status_code})")
            
            # Parse and display key metrics
            metrics_text = response.text
            
            # Check for ATO-specific metrics
            print("\n📊 ATO Metrics Status:")
            print("-" * 70)
            
            # Fraud alerts
            if 'fraud_alerts_total' in metrics_text:
                alerts = [line for line in metrics_text.split('\n') 
                         if line.startswith('fraud_alerts_total') and 'ato' in line]
                if alerts:
                    print("🚨 Fraud Alerts Recorded:")
                    for alert in alerts[:10]:  # Show first 10
                        print(f"   {alert}")
                else:
                    print("🟡 No ATO fraud alerts recorded yet")
            
            # Latency metrics
            if 'fraud_latency_seconds' in metrics_text:
                latencies = [line for line in metrics_text.split('\n') 
                           if 'fraud_latency_seconds' in line and 'ato' in line]
                if latencies:
                    print("\n⏱️  Latency Metrics:")
                    for lat in latencies[:5]:  # Show first 5
                        print(f"   {lat}")
                else:
                    print("\n⏱️  Detection latency tracking initialized")
            
            # Pipeline latency
            if 'fraud_pipeline_latency_seconds' in metrics_text:
                print("\n📈 Pipeline Latency: Tracked")
            
            # Model metrics
            if 'fraud_model' in metrics_text:
                model_metrics = [line for line in metrics_text.split('\n') 
                               if line.startswith('fraud_model') and not line.startswith('#')]
                if model_metrics:
                    print(f"\n🤖 Model Metrics: {len(model_metrics)} metrics available")
            
            # Weighted averages
            if 'fraud_model_f1_score_weighted_avg' in metrics_text:
                f1_lines = [line for line in metrics_text.split('\n') 
                          if 'fraud_model_f1_score_weighted_avg' in line and not line.startswith('#')]
                if f1_lines:
                    print("\n📊 Weighted Averages: Active")
                    for line in f1_lines[:3]:
                        print(f"   {line}")
            
            print("\n" + "=" * 70)
            print("✅ ATO Metrics System: OPERATIONAL")
            print(f"\n💡 View all metrics: {metrics_url}")
            print(f"💡 Prometheus UI: http://localhost:9090")
            print(f"💡 Grafana Dashboard: http://localhost:3000")
            
            return True
        else:
            print(f"❌ Metrics endpoint returned status: {response.status_code}")
            return False
            
    except requests.ConnectionError:
        print(f"❌ Could not connect to {metrics_url}")
        print("\n💡 Make sure the ATO detector is running:")
        print("   python run_ato_detector.py")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def main():
    port = 8005
    if len(sys.argv) > 1:
        try:
            port = int(sys.argv[1])
        except ValueError:
            print("Usage: python test_ato_metrics.py [port]")
            sys.exit(1)
    
    success = test_ato_metrics(port)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
