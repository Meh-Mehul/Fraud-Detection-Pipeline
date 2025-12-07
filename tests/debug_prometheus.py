#!/usr/bin/env python3
"""
Comprehensive Prometheus Debug Script
Diagnoses why Prometheus can't scrape metrics
"""

import urllib.request
import urllib.error
import json
import subprocess
import sys
from pathlib import Path

def check_prometheus_running():
    """Check if Prometheus is running"""
    print("\n" + "="*70)
    print("1️⃣  CHECKING PROMETHEUS STATUS")
    print("="*70)
    
    try:
        with urllib.request.urlopen('http://localhost:9090/-/healthy', timeout=5) as response:
            print("✅ Prometheus is running on http://localhost:9090")
            return True
    except urllib.error.URLError as e:
        print(f"❌ Prometheus is NOT running: {e}")
        print("\n🔧 Fix: Start Prometheus with:")
        print("   docker-compose -f docker-compose-monitoring.yml up -d prometheus")
        return False

def check_prometheus_targets():
    """Check Prometheus target configuration and health"""
    print("\n" + "="*70)
    print("2️⃣  CHECKING PROMETHEUS TARGETS")
    print("="*70)
    
    try:
        with urllib.request.urlopen('http://localhost:9090/api/v1/targets', timeout=5) as response:
            data = json.loads(response.read().decode('utf-8'))
            
            if data['status'] != 'success':
                print("❌ Failed to get targets")
                return False
            
            active_targets = data['data']['activeTargets']
            print(f"\n📊 Found {len(active_targets)} configured targets:\n")
            
            all_up = True
            for target in active_targets:
                job = target['labels'].get('job', 'unknown')
                health = target['health']
                endpoint = target['scrapeUrl']
                last_error = target.get('lastError', '')
                last_scrape = target.get('lastScrape', '')
                
                status = "✅" if health == "up" else "❌"
                print(f"{status} {job}")
                print(f"   Endpoint: {endpoint}")
                print(f"   Health: {health}")
                
                if health != "up":
                    all_up = False
                    print(f"   ⚠️  Last Error: {last_error}")
                    print(f"   Last Scrape: {last_scrape}")
                
                print()
            
            if not all_up:
                print("⚠️  Some targets are DOWN - this is why Prometheus is empty!")
                return False
            
            return True
            
    except Exception as e:
        print(f"❌ Error checking targets: {e}")
        return False

def check_host_docker_internal():
    """Check if host.docker.internal is accessible from Docker"""
    print("\n" + "="*70)
    print("3️⃣  CHECKING host.docker.internal CONNECTIVITY")
    print("="*70)
    
    print("\n🔍 Testing if Prometheus container can reach host machine...")
    
    try:
        # Try to exec into prometheus container and ping host
        result = subprocess.run(
            ['docker', 'exec', 'fraud-prometheus', 'ping', '-c', '1', 'host.docker.internal'],
            capture_output=True,
            text=True,
            timeout=5
        )
        
        if result.returncode == 0:
            print("✅ host.docker.internal is reachable from Prometheus container")
            return True
        else:
            print("❌ host.docker.internal is NOT reachable")
            print(f"   Error: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print("❌ Timeout trying to reach host.docker.internal")
        return False
    except FileNotFoundError:
        print("⚠️  Docker command not found - skipping this check")
        return None
    except Exception as e:
        print(f"⚠️  Could not check: {e}")
        return None

def check_metrics_endpoints():
    """Check if metrics endpoints are accessible"""
    print("\n" + "="*70)
    print("4️⃣  CHECKING METRICS ENDPOINTS (from host)")
    print("="*70)
    
    endpoints = {
        'Detector': 'http://localhost:8001/metrics',
        'Stats Updater': 'http://localhost:8002/metrics',
        'Feedback': 'http://localhost:8003/metrics',
        'Report Generator': 'http://localhost:8004/metrics',
    }
    
    all_accessible = True
    
    for name, url in endpoints.items():
        try:
            with urllib.request.urlopen(url, timeout=2) as response:
                content = response.read().decode('utf-8')
                lines = [l for l in content.split('\n') if l and not l.startswith('#')]
                print(f"✅ {name}: {url}")
                print(f"   → {len(lines)} metric lines")
        except urllib.error.URLError:
            print(f"❌ {name}: {url}")
            print(f"   → NOT RUNNING or not accessible")
            all_accessible = False
        except Exception as e:
            print(f"⚠️  {name}: {url}")
            print(f"   → Error: {e}")
            all_accessible = False
    
    return all_accessible

def check_prometheus_config():
    """Check if prometheus.yml exists and is correct"""
    print("\n" + "="*70)
    print("5️⃣  CHECKING PROMETHEUS CONFIGURATION")
    print("="*70)
    
    config_file = Path("monitoring/prometheus.yml")
    
    if not config_file.exists():
        print(f"❌ Config file not found: {config_file}")
        print("\n🔧 Fix: Create monitoring/prometheus.yml with correct scrape configs")
        return False
    
    print(f"✅ Found config file: {config_file}")
    
    # Read and validate config
    with open(config_file, 'r') as f:
        content = f.read()
    
    print("\n📄 Checking scrape_configs...")
    
    required_ports = ['8001', '8002', '8003', '8004']
    found_ports = []
    
    for port in required_ports:
        if port in content:
            print(f"✅ Port {port} configured")
            found_ports.append(port)
        else:
            print(f"❌ Port {port} NOT configured")
    
    if len(found_ports) < len(required_ports):
        print(f"\n⚠️  Missing {len(required_ports) - len(found_ports)} port configurations")
        return False
    
    # Check for host.docker.internal
    if 'host.docker.internal' in content:
        print("✅ Using host.docker.internal (correct for Docker)")
    elif 'localhost' in content:
        print("⚠️  Using 'localhost' - should be 'host.docker.internal' for Docker")
        print("   Prometheus inside Docker can't access host's localhost")
        return False
    
    return True

def test_scrape_from_container():
    """Try to scrape metrics from inside Prometheus container"""
    print("\n" + "="*70)
    print("6️⃣  TESTING SCRAPE FROM INSIDE CONTAINER")
    print("="*70)
    
    print("\n🔍 Attempting to curl metrics from inside Prometheus container...")
    
    try:
        result = subprocess.run(
            ['docker', 'exec', 'fraud-prometheus', 
             'wget', '-O-', '-q', 'http://host.docker.internal:8001/metrics'],
            capture_output=True,
            text=True,
            timeout=5
        )
        
        if result.returncode == 0:
            lines = [l for l in result.stdout.split('\n') if l and not l.startswith('#')]
            print(f"✅ Successfully scraped from port 8001")
            print(f"   → {len(lines)} metric lines retrieved")
            print(f"\n   Sample metrics:")
            for line in lines[:5]:
                print(f"   {line}")
            return True
        else:
            print(f"❌ Failed to scrape from port 8001")
            print(f"   Error: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print("❌ Timeout trying to scrape metrics")
        return False
    except Exception as e:
        print(f"⚠️  Could not test: {e}")
        return None

def check_prometheus_data():
    """Check if Prometheus has any data"""
    print("\n" + "="*70)
    print("7️⃣  CHECKING PROMETHEUS DATA")
    print("="*70)
    
    # Query for any fraud metrics
    queries = [
        'fraud_transactions_total',
        'fraud_alerts_total',
        'fraud_latency_seconds',
        'up'  # Basic metric that should always exist
    ]
    
    found_any = False
    
    for query in queries:
        try:
            url = f'http://localhost:9090/api/v1/query?query={query}'
            with urllib.request.urlopen(url, timeout=5) as response:
                data = json.loads(response.read().decode('utf-8'))
                
                if data['status'] == 'success':
                    result_count = len(data['data']['result'])
                    if result_count > 0:
                        print(f"✅ {query}: {result_count} time series found")
                        found_any = True
                    else:
                        print(f"⚠️  {query}: No data")
        except Exception as e:
            print(f"❌ {query}: Error - {e}")
    
    return found_any

def main():
    """Run all diagnostic checks"""
    print("""
╔══════════════════════════════════════════════════════════════════╗
║           PROMETHEUS DEBUGGING TOOL                               ║
║           Finding why Prometheus is empty...                      ║
╚══════════════════════════════════════════════════════════════════╝
""")
    
    results = {}
    
    # Run all checks
    results['prometheus_running'] = check_prometheus_running()
    results['config_valid'] = check_prometheus_config()
    results['metrics_endpoints'] = check_metrics_endpoints()
    results['targets'] = check_prometheus_targets()
    results['host_connectivity'] = check_host_docker_internal()
    results['container_scrape'] = test_scrape_from_container()
    results['has_data'] = check_prometheus_data()
    
    # Summary
    print("\n" + "="*70)
    print("📋 DIAGNOSTIC SUMMARY")
    print("="*70 + "\n")
    
    def status(value):
        if value is True:
            return "✅ PASS"
        elif value is False:
            return "❌ FAIL"
        else:
            return "⚠️  SKIP"
    
    print(f"1. Prometheus Running:        {status(results['prometheus_running'])}")
    print(f"2. Config Valid:              {status(results['config_valid'])}")
    print(f"3. Metrics Endpoints:         {status(results['metrics_endpoints'])}")
    print(f"4. Targets Configured:        {status(results['targets'])}")
    print(f"5. Host Connectivity:         {status(results['host_connectivity'])}")
    print(f"6. Container Can Scrape:      {status(results['container_scrape'])}")
    print(f"7. Has Data:                  {status(results['has_data'])}")
    
    # Diagnosis
    print("\n" + "="*70)
    print("🔍 DIAGNOSIS")
    print("="*70 + "\n")
    
    if not results['prometheus_running']:
        print("❌ ISSUE: Prometheus is not running")
        print("\n💡 SOLUTION:")
        print("   docker-compose -f docker-compose-monitoring.yml up -d prometheus")
    
    elif not results['config_valid']:
        print("❌ ISSUE: Prometheus configuration is invalid or missing")
        print("\n💡 SOLUTION:")
        print("   1. Ensure monitoring/prometheus.yml exists")
        print("   2. Check it has scrape_configs for ports 8001-8004")
        print("   3. Use 'host.docker.internal' not 'localhost'")
    
    elif not results['metrics_endpoints']:
        print("❌ ISSUE: Metrics endpoints are not accessible")
        print("\n💡 SOLUTION:")
        print("   Start your fraud detection pipeline components:")
        print("   1. python detector_ronly.py")
        print("   2. python detector_stats_upd.py")
        print("   3. python feedback_writer.py")
        print("   4. python pathway_nats_report.py")
    
    elif results['host_connectivity'] is False:
        print("❌ ISSUE: Prometheus container cannot reach host machine")
        print("\n💡 SOLUTION:")
        print("   On macOS/Windows: host.docker.internal should work")
        print("   On Linux: Use --network=host or host's IP address")
        print("   Alternative: Change prometheus.yml to use container IPs")
    
    elif results['container_scrape'] is False:
        print("❌ ISSUE: Prometheus can't scrape metrics from inside container")
        print("\n💡 SOLUTION:")
        print("   This is likely a networking issue.")
        print("   1. Check if metrics endpoints are running")
        print("   2. Verify firewall isn't blocking ports 8001-8004")
        print("   3. Try restarting Docker: docker-compose restart")
    
    elif not results['has_data']:
        print("⚠️  ISSUE: Prometheus is running but has no data yet")
        print("\n💡 SOLUTION:")
        print("   This is normal if you just started everything!")
        print("   1. Wait 10-15 seconds for first scrape")
        print("   2. Check http://localhost:9090/targets")
        print("   3. Verify targets show 'UP' status")
        print("   4. Run your pipeline to generate metrics")
    
    else:
        print("✅ Everything looks good!")
        print("   If you still don't see data:")
        print("   1. Check the time range in Prometheus (default is last 1h)")
        print("   2. Make sure your pipeline is actively processing data")
        print("   3. Try querying: fraud_transactions_total")

if __name__ == "__main__":
    main()