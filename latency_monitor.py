#!/usr/bin/env python3
"""
Latency Monitor - Auto-restart pipeline if latency exceeds threshold
For Inter IIT Demo - safety feature to ensure smooth operation
"""

import subprocess
import time
import requests
import os
import signal
import sys
from datetime import datetime

# Configuration
PROMETHEUS_URL = "http://localhost:9090"
LATENCY_THRESHOLD_SECONDS = 10.0  # Restart if latency > 10s
CHECK_INTERVAL_SECONDS = 5  # Check every 5 seconds
STARTUP_DELAY_SECONDS = 60  # Wait 60s after restart before checking again

# Process management
COMPONENTS = [
    {"name": "detector", "cmd": "python run_detector.py", "pid": None},
    {"name": "report", "cmd": "python run_report.py", "pid": None},
    {"name": "feedback", "cmd": "python run_feedback.py", "pid": None},
    {"name": "stats_updater", "cmd": "python run_stats_updater.py", "pid": None},
    {"name": "publisher", "cmd": "python publisher/pub_common.py", "pid": None},
]

running = True

def signal_handler(sig, frame):
    global running
    print("\n🛑 Stopping latency monitor...")
    running = False
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

def get_current_latency():
    """Query Prometheus for current p50 latency"""
    try:
        query = 'histogram_quantile(0.50, rate(fraud_pipeline_latency_seconds_bucket{stage="detector_to_report"}[1m]))'
        response = requests.get(
            f"{PROMETHEUS_URL}/api/v1/query",
            params={"query": query},
            timeout=5
        )
        data = response.json()
        
        if data.get("status") == "success":
            results = data.get("data", {}).get("result", [])
            if results:
                value = float(results[0].get("value", [0, 0])[1])
                return value if value > 0 and value < 1000 else None
        return None
    except Exception as e:
        print(f"⚠️  Error querying Prometheus: {e}")
        return None

def stop_all_components():
    """Stop all pipeline components"""
    print("🔄 Stopping all components...")
    subprocess.run(["pkill", "-f", "run_detector.py"], capture_output=True)
    subprocess.run(["pkill", "-f", "run_report.py"], capture_output=True)
    subprocess.run(["pkill", "-f", "run_feedback.py"], capture_output=True)
    subprocess.run(["pkill", "-f", "run_stats_updater.py"], capture_output=True)
    subprocess.run(["pkill", "-f", "pub_common.py"], capture_output=True)
    time.sleep(2)
    print("✓ All components stopped")

def start_all_components():
    """Start all pipeline components in background"""
    print("🚀 Starting all components...")
    
    project_dir = os.path.dirname(os.path.abspath(__file__))
    
    for comp in COMPONENTS:
        print(f"   Starting {comp['name']}...")
        process = subprocess.Popen(
            comp['cmd'].split(),
            cwd=project_dir,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True
        )
        comp['pid'] = process.pid
        time.sleep(1)  # Small delay between starts
    
    print("✓ All components started")

def restart_pipeline():
    """Full pipeline restart"""
    print(f"\n{'='*60}")
    print(f"⚠️  HIGH LATENCY DETECTED - RESTARTING PIPELINE")
    print(f"   Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")
    
    stop_all_components()
    time.sleep(2)
    start_all_components()
    
    print(f"\n✓ Pipeline restarted. Waiting {STARTUP_DELAY_SECONDS}s before next check...")
    time.sleep(STARTUP_DELAY_SECONDS)

def main():
    print("╔═══════════════════════════════════════════╗")
    print("║     LATENCY MONITOR - Inter IIT Demo      ║")
    print("╚═══════════════════════════════════════════╝")
    print(f"Threshold: {LATENCY_THRESHOLD_SECONDS}s")
    print(f"Check interval: {CHECK_INTERVAL_SECONDS}s")
    print(f"Prometheus: {PROMETHEUS_URL}")
    print()
    print("Press Ctrl+C to stop monitoring")
    print("─" * 43)
    
    last_restart = 0
    
    while running:
        latency = get_current_latency()
        
        if latency is not None:
            status = "✅" if latency < LATENCY_THRESHOLD_SECONDS else "⚠️"
            print(f"{datetime.now().strftime('%H:%M:%S')} | Latency: {latency:.2f}s {status}")
            
            # Check if restart needed
            if latency > LATENCY_THRESHOLD_SECONDS:
                # Avoid restart loops - wait at least 2 minutes between restarts
                if time.time() - last_restart > 120:
                    restart_pipeline()
                    last_restart = time.time()
                else:
                    print("   (Skipping restart - too soon since last restart)")
        else:
            print(f"{datetime.now().strftime('%H:%M:%S')} | No latency data (pipeline warming up)")
        
        time.sleep(CHECK_INTERVAL_SECONDS)

if __name__ == "__main__":
    main()
