"""
Complete Fraud Detection System Orchestrator
Manages all components of the feedback-enhanced fraud detection system

ARCHITECTURE:
┌──────────────────────────────────────────────────────────────────┐
│                      TRAINING PIPELINE                            │
│  FraudTrain.csv → Publisher → Detector → Shared Model Storage    │
└──────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────┐
│                      INFERENCE PIPELINE                           │
│  FraudTest.csv → Test Publisher → Inference Detector → Dashboard │
└──────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────┐
│                      FEEDBACK LOOP                                │
│  Dashboard → Employee Review → Feedback Trainer → Model Update   │
└──────────────────────────────────────────────────────────────────┘
"""

import subprocess
import time
import sys
from pathlib import Path

COMPONENTS = {
    "nats": {
        "name": "NATS Server",
        "command": ["nats-server"],
        "check_delay": 2,
        "description": "Message broker for streaming"
    },
    "train_publisher": {
        "name": "Training Data Publisher",
        "command": [sys.executable, "publisher.py"],
        "check_delay": 3,
        "description": "Streams FraudTrain.csv (labeled data)"
    },
    "detector": {
        "name": "Fraud Detector (Training)",
        "command": [sys.executable, "fraud_detector_modular.py"],
        "check_delay": 3,
        "description": "Trains model on labeled data"
    },
    "test_publisher": {
        "name": "Test Data Publisher",
        "command": [sys.executable, "test_publisher.py"],
        "check_delay": 3,
        "description": "Streams FraudTest.csv (unlabeled)"
    },
    "inference": {
        "name": "Inference Detector",
        "command": [sys.executable, "inference_detector.py"],
        "check_delay": 3,
        "description": "Makes predictions for dashboard review"
    },
    "dashboard": {
        "name": "Fraud Review Dashboard",
        "command": [sys.executable, "fraud_dashboard.py"],
        "check_delay": 3,
        "description": "Web interface for employees"
    },
    "feedback": {
        "name": "Feedback Trainer",
        "command": [sys.executable, "feedback_trainer_enhanced.py"],
        "check_delay": 3,
        "description": "Learns from employee corrections"
    },
    "report_gen": {
        "name": "Report Generator",
        "command": [sys.executable, "fraud_report_generator.py"],
        "check_delay": 3,
        "description": "Generates PDF reports (optional)"
    }
}

class SystemOrchestrator:
    def __init__(self):
        self.processes = {}
        
    def print_header(self):
        print("═" * 70)
        print("   FRAUD DETECTION SYSTEM - COMPLETE ORCHESTRATOR")
        print("   Feedback-Enhanced ML with Employee Review")
        print("═" * 70)
        print()
    
    def check_files(self):
        """Check if required files exist"""
        print("📋 Checking required files...")
        
        required = {
            "fraudTrain.csv": "Training data (labeled)",
            "fraudTest.csv": "Test data (can be labeled or unlabeled)",
            "fraud_detection_rules.json": "Detection rules configuration",
            "fraud_rules_loader.py": "Rules loader module"
        }
        
        missing = []
        for file, desc in required.items():
            if Path(file).exists():
                print(f"   ✓ {file} - {desc}")
            else:
                print(f"   ❌ {file} - {desc} [MISSING]")
                missing.append(file)
        
        print()
        
        if missing:
            print("⚠️  Missing required files:")
            for f in missing:
                print(f"   - {f}")
            print()
            response = input("Continue anyway? (y/n): ")
            if response.lower() != 'y':
                sys.exit(1)
    
    def start_component(self, key, config):
        """Start a single component"""
        print(f"🚀 Starting: {config['name']}")
        print(f"   {config['description']}")
        
        try:
            process = subprocess.Popen(
                config['command'],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True
            )
            
            self.processes[key] = process
            time.sleep(config['check_delay'])
            
            # Check if process is still running
            if process.poll() is None:
                print(f"   ✓ {config['name']} started (PID: {process.pid})")
            else:
                print(f"   ❌ {config['name']} failed to start")
                stdout, stderr = process.communicate()
                if stderr:
                    print(f"   Error: {stderr[:200]}")
            
            print()
            
        except FileNotFoundError:
            print(f"   ❌ Command not found: {' '.join(config['command'])}")
            print()
        except Exception as e:
            print(f"   ❌ Error: {e}")
            print()
    
    def run_full_system(self):
        """Start all components in order"""
        self.print_header()
        self.check_files()
        
        print("🎯 STARTING COMPLETE SYSTEM")
        print("─" * 70)
        print()
        
        # Start in order
        order = [
            "nats",
            "train_publisher",
            "detector",
            "test_publisher",
            "inference",
            "feedback",
            "dashboard",
            # "report_gen"  # Optional
        ]
        
        for component in order:
            if component in COMPONENTS:
                self.start_component(component, COMPONENTS[component])
                time.sleep(2)
        
        print("═" * 70)
        print("   ✅ SYSTEM STARTUP COMPLETE")
        print("═" * 70)
        print()
        print("📊 SYSTEM OVERVIEW:")
        print()
        print("  TRAINING PIPELINE:")
        print("    • Training data streaming from FraudTrain.csv")
        print("    • Model learning patterns and storing to shared storage")
        print()
        print("  INFERENCE PIPELINE:")
        print("    • Test data streaming from FraudTest.csv (unlabeled)")
        print("    • Model making predictions using trained weights")
        print("    • Flagged transactions sent to dashboard for review")
        print()
        print("  FEEDBACK LOOP:")
        print("    • Employees review flagged transactions on dashboard")
        print("    • Corrections sent back to feedback trainer")
        print("    • Model continuously improves from employee feedback")
        print()
        print("🌐 DASHBOARD: http://localhost:8000")
        print()
        print("Press Ctrl+C to stop all components")
        print()
        
        try:
            while True:
                time.sleep(5)
                # Check if any process died
                for key, process in list(self.processes.items()):
                    if process.poll() is not None:
                        print(f"⚠️  {COMPONENTS[key]['name']} stopped unexpectedly")
        
        except KeyboardInterrupt:
            print("\n")
            print("🛑 Shutting down system...")
            self.stop_all()
    
    def run_minimal(self):
        """Start minimal system for testing"""
        self.print_header()
        
        print("🎯 STARTING MINIMAL SYSTEM (Training + Dashboard Only)")
        print("─" * 70)
        print()
        
        minimal = ["nats", "train_publisher", "detector", "dashboard", "feedback"]
        
        for component in minimal:
            if component in COMPONENTS:
                self.start_component(component, COMPONENTS[component])
                time.sleep(2)
        
        print("✅ Minimal system running")
        print("🌐 Dashboard: http://localhost:8000")
        print()
        print("Press Ctrl+C to stop")
        
        try:
            while True:
                time.sleep(10)
        except KeyboardInterrupt:
            self.stop_all()
    
    def stop_all(self):
        """Stop all running processes"""
        print("\nStopping all components...")
        
        for key, process in self.processes.items():
            try:
                process.terminate()
                process.wait(timeout=5)
                print(f"   ✓ Stopped: {COMPONENTS[key]['name']}")
            except:
                process.kill()
                print(f"   ✓ Killed: {COMPONENTS[key]['name']}")
        
        print("\n✅ All components stopped")
        print()

def main():
    orchestrator = SystemOrchestrator()
    
    print()
    print("Select startup mode:")
    print("  1. Full System (Training + Inference + Dashboard + Feedback)")
    print("  2. Minimal System (Training + Dashboard only)")
    print("  3. Training Only (No dashboard)")
    print("  4. Dashboard Only (View existing results)")
    print()
    
    try:
        choice = input("Enter choice (1-4) [default: 1]: ").strip() or "1"
        
        if choice == "1":
            orchestrator.run_full_system()
        elif choice == "2":
            orchestrator.run_minimal()
        elif choice == "3":
            orchestrator.start_component("nats", COMPONENTS["nats"])
            time.sleep(2)
            orchestrator.start_component("train_publisher", COMPONENTS["train_publisher"])
            orchestrator.start_component("detector", COMPONENTS["detector"])
            print("\n✅ Training pipeline running")
            print("Press Ctrl+C to stop")
            try:
                while True:
                    time.sleep(10)
            except KeyboardInterrupt:
                orchestrator.stop_all()
        elif choice == "4":
            orchestrator.start_component("dashboard", COMPONENTS["dashboard"])
            print("\n✅ Dashboard running at http://localhost:8000")
            print("Press Ctrl+C to stop")
            try:
                while True:
                    time.sleep(10)
            except KeyboardInterrupt:
                orchestrator.stop_all()
        else:
            print("Invalid choice")
    
    except KeyboardInterrupt:
        orchestrator.stop_all()

if __name__ == "__main__":
    main()