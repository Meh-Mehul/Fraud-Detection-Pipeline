"""
Unified Fraud Detection Pipeline Launcher
Runs all components in a single command with proper process management
"""

import subprocess
import sys
import time
import signal
import os
from pathlib import Path

# Pipeline components to run
COMPONENTS = [
    ("Detector", "python run_detector.py"),
    ("Report Generator", "python run_report.py"),
    ("Stats Updater", "python run_stats_updater.py"),
    ("Feedback Handler", "python run_feedback.py"),
    ("Publisher", "python publisher/pub_common.py"),
]

class PipelineManager:
    def __init__(self):
        self.processes = {}
        self.running = True
    
    def start_component(self, name, command):
        """Start a pipeline component in subprocess"""
        try:
            print(f"Starting {name}...")
            process = subprocess.Popen(
                command,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1
            )
            self.processes[name] = process
            print(f"{name} started (PID: {process.pid})")
        except Exception as e:
            print(f"Failed to start {name}: {e}")
    
    def monitor_processes(self):
        """Monitor all processes for failures"""
        while self.running:
            for name, process in list(self.processes.items()):
                if process.poll() is not None:  # Process ended
                    print(f"{name} crashed (exit code: {process.returncode})")
                    if self.running:
                        print(f"Restarting {name}...")
                        self.start_component(name, self._get_command(name))
            time.sleep(5)
    
    def _get_command(self, name):
        """Get command for component by name"""
        for comp_name, cmd in COMPONENTS:
            if comp_name == name:
                return cmd
        return ""
    
    def shutdown(self, signum=None, frame=None):
        """Gracefully shutdown all processes"""
        print("Pipeline shutdown - Stopping all components...")
        
        self.running = False
        
        # Terminate all processes
        for name, process in self.processes.items():
            if process.poll() is None:  # Still running
                print(f"Stopping {name} (PID: {process.pid})...")
                process.terminate()
        
        # Wait for graceful shutdown
        time.sleep(2)
        
        # Force kill if needed
        for name, process in self.processes.items():
            if process.poll() is None:
                print(f"Force killing {name}...")
                process.kill()
        
        print("Pipeline shutdown complete")
        sys.exit(0)
    
    def run(self):
        """Start all components"""
        # Setup signal handlers
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)
        
        # Start all components
        print("Starting pipeline components...")
        for name, command in COMPONENTS:
            self.start_component(name, command)
            time.sleep(1)  # Stagger startup
        
        print("All components started")
        print("Press Ctrl+C to shutdown")
        
        # Monitor processes
        try:
            self.monitor_processes()
        except KeyboardInterrupt:
            self.shutdown()


if __name__ == "__main__":
    manager = PipelineManager()
    manager.run()