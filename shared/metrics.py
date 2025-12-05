# shared/metrics.py
"""
Streamlined Prometheus metrics for fraud detection pipeline
Tracks: pipeline latency, fraud alerts, model updates, model weight delta
"""

from prometheus_client import (
    Counter, Histogram, Gauge,
    start_http_server, REGISTRY
)
import time
from typing import Optional
from collections import deque


# ============================================================================
# METRIC DEFINITIONS
# ============================================================================

# Pipeline End-to-End Latency (Publisher→Detector, Detector→Report)
pipeline_latency_seconds = Histogram(
    'fraud_pipeline_latency_seconds',
    'End-to-end pipeline latency in seconds',
    ['stage'],  # publisher_to_detector, detector_to_report
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
)

# Internal Processing Latency (for debugging)
latency_seconds = Histogram(
    'fraud_latency_seconds',
    'Internal processing latency in seconds',
    ['stage'],  # detector_total, ml_inference, rules_evaluation
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]
)

# Alert Metrics
alerts_total = Counter(
    'fraud_alerts_total',
    'Total fraud alerts generated',
    ['tier', 'pattern']
)

# Model Update Tracking
model_updates_total = Counter(
    'fraud_model_updates_total',
    'Total model updates/saves'
)

model_last_update_timestamp = Gauge(
    'fraud_model_last_update_timestamp',
    'Unix timestamp of last model update'
)

# Model Weight Change Delta
model_weight_delta = Gauge(
    'fraud_model_weight_delta',
    'Magnitude of model weight changes (0-1 normalized)'
)

# Model Performance Metrics (from feedback)
model_f1_score = Gauge(
    'fraud_model_f1_score',
    'Model F1 score (sliding window)'
)

model_precision = Gauge(
    'fraud_model_precision',
    'Model precision (sliding window)'
)

model_recall = Gauge(
    'fraud_model_recall',
    'Model recall (sliding window)'
)

model_accuracy = Gauge(
    'fraud_model_accuracy',
    'Model accuracy (sliding window)'
)

model_training_samples = Counter(
    'fraud_model_training_samples_total',
    'Training samples processed',
    ['label']  # fraud or legitimate
)


# ============================================================================
# PERFORMANCE CALCULATOR (for F1, Precision, Recall)
# ============================================================================

class PerformanceCalculator:
    """
    Calculates model performance metrics using a sliding window
    Tracks predictions vs ground truth from feedback loop
    """
    
    def __init__(self, window_size: int = 1000):
        self.window_size = window_size
        self.predictions = deque(maxlen=window_size)
        self.ground_truth = deque(maxlen=window_size)
        
        # Counters for current window
        self.tp = 0  # True Positives
        self.fp = 0  # False Positives
        self.tn = 0  # True Negatives
        self.fn = 0  # False Negatives
        
        self.last_update = time.time()
    
    def add_sample(self, prediction: int, actual: int):
        """
        Add a prediction-actual pair
        prediction: 1 if alert, 0 if no alert
        actual: 1 if fraud, 0 if legitimate
        """
        # If window is full, remove oldest sample's contribution
        if len(self.predictions) == self.window_size:
            old_pred = self.predictions[0]
            old_actual = self.ground_truth[0]
            self._remove_sample(old_pred, old_actual)
        
        # Add new sample
        self.predictions.append(prediction)
        self.ground_truth.append(actual)
        self._add_sample(prediction, actual)
        
        # Update metrics every 10 samples
        if len(self.predictions) % 10 == 0:
            self.update_metrics()
    
    def _add_sample(self, pred: int, actual: int):
        """Update confusion matrix counters"""
        if pred == 1 and actual == 1:
            self.tp += 1
        elif pred == 1 and actual == 0:
            self.fp += 1
        elif pred == 0 and actual == 1:
            self.fn += 1
        elif pred == 0 and actual == 0:
            self.tn += 1
    
    def _remove_sample(self, pred: int, actual: int):
        """Remove oldest sample from confusion matrix"""
        if pred == 1 and actual == 1:
            self.tp -= 1
        elif pred == 1 and actual == 0:
            self.fp -= 1
        elif pred == 0 and actual == 1:
            self.fn -= 1
        elif pred == 0 and actual == 0:
            self.tn -= 1
    
    def update_metrics(self):
        """Calculate and update Prometheus metrics"""
        # Precision = TP / (TP + FP)
        precision = self.tp / (self.tp + self.fp) if (self.tp + self.fp) > 0 else 0.0
        
        # Recall = TP / (TP + FN)
        recall = self.tp / (self.tp + self.fn) if (self.tp + self.fn) > 0 else 0.0
        
        # F1 = 2 * (Precision * Recall) / (Precision + Recall)
        f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0.0
        
        # Accuracy = (TP + TN) / Total
        total = self.tp + self.fp + self.tn + self.fn
        accuracy = (self.tp + self.tn) / total if total > 0 else 0.0
        
        # Update Prometheus gauges
        model_precision.set(precision)
        model_recall.set(recall)
        model_f1_score.set(f1)
        model_accuracy.set(accuracy)
        
        self.last_update = time.time()
    
    def get_stats(self) -> dict:
        """Get current performance statistics"""
        return {
            'tp': self.tp,
            'fp': self.fp,
            'tn': self.tn,
            'fn': self.fn,
            'window_size': len(self.predictions),
            'precision': model_precision._value.get(),
            'recall': model_recall._value.get(),
            'f1': model_f1_score._value.get(),
            'accuracy': model_accuracy._value.get()
        }


# ============================================================================
# METRICS MANAGER
# ============================================================================

class MetricsManager:
    """Central metrics management"""
    
    def __init__(self, component_name: str, port: int):
        self.component_name = component_name
        self.port = port
        self.start_time = time.time()
        self.performance_calc = PerformanceCalculator(window_size=1000)
        
        # Start Prometheus HTTP server
        try:
            start_http_server(port)
            print(f"📊 Metrics server started on port {port}")
        except OSError:
            print(f"⚠️  Port {port} already in use - metrics endpoint already running")
    
    def add_performance_sample(self, prediction: int, actual: int):
        """Add a prediction-actual pair for performance calculation"""
        self.performance_calc.add_sample(prediction, actual)
    
    def get_performance_stats(self) -> dict:
        """Get current performance statistics"""
        return self.performance_calc.get_stats()


# Global metrics manager instance
_metrics_manager: Optional[MetricsManager] = None


# ============================================================================
# PUBLIC API
# ============================================================================

def initialize_metrics(component_name: str, port: int) -> MetricsManager:
    """Initialize metrics for a component"""
    global _metrics_manager
    _metrics_manager = MetricsManager(component_name, port)
    return _metrics_manager


def get_metrics_manager() -> Optional[MetricsManager]:
    """Get the global metrics manager"""
    return _metrics_manager


# Pipeline latency tracking (end-to-end)
def record_pipeline_latency(stage: str, duration: float):
    """Record end-to-end pipeline latency (publisher_to_detector, detector_to_report)"""
    pipeline_latency_seconds.labels(stage=stage).observe(duration)


# Internal latency tracking
def record_latency(stage: str, duration: float):
    """Record latency for an internal processing stage"""
    latency_seconds.labels(stage=stage).observe(duration)


# Alert tracking
def record_fraud_alert(tier, pattern: str, risk_score: float, component: str):
    """Record a fraud alert"""
    alerts_total.labels(tier=str(tier), pattern=pattern).inc()


# Model tracking
def record_model_update():
    """Record a model save/update"""
    model_updates_total.inc()
    model_last_update_timestamp.set(time.time())


def record_training_sample(label: str):
    """Record a training sample (fraud or legitimate)"""
    model_training_samples.labels(label=label).inc()


def set_model_weight_delta(delta: float):
    """Set the model weight change delta (0-1 normalized)"""
    model_weight_delta.set(delta)


# Context manager for timing
class MetricsTimer:
    """Context manager for timing operations"""
    
    def __init__(self, stage: str, pipeline: bool = False):
        self.stage = stage
        self.pipeline = pipeline
        self.start_time = None
    
    def __enter__(self):
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = time.time() - self.start_time
        if self.pipeline:
            record_pipeline_latency(self.stage, duration)
        else:
            record_latency(self.stage, duration)


# Utility function to get current timestamp in ms
def get_timestamp_ms() -> int:
    """Get current Unix timestamp in milliseconds"""
    return int(time.time() * 1000)