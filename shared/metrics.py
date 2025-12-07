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
import math
import json
from pathlib import Path
from typing import Optional, List, Tuple
from collections import deque

# Baseline metrics file path
BASELINE_METRICS_PATH = Path("./pathway_persistence/baseline_metrics.json")


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

# 1-Minute Weighted Moving Averages (recent values get more weight)
model_f1_score_weighted_avg = Gauge(
    'fraud_model_f1_score_weighted_avg',
    'Model F1 score (1-min weighted moving avg, recent gets more weight)'
)

model_latency_weighted_avg = Gauge(
    'fraud_latency_weighted_avg_seconds',
    'Processing latency (1-min weighted moving avg, recent gets more weight)'
)

pipeline_latency_weighted_avg = Gauge(
    'fraud_pipeline_latency_weighted_avg_seconds',
    'Pipeline latency (1-min weighted moving avg, recent gets more weight)'
)


# ============================================================================
# WEIGHTED MOVING AVERAGE (1-minute window, exponential decay)
# ============================================================================

class WeightedMovingAverage:
    """
    Calculates 1-minute weighted moving average with exponential decay.
    Recent values get more weight (half-life of 15 seconds).
    """
    
    def __init__(self, window_seconds: float = 60.0, half_life_seconds: float = 15.0):
        self.window_seconds = window_seconds
        self.half_life_seconds = half_life_seconds
        # decay_rate = ln(2) / half_life, so weight = exp(-decay_rate * age)
        self.decay_rate = math.log(2) / half_life_seconds
        # Store (timestamp, value) pairs
        self.samples: List[Tuple[float, float]] = []
    
    def add_sample(self, value: float, timestamp: float = None):
        """Add a new sample with current or provided timestamp"""
        if timestamp is None:
            timestamp = time.time()
        self.samples.append((timestamp, value))
        # Prune old samples outside the window
        self._prune_old_samples(timestamp)
    
    def _prune_old_samples(self, current_time: float):
        """Remove samples older than the window"""
        cutoff = current_time - self.window_seconds
        self.samples = [(t, v) for t, v in self.samples if t >= cutoff]
    
    def get_weighted_average(self) -> float:
        """
        Calculate weighted average with exponential decay.
        Weight = exp(-decay_rate * age_in_seconds)
        More recent samples have higher weights.
        """
        if not self.samples:
            return 0.0
        
        current_time = time.time()
        self._prune_old_samples(current_time)
        
        if not self.samples:
            return 0.0
        
        weighted_sum = 0.0
        total_weight = 0.0
        
        for timestamp, value in self.samples:
            age = current_time - timestamp
            # Exponential decay: weight = e^(-decay_rate * age)
            weight = math.exp(-self.decay_rate * age)
            weighted_sum += weight * value
            total_weight += weight
        
        return weighted_sum / total_weight if total_weight > 0 else 0.0
    
    def sample_count(self) -> int:
        """Get number of samples in the window"""
        current_time = time.time()
        self._prune_old_samples(current_time)
        return len(self.samples)
    
    def bootstrap(self, value: float, num_samples: int = 10):
        """
        Bootstrap the moving average with initial samples.
        Spreads samples across the window to provide stable initial value.
        """
        current_time = time.time()
        # Add samples spread across the last 30 seconds
        for i in range(num_samples):
            # Spread samples from 30 seconds ago to now
            age = 30.0 * (num_samples - i) / num_samples
            self.add_sample(value, current_time - age)


# Singleton instances for weighted moving averages
_f1_wma = WeightedMovingAverage(window_seconds=60.0, half_life_seconds=15.0)
_latency_wma = WeightedMovingAverage(window_seconds=60.0, half_life_seconds=15.0)
_pipeline_latency_wma = WeightedMovingAverage(window_seconds=60.0, half_life_seconds=15.0)


def load_baseline_metrics() -> Optional[dict]:
    """
    Load baseline metrics from pretrain and bootstrap the F1 weighted moving average.
    Call this at startup to avoid slow F1 warm-up.
    Returns the loaded metrics or None if file doesn't exist.
    """
    global _f1_wma
    
    if not BASELINE_METRICS_PATH.exists():
        print(f"[WARN]  No baseline metrics found at {BASELINE_METRICS_PATH}")
        return None
    
    try:
        with open(BASELINE_METRICS_PATH, 'r') as f:
            baseline = json.load(f)
        
        f1_value = baseline.get('f1', 0.0)
        precision_value = baseline.get('precision', 0.0)
        recall_value = baseline.get('recall', 0.0)
        
        # Bootstrap the F1 weighted moving average
        _f1_wma.bootstrap(f1_value, num_samples=15)
        
        # Also set the gauges immediately
        model_f1_score.set(f1_value)
        model_f1_score_weighted_avg.set(f1_value)
        model_precision.set(precision_value)
        model_recall.set(recall_value)
        
        # Bootstrap the PerformanceCalculator with confusion matrix if available
        if _metrics_manager is not None:
            tp = baseline.get('tp', 0)
            fp = baseline.get('fp', 0)
            tn = baseline.get('tn', 0)
            fn = baseline.get('fn', 0)
            if tp + fp + tn + fn > 0:
                _metrics_manager.performance_calc.bootstrap(tp, fp, tn, fn)
        
        print(f"[OK] Loaded baseline metrics from pretrain:")
        print(f"  F1: {f1_value*100:.1f}%, Precision: {precision_value*100:.1f}%, Recall: {recall_value*100:.1f}%")
        
        return baseline
        
    except Exception as e:
        print(f"[WARN]  Error loading baseline metrics: {e}")
        return None


def record_f1_weighted_sample(f1_value: float):
    """Record an F1 score sample and update the weighted moving average gauge"""
    _f1_wma.add_sample(f1_value)
    model_f1_score_weighted_avg.set(_f1_wma.get_weighted_average())


def record_latency_weighted_sample(latency_seconds: float):
    """Record an internal latency sample and update the weighted moving average gauge"""
    _latency_wma.add_sample(latency_seconds)
    model_latency_weighted_avg.set(_latency_wma.get_weighted_average())


def record_pipeline_latency_weighted_sample(latency_seconds: float):
    """Record a pipeline latency sample and update the weighted moving average gauge"""
    _pipeline_latency_wma.add_sample(latency_seconds)
    pipeline_latency_weighted_avg.set(_pipeline_latency_wma.get_weighted_average())


def get_weighted_averages() -> dict:
    """Get current weighted moving averages"""
    return {
        'f1_weighted_avg': _f1_wma.get_weighted_average(),
        'latency_weighted_avg': _latency_wma.get_weighted_average(),
        'pipeline_latency_weighted_avg': _pipeline_latency_wma.get_weighted_average(),
        'f1_sample_count': _f1_wma.sample_count(),
        'latency_sample_count': _latency_wma.sample_count(),
        'pipeline_latency_sample_count': _pipeline_latency_wma.sample_count()
    }


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
    
    def bootstrap(self, tp: int, fp: int, tn: int, fn: int):
        """
        Bootstrap the calculator with baseline confusion matrix values.
        This allows F1 to show immediately on restart without waiting for samples.
        """
        self.tp = tp
        self.fp = fp
        self.tn = tn
        self.fn = fn
        # Immediately update metrics to set the gauges
        self.update_metrics()
        print(f"  [INFO] Bootstrapped with TP={tp}, FP={fp}, TN={tn}, FN={fn}")
    
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
        
        # Also update weighted moving average for F1
        record_f1_weighted_sample(f1)
        
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
            print(f"[INFO] Metrics server started on port {port}")
        except OSError:
            print(f"[WARN]  Port {port} already in use - metrics endpoint already running")
    
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
    # Also update weighted moving average
    record_pipeline_latency_weighted_sample(duration)


# Internal latency tracking
def record_latency(stage: str, duration: float):
    """Record latency for an internal processing stage"""
    latency_seconds.labels(stage=stage).observe(duration)
    # NOTE: Internal latencies are NOT added to weighted avg (different scale)


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