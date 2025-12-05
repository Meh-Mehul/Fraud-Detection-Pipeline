# shared/metrics.py
"""
Enhanced Prometheus metrics for fraud detection pipeline
Tracks: latency, model performance, alerts, throughput
"""

from prometheus_client import (
    Counter, Histogram, Gauge, Summary,
    start_http_server, REGISTRY
)
import time
from typing import Optional
from collections import deque
from datetime import datetime


# ============================================================================
# METRIC DEFINITIONS
# ============================================================================

# Transaction Processing
transactions_total = Counter(
    'fraud_transactions_total',
    'Total transactions processed',
    ['component']
)

# Latency Metrics
latency_seconds = Histogram(
    'fraud_latency_seconds',
    'Processing latency in seconds',
    ['stage'],
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]
)

# Alert Metrics
alerts_total = Counter(
    'fraud_alerts_total',
    'Total fraud alerts generated',
    ['tier', 'pattern']
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

# Model Update Tracking
model_updates_total = Counter(
    'fraud_model_updates_total',
    'Total model updates/saves'
)

model_training_samples = Counter(
    'fraud_model_training_samples_total',
    'Training samples processed',
    ['label']  # fraud or legitimate
)

model_last_update_timestamp = Gauge(
    'fraud_model_last_update_timestamp',
    'Unix timestamp of last model update'
)

# Model Status
model_status = Gauge(
    'fraud_ml_model_status',
    'ML model availability (1=available, 0=unavailable)',
    ['model_type']
)

# Redis Operations
redis_operation_duration = Histogram(
    'fraud_redis_operation_duration_seconds',
    'Redis operation latency',
    ['operation', 'entity_type'],
    buckets=[0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1]
)

redis_entity_count = Gauge(
    'fraud_redis_entity_count',
    'Number of entities in Redis',
    ['entity_type']
)

redis_connection_status = Gauge(
    'fraud_redis_connection_status',
    'Redis connection status (1=connected, 0=disconnected)'
)

# Component Health
component_uptime_seconds = Gauge(
    'fraud_pipeline_uptime_seconds',
    'Component uptime in seconds',
    ['component']
)

component_errors_total = Counter(
    'fraud_component_errors_total',
    'Total errors by component',
    ['component', 'error_type']
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
    
    def update_component_uptime(self):
        """Update component uptime metric"""
        uptime = time.time() - self.start_time
        component_uptime_seconds.labels(component=self.component_name).set(uptime)
    
    def record_error(self, component: str, error_type: str):
        """Record an error"""
        component_errors_total.labels(
            component=component,
            error_type=error_type
        ).inc()
    
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


# Transaction tracking
def record_transaction(component: str):
    """Record a processed transaction"""
    transactions_total.labels(component=component).inc()


# Latency tracking
def record_latency(stage: str, duration: float):
    """Record latency for a processing stage"""
    latency_seconds.labels(stage=stage).observe(duration)


# Alert tracking
def record_fraud_alert(tier: str, pattern: str, risk_score: float, component: str):
    """Record a fraud alert"""
    alerts_total.labels(tier=tier, pattern=pattern).inc()


# Model tracking
def record_model_update():
    """Record a model save/update"""
    model_updates_total.inc()
    model_last_update_timestamp.set(time.time())


def record_training_sample(label: str):
    """Record a training sample (fraud or legitimate)"""
    model_training_samples.labels(label=label).inc()


def set_model_status(model_type: str, available: bool):
    """Set model availability status"""
    model_status.labels(model_type=model_type).set(1 if available else 0)


# Redis tracking
def record_redis_operation(operation: str, entity_type: str, duration: float, success: bool = True):
    """Record a Redis operation"""
    if success:
        redis_operation_duration.labels(
            operation=operation,
            entity_type=entity_type
        ).observe(duration)


def update_redis_entity_counts(customers: int, merchants: int, categories: int):
    """Update Redis entity counts"""
    redis_entity_count.labels(entity_type='customer').set(customers)
    redis_entity_count.labels(entity_type='merchant').set(merchants)
    redis_entity_count.labels(entity_type='category').set(categories)


def set_redis_connection_status(connected: bool):
    """Set Redis connection status"""
    redis_connection_status.set(1 if connected else 0)


# Context manager for timing
class MetricsTimer:
    """Context manager for timing operations"""
    
    def __init__(self, stage: str):
        self.stage = stage
        self.start_time = None
    
    def __enter__(self):
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = time.time() - self.start_time
        record_latency(self.stage, duration)