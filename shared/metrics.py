"""
Prometheus Metrics Integration for Fraud Detection Pipeline
Provides metrics collection and HTTP server for Prometheus scraping
"""
import time
from prometheus_client import (
    Counter, Histogram, Gauge, Summary,
    CollectorRegistry, start_http_server, generate_latest
)
from contextlib import contextmanager
import threading

# Global registry for metrics
_registry = CollectorRegistry()
_metrics_manager = None
_http_server_started = False
_server_lock = threading.Lock()

# ============================================================================
# METRIC DEFINITIONS
# ============================================================================

# Transaction counters
transactions_total = Counter(
    'fraud_transactions_total',
    'Total number of transactions processed',
    ['component'],
    registry=_registry
)

# Fraud alert counters
alerts_total = Counter(
    'fraud_alerts_total',
    'Total number of fraud alerts generated',
    ['tier', 'component'],
    registry=_registry
)

alerts_by_pattern = Counter(
    'fraud_alerts_by_pattern',
    'Fraud alerts by pattern type',
    ['pattern', 'tier'],
    registry=_registry
)

# Risk score distribution
risk_score_histogram = Histogram(
    'fraud_risk_score',
    'Distribution of fraud risk scores',
    ['component'],
    buckets=[0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 95, 100],
    registry=_registry
)

# ML model metrics
ml_score_histogram = Histogram(
    'fraud_ml_score',
    'Distribution of ML fraud scores',
    buckets=[0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 95, 100],
    registry=_registry
)

ml_model_status = Gauge(
    'fraud_ml_model_status',
    'ML model availability status (1=available, 0=unavailable)',
    ['model_type'],
    registry=_registry
)

# Model training metrics
model_training_samples = Counter(
    'fraud_model_training_samples_total',
    'Total number of training samples processed',
    ['label'],
    registry=_registry
)

model_updates = Counter(
    'fraud_model_updates_total',
    'Total number of model updates',
    ['component'],
    registry=_registry
)

# Redis operations
redis_operation_duration = Histogram(
    'fraud_redis_operation_duration_seconds',
    'Duration of Redis operations',
    ['operation', 'entity_type'],
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
    registry=_registry
)

redis_operation_errors = Counter(
    'fraud_redis_operation_errors_total',
    'Total Redis operation errors',
    ['operation', 'entity_type'],
    registry=_registry
)

redis_entity_count = Gauge(
    'fraud_redis_entity_count',
    'Number of entities stored in Redis',
    ['entity_type'],
    registry=_registry
)

redis_connection_status = Gauge(
    'fraud_redis_connection_status',
    'Redis connection status (1=connected, 0=disconnected)',
    registry=_registry
)

# Report generation metrics
reports_generated = Counter(
    'fraud_reports_generated_total',
    'Total fraud reports generated',
    ['status', 'tier'],
    registry=_registry
)

report_generation_duration = Histogram(
    'fraud_report_generation_duration_seconds',
    'Time taken to generate reports',
    buckets=[0.5, 1, 2, 5, 10, 30, 60],
    registry=_registry
)

# Pipeline health metrics
component_uptime = Gauge(
    'fraud_pipeline_uptime_seconds',
    'Component uptime in seconds',
    ['component'],
    registry=_registry
)

component_errors = Counter(
    'fraud_pipeline_errors_total',
    'Total errors by component',
    ['component', 'error_type'],
    registry=_registry
)

# Processing latency
processing_duration = Histogram(
    'fraud_processing_duration_seconds',
    'Transaction processing duration',
    ['component', 'stage'],
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0],
    registry=_registry
)


# ============================================================================
# METRICS MANAGER CLASS
# ============================================================================

class MetricsManager:
    """Manages metrics collection and HTTP server"""
    
    def __init__(self, component_name: str, port: int = 8001):
        self.component_name = component_name
        self.port = port
        self.start_time = time.time()
        
    def start_http_server(self):
        """Start Prometheus HTTP server for metrics scraping"""
        global _http_server_started
        
        with _server_lock:
            if _http_server_started:
                print(f"   ℹ️  Metrics server already running on port {self.port}")
                return
            
            try:
                # Start HTTP server in separate thread
                start_http_server(self.port, registry=_registry)
                _http_server_started = True
                print(f"✅ Metrics server started on http://localhost:{self.port}/metrics")
                print(f"   Component: {self.component_name}")
            except OSError as e:
                if "Address already in use" in str(e):
                    print(f"⚠️  Port {self.port} already in use - metrics server not started")
                    print(f"   This is normal if multiple processes share the same port")
                else:
                    print(f"❌ Failed to start metrics server: {e}")
    
    def record_error(self, error_type: str):
        """Record an error"""
        component_errors.labels(
            component=self.component_name,
            error_type=error_type
        ).inc()
    
    def update_component_uptime(self):
        """Update component uptime metric"""
        uptime = time.time() - self.start_time
        component_uptime.labels(component=self.component_name).set(uptime)
    
    def get_metrics_text(self):
        """Get current metrics in Prometheus text format"""
        return generate_latest(_registry).decode('utf-8')


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

def initialize_metrics(component_name: str, port: int = 8001) -> MetricsManager:
    """
    Initialize metrics for a component and start HTTP server
    
    Args:
        component_name: Name of the component (detector, stats_updater, etc.)
        port: Port for Prometheus HTTP server
    
    Returns:
        MetricsManager instance
    """
    global _metrics_manager
    
    _metrics_manager = MetricsManager(component_name, port)
    _metrics_manager.start_http_server()
    
    # Initialize component uptime
    component_uptime.labels(component=component_name).set(0)
    
    return _metrics_manager


def get_metrics_manager() -> MetricsManager:
    """Get the global metrics manager instance"""
    return _metrics_manager


# ============================================================================
# METRIC RECORDING FUNCTIONS
# ============================================================================

def record_transaction(component: str):
    """Record a processed transaction"""
    transactions_total.labels(component=component).inc()


def record_fraud_alert(tier: int, pattern: str, risk_score: float, component: str = "detector"):
    """Record a fraud alert"""
    tier_label = f"tier{tier}"
    
    # Increment alert counters
    alerts_total.labels(tier=tier_label, component=component).inc()
    alerts_by_pattern.labels(pattern=pattern, tier=tier_label).inc()
    
    # Record risk score
    risk_score_histogram.labels(component=component).observe(risk_score)


def record_ml_score(score: float, model_available: bool):
    """Record ML model score and availability"""
    ml_score_histogram.observe(score)
    
    # Update model status
    ml_model_status.labels(model_type="main").set(1 if model_available else 0)
    ml_model_status.labels(model_type="validator").set(1 if model_available else 0)


def record_model_training(label: str, component: str = "feedback"):
    """Record model training sample"""
    model_training_samples.labels(label=label).inc()


def record_model_update(component: str = "feedback"):
    """Record model update/save"""
    model_updates.labels(component=component).inc()


def record_redis_operation(operation: str, entity_type: str, duration: float, success: bool = True):
    """Record Redis operation metrics"""
    redis_operation_duration.labels(
        operation=operation,
        entity_type=entity_type
    ).observe(duration)
    
    if not success:
        redis_operation_errors.labels(
            operation=operation,
            entity_type=entity_type
        ).inc()


def update_redis_entity_counts(customers: int, merchants: int, categories: int):
    """Update Redis entity count metrics"""
    redis_entity_count.labels(entity_type="customer").set(customers)
    redis_entity_count.labels(entity_type="merchant").set(merchants)
    redis_entity_count.labels(entity_type="category").set(categories)


def set_redis_connection_status(connected: bool):
    """Update Redis connection status"""
    redis_connection_status.set(1 if connected else 0)


def record_report_generation(status: str, tier: int, duration: float):
    """Record report generation metrics"""
    tier_label = f"tier{tier}"
    reports_generated.labels(status=status, tier=tier_label).inc()
    report_generation_duration.observe(duration)


def record_processing_duration(component: str, stage: str, duration: float):
    """Record processing duration for a specific stage"""
    processing_duration.labels(component=component, stage=stage).observe(duration)


# ============================================================================
# CONTEXT MANAGER FOR TIMING
# ============================================================================

@contextmanager
def MetricsTimer(component: str, stage: str):
    """
    Context manager for timing operations
    
    Usage:
        with MetricsTimer("detector", "inference"):
            # ... code to time ...
    """
    start = time.time()
    try:
        yield
    finally:
        duration = time.time() - start
        record_processing_duration(component, stage, duration)


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def get_current_metrics():
    """Get current metrics as text (for debugging)"""
    return generate_latest(_registry).decode('utf-8')


def reset_metrics():
    """Reset all metrics (for testing)"""
    global _registry, _metrics_manager, _http_server_started
    _registry = CollectorRegistry()
    _metrics_manager = None
    _http_server_started = False


# ============================================================================
# HEALTH CHECK
# ============================================================================

def metrics_health_check():
    """Check if metrics system is healthy"""
    try:
        metrics_text = get_current_metrics()
        return len(metrics_text) > 0
    except Exception as e:
        print(f"❌ Metrics health check failed: {e}")
        return False


if __name__ == "__main__":
    # Test metrics server
    print("Testing Prometheus metrics server...")
    manager = initialize_metrics("test_component", port=8001)
    
    # Record some test metrics
    record_transaction("test")
    record_fraud_alert(1, "TEST_PATTERN", 85.5, "test")
    record_ml_score(75.0, True)
    
    print("\n📊 Current metrics:")
    print(get_current_metrics()[:500])
    print("\n✅ Metrics server running. Press Ctrl+C to stop.")
    print(f"   Visit: http://localhost:8001/metrics")
    
    try:
        # Keep server running
        while True:
            time.sleep(1)
            manager.update_component_uptime()
    except KeyboardInterrupt:
        print("\n\n👋 Shutting down...")