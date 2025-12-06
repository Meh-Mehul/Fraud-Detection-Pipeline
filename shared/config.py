"""
Centralized configuration for all fraud detection pipelines
"""
import pathway as pw
from pathlib import Path

# ============================================================================
# NATS Configuration
# ============================================================================
NATS_URI = "nats://localhost:4222"

# Credit Card Fraud Topics (Existing)
CC_FRAUD_TRANSACTIONS_TOPIC = "fraud.transactions"
CC_FRAUD_FEEDBACK_TOPIC = "fraud.feedback"
CC_FRAUD_RESULTS_TOPIC = "fraud.results"
CC_FRAUD_ALERTS_TOPIC = "fraud.alerts"
CC_FRAUD_REPORTS_TOPIC = "fraud.reports"

# ATO Fraud Topics (New)
ATO_LOGIN_ATTEMPTS_TOPIC = "ato.login_attempts"
ATO_USER_PROFILES_TOPIC = "ato.user_profiles"
ATO_FRAUD_ALERTS_TOPIC = "ato.fraud_alerts"
ATO_MANUAL_REVIEW_TOPIC = "ato.manual_review"
ATO_APPROVED_LOGINS_TOPIC = "ato.approved_logins"
ATO_FEEDBACK_LOOP_TOPIC = "ato.feedback_loop"

# ============================================================================
# Persistence Configuration
# ============================================================================
PERSISTENCE_DIR = Path("pathway_persistence")
PERSISTENCE_DIR.mkdir(exist_ok=True)

# Credit Card Fraud Checkpoints
CC_CHECKPOINT_CONFIG = pw.persistence.Config.simple_config(
    pw.persistence.Backend.filesystem(str(PERSISTENCE_DIR / "cc_fraud_checkpoints")),
    snapshot_interval_ms=10000
)

# ATO Checkpoints
ATO_CHECKPOINT_CONFIG = pw.persistence.Config.simple_config(
    pw.persistence.Backend.filesystem(str(PERSISTENCE_DIR / "ato_checkpoints")),
    snapshot_interval_ms=10000
)

# ============================================================================
# Metrics Ports
# ============================================================================
METRICS_CC_DETECTOR = 8001
METRICS_CC_STATS_UPDATER = 8002
METRICS_CC_FEEDBACK = 8003
METRICS_CC_REPORT = 8004

# New ATO Metrics Ports
METRICS_ATO_DETECTOR = 8005
METRICS_ATO_ENRICHMENT = 8006
METRICS_ATO_FEEDBACK = 8007

# ============================================================================
# Processing Configuration
# ============================================================================
AUTOCOMMIT_DURATION_MS = 1000  # Pathway autocommit interval