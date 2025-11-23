"""
Shared configuration constants for the Fraud Detection Pipeline
"""

from pathlib import Path
import pathway as pw

# ============================================================================
# NATS CONFIGURATION
# ============================================================================

NATS_URI = "nats://localhost:4222"
NATS_INPUT_TOPIC = "fraud.transactions"
NATS_ALERTS_TOPIC = "fraud.alerts"
NATS_RESULTS_TOPIC = "fraud.results"
NATS_REPORTS_TOPIC = "fraud.reports"

# ============================================================================
# PERSISTENCE CONFIGURATION
# ============================================================================

PERSISTENCE_DIR = Path("pathway_persistence")
PERSISTENCE_DIR.mkdir(exist_ok=True)

CHECKPOINT_CONFIG = pw.persistence.Config.simple_config(
    pw.persistence.Backend.filesystem(str(PERSISTENCE_DIR / "checkpoints")),
    snapshot_interval_ms=10000
)

# ============================================================================
# PUBLISHER CONFIGURATION
# ============================================================================

TARGET_TPS = 1000  # Transactions per second (realistic bank load)
BATCH_SIZE = 1000  # Process in batches
ORIGINAL_DATA_FILE = "fraudTrain.csv"
AUTOCOMMIT_DURATION_MS = 100
PUBLISHER_STREAM_FILE = "fraud_stream.csv"
PUBLISHER_DETECTOR_TEMP_FILE = "./publisher/temp_det_stream.csv"
PUBLISHER_FEEDBACK_TEMP_FILE = "./publisher/temp_feed_stream.csv"

# ============================================================================
# REPORT GENERATION CONFIGURATION
# ============================================================================

REPORTS_DIR = "fraud_reports"
PDF_AVAILABLE = False  # Will be set dynamically based on reportlab availability

# ============================================================================
# DETECTOR CONFIGURATION
# ============================================================================

# ML Model parameters
ML_MODEL_GRACE_PERIOD_MAIN = 200
ML_MODEL_DELTA_MAIN = 0.00001
ML_MODEL_SEED_MAIN = 42

ML_MODEL_GRACE_PERIOD_VALIDATOR = 150
ML_MODEL_DELTA_VALIDATOR = 0.0001
ML_MODEL_SEED_VALIDATOR = 123

# Persistence for detector
DETECTOR_PERSIST_DIR = Path("pipeline/pathway_persistence")
DETECTOR_CHECKPOINT_CONFIG = pw.persistence.Config.simple_config(
    pw.persistence.Backend.filesystem(str(DETECTOR_PERSIST_DIR / "checkpoints_detector")),
    snapshot_interval_ms=10000
)

# Detection thresholds
FRAUD_HISTORY_THRESHOLD = 3
Z_AMT_THRESHOLD = 3.5
AMT_MIN_FOR_HUGE = 500
ML_SCORE_HIGH = 80
ML_SCORE_MEDIUM = 82
ML_SCORE_LOW = 75
CONFIDENCE_HIGH = 90
CONFIDENCE_MEDIUM = 75
CONFIDENCE_LOW = 70

# Additional detector constants
ONLINE_CATEGORIES = ['shopping_net', 'misc_net', 'grocery_net']
LATE_NIGHT_START = 1
LATE_NIGHT_END = 5
MODEL_SAVE_INTERVAL = 60
PROCESSED_MAX_SIZE = 100000
MIN_TRAINING_TRANSACTIONS = 10
PROGRESS_LOG_INTERVAL = 10000
MAX_TXN_COUNT = 1000
ML_AGREEMENT_THRESHOLD = 20
ALERT_LOG_INTERVAL = 50
MIN_MERCH_TOTAL_FOR_RATE = 30
MIN_CAT_TOTAL_FOR_RATE = 100

# Confidence levels for tiers
CONFIDENCE_TIER1_EXTREME = 95
CONFIDENCE_TIER1_HIGH = 90
CONFIDENCE_TIER2 = 80
CONFIDENCE_TIER3 = 75

# Extreme signals thresholds
EXTREME_Z_AMT = 4.5
HUGE_Z_AMT = 3.8
EXTREME_Z_DIST = 4
VERY_FAR_Z_DIST = 3.2
VERY_FAR_DIST = 100
HIGH_MERCH_FRAUD_RATE = 0.4
MIN_MERCH_TOTAL = 50
FRAUD_HISTORY_EXTREME = 3
CONFIDENCE_EXTREME = 95

# Tier 2 thresholds
TIER2_Z_AMT_HIGH = 3.5
TIER2_Z_AMT_MEDIUM = 3
TIER2_Z_DIST_HIGH = 3.5
TIER2_Z_DIST_MEDIUM = 3
TIER2_MERCH_FRAUD_RATE = 0.3
TIER2_MIN_MERCH_TOTAL = 40
TIER2_LATE_ONLINE_AMT = 400
TIER2_FRAUD_HISTORY = 2
TIER2_Z_COMBO = 2.5
TIER2_ML_SCORE_HIGH = 80
TIER2_ML_SCORE_MEDIUM = 70
TIER2_THRESHOLD = 75

# Tier 3 thresholds
TIER3_ML_SCORE = 82
TIER3_SUPPORT_COUNT = 2
TIER3_Z_THRESHOLD = 2.5
TIER3_MERCH_FRAUD_THRESHOLD = 0.15
TIER3_CAT_FRAUD_THRESHOLD = 0.1
TIER3_FRAUD_HISTORY_MIN = 1

# ============================================================================
# OTHER CONSTANTS
# ============================================================================

# File paths
PATHWAY_STREAMS_DIR = Path("pathway_streams")
PATHWAY_STREAMS_DIR.mkdir(exist_ok=True)

ALL_RESULTS_FILE = PATHWAY_STREAMS_DIR / "all_results.jsonl"
FRAUD_ALERTS_FILE = PATHWAY_STREAMS_DIR / "fraud_alerts.jsonl"

# ============================================================================
# FEEDBACK CONFIGURATION
# ============================================================================

FEEDBACK_TOPIC = "fraud.feedback"

FEEDBACK_CHECKPOINT_CONFIG = pw.persistence.Config.simple_config(
    pw.persistence.Backend.filesystem(str(DETECTOR_PERSIST_DIR / "checkpoints_feedback")),
    snapshot_interval_ms=10000
)