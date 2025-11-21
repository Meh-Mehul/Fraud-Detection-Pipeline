"""
Enhanced Feedback Trainer
Learns from employee corrections on the dashboard
Trains shared model with CORRECTED labels
"""

import pathway as pw
import json
import pickle
from datetime import datetime
from pathlib import Path
from river import tree, preprocessing, compose
import math

# ============================================================================
# CONFIGURATION
# ============================================================================

NATS_URI = "nats://localhost:4222"
FEEDBACK_TOPIC = "fraud.feedback"

PERSIST = Path("pathway_persistence")
PERSIST.mkdir(exist_ok=True)

MODEL_PATH = PERSIST / "ml_models.pkl"
STATS_PATH = PERSIST / "stats.json"
PROCESSED_PATH = PERSIST / "processed_feedback.json"
CHECKPOINT_DIR = PERSIST / "feedback_checkpoints"

# ============================================================================
# FEEDBACK SCHEMA
# ============================================================================

class FeedbackSchema(pw.Schema):
    """Schema for employee feedback from dashboard"""
    trans_num: str = pw.column_definition(dtype=str)
    cc_num: int = pw.column_definition(dtype=int)
    merchant: str = pw.column_definition(dtype=str)
    category: str = pw.column_definition(dtype=str)
    amt: float = pw.column_definition(dtype=float)
    is_fraud: int = pw.column_definition(dtype=int)  # CORRECTED by employee
    prediction: str = pw.column_definition(dtype=str)
    ml_score: float = pw.column_definition(dtype=float)
    tier: int = pw.column_definition(dtype=int)
    reviewer_id: str = pw.column_definition(dtype=str)
    reviewer_notes: str = pw.column_definition(dtype=str)
    review_timestamp: str = pw.column_definition(dtype=str)

# ============================================================================
# SHARED MODEL MANAGER
# ============================================================================

class SharedModelManager:
    """Manages shared ML model - writes updates from feedback"""
    
    def __init__(self):
        if MODEL_PATH.exists():
            print("🔄 Loading existing shared ML models...")
            with open(MODEL_PATH, "rb") as f:
                saved = pickle.load(f)
                self.model_main = saved["model_main"]
                self.model_validator = saved["model_validator"]
            print("✓ Shared models loaded")
        else:
            print("⚠️ No model found. Initializing new models.")
            self.model_main = compose.Pipeline(
                preprocessing.StandardScaler(),
                tree.HoeffdingAdaptiveTreeClassifier(
                    grace_period=200, delta=1e-5, seed=42
                )
            )
            self.model_validator = tree.HoeffdingAdaptiveTreeClassifier(
                grace_period=150, delta=1e-4, seed=123
            )
        
        # Load stats
        self.stats = {
            "feedback_learned": 0,
            "false_positives_corrected": 0,
            "true_frauds_confirmed": 0,
            "total_train_samples": 0
        }
        if STATS_PATH.exists():
            saved_stats = json.load(open(STATS_PATH))
            self.stats.update(saved_stats)
        
        # Track processed feedback
        self.processed = set()
        if PROCESSED_PATH.exists():
            self.processed = set(json.load(open(PROCESSED_PATH)))
        
        print(f"✓ Feedback stats: {self.stats['feedback_learned']:,} corrections processed")
        
        self.last_save = datetime.now()
    
    def save(self):
        """Save shared model + stats"""
        try:
            with open(MODEL_PATH, "wb") as f:
                pickle.dump({
                    "model_main": self.model_main,
                    "model_validator": self.model_validator
                }, f)
            
            # Update total training count
            if STATS_PATH.exists():
                existing = json.load(open(STATS_PATH))
                self.stats["total"] = existing.get("total", 0)
            
            json.dump(self.stats, open(STATS_PATH, "w"))
            
            processed_list = list(self.processed)
            if len(processed_list) > 50000:
                processed_list = processed_list[-50000:]
                self.processed = set(processed_list)
            
            json.dump(processed_list, open(PROCESSED_PATH, "w"))
            
            print(f"💾 Model saved ({self.stats['feedback_learned']:,} feedback samples)")
            
        except Exception as e:
            print(f"⚠️ Could not save model: {e}")

shared = SharedModelManager()

# ============================================================================
# HELPER UDFs
# ============================================================================

@pw.udf
def haversine_distance(lat1, lon1, lat2, lon2):
    try:
        d_lat = math.radians(lat2 - lat1)
        d_lon = math.radians(lon2 - lon1)
        a = (math.sin(d_lat/2)**2 +
             math.cos(math.radians(lat1)) *
             math.cos(math.radians(lat2)) *
             math.sin(d_lon/2)**2)
        return 6371 * 2 * math.asin(math.sqrt(a))
    except:
        return 0.0

@pw.udf
def extract_hour(unix_time):
    try:
        return datetime.fromtimestamp(unix_time).hour
    except:
        return 0

# ============================================================================
# FEEDBACK LEARNING UDF
# ============================================================================

@pw.udf
def learn_from_feedback(
    trans_num, amt, category, is_fraud, 
    prediction, ml_score, tier, reviewer_id):
    """
    Learn from employee feedback
    This is the GROUND TRUTH - employee has verified the correct label
    """
    
    # Dedup check
    if trans_num in shared.processed:
        return json.dumps({"already_processed": True})
    
    shared.processed.add(trans_num)
    shared.stats["feedback_learned"] += 1
    
    # Track correction types
    was_predicted_fraud = (prediction == "FRAUD")
    actual_is_fraud = (is_fraud == 1)
    
    if was_predicted_fraud and not actual_is_fraud:
        # Model predicted fraud, but employee says legitimate
        shared.stats["false_positives_corrected"] += 1
        correction_type = "FALSE_POSITIVE"
    elif was_predicted_fraud and actual_is_fraud:
        # Model was correct
        shared.stats["true_frauds_confirmed"] += 1
        correction_type = "TRUE_POSITIVE"
    elif not was_predicted_fraud and actual_is_fraud:
        # Model missed fraud
        correction_type = "FALSE_NEGATIVE"
    else:
        # Model correctly identified legitimate
        correction_type = "TRUE_NEGATIVE"
    
    # Create features (simplified for feedback)
    feats = {
        "amt": float(amt),
        "online": 1 if category in ["shopping_net", "misc_net", "grocery_net"] else 0,
        "ml_score": float(ml_score),
        "tier": float(tier)
    }
    
    # CRITICAL: Train on CORRECTED label from employee
    try:
        shared.model_main.learn_one(feats, is_fraud)
        shared.model_validator.learn_one(feats, is_fraud)
    except Exception as e:
        print(f"[WARN] Learning failed: {e}")
    
    # Periodic save
    if shared.stats["feedback_learned"] % 20 == 0:
        print(f"📊 Feedback Learning Stats:")
        print(f"   Total Corrections: {shared.stats['feedback_learned']:,}")
        print(f"   False Positives Fixed: {shared.stats['false_positives_corrected']}")
        print(f"   True Frauds Confirmed: {shared.stats['true_frauds_confirmed']}")
        print()
        shared.save()
    
    return json.dumps({
        "learned": True,
        "correction_type": correction_type,
        "trans_num": trans_num,
        "reviewer": reviewer_id
    })

# ============================================================================
# MAIN FEEDBACK PIPELINE
# ============================================================================

def run_feedback_trainer():
    print("═══════════════════════════════════════════════════════════")
    print("   FEEDBACK TRAINER - Employee Corrections")
    print("   Learns from dashboard reviews to improve model")
    print("═══════════════════════════════════════════════════════════")
    print(f"Listening on: {FEEDBACK_TOPIC}")
    print(f"Model Storage: {MODEL_PATH}")
    print()
    
    # Read feedback from dashboard
    feedback = pw.io.nats.read(
        uri=NATS_URI,
        topic=FEEDBACK_TOPIC,
        schema=FeedbackSchema,
        format="json",
        persistent_id="feedback_trainer"
    )
    
    print("✓ Subscribed to employee feedback stream")
    print()
    
    # Process feedback
    learned = feedback.select(
        result=learn_from_feedback(
            pw.this.trans_num,
            pw.this.amt,
            pw.this.category,
            pw.this.is_fraud,
            pw.this.prediction,
            pw.this.ml_score,
            pw.this.tier,
            pw.this.reviewer_id
        )
    )
    
    print("🎯 Feedback learning active...")
    print("   Waiting for employee corrections from dashboard")
    print("   Model will continuously improve from feedback")
    print()
    
    # Run with persistence
    pw.run(
        persistence_config=pw.persistence.Config.simple_config(
            pw.persistence.Backend.filesystem(str(CHECKPOINT_DIR)),
            snapshot_interval_ms=10000
        ),
        monitoring_level=pw.MonitoringLevel.NONE
    )

if __name__ == "__main__":
    run_feedback_trainer()