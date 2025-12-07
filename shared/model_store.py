# pipeline/shared/model_store.py
"""
Shared model store for saving/loading River ML models.
Thread-safe with atomic writes.
"""
import pickle
import threading
from pathlib import Path

# ──────────────────────────────────────────────────────
# PATHS & LOCK
# ──────────────────────────────────────────────────────
PERSIST_DIR = Path("./pathway_persistence")
PERSIST_DIR.mkdir(parents=True, exist_ok=True)

MODEL_FILE = PERSIST_DIR / "ml_models.pkl"
_LOCK = threading.Lock()


# ──────────────────────────────────────────────────────
# SAVE FUNCTION
# ──────────────────────────────────────────────────────
def save(model_main, model_validator):
    """
    Save both models as a tuple to disk atomically.
    
    Args:
        model_main: Primary River model
        model_validator: Secondary River model
    
    Returns:
        bool: True if save succeeded
    """
    with _LOCK:
        try:
            # Create tuple of models
            models = (model_main, model_validator)
            
            # Write to temp file first (atomic write)
            tmp_file = MODEL_FILE.with_suffix(".pkl.tmp")
            with open(tmp_file, 'wb') as f:
                pickle.dump(models, f, protocol=pickle.HIGHEST_PROTOCOL)
            
            # Atomic rename
            tmp_file.replace(MODEL_FILE)
            
            # Verify the file was created and is valid
            if not MODEL_FILE.exists():
                print(f"[ERROR] Model file not created: {MODEL_FILE}")
                return False
            
            # Quick validation - try to load it back
            with open(MODEL_FILE, 'rb') as f:
                test_load = pickle.load(f)
            
            if not isinstance(test_load, tuple) or len(test_load) != 2:
                print(f"[ERROR] Model file invalid format after save")
                return False
            
            print(f"[OK] Models saved successfully to {MODEL_FILE} ({MODEL_FILE.stat().st_size / 1024:.1f} KB)")
            return True
            
        except Exception as e:
            print(f"[ERROR] Model save error: {e}")
            import traceback
            traceback.print_exc()
            return False


# ──────────────────────────────────────────────────────
# LOAD FUNCTION
# ──────────────────────────────────────────────────────
def load():
    """
    Load both models from disk.
    
    Returns:
        tuple: (model_main, model_validator) or None if file doesn't exist or invalid
    """
    with _LOCK:
        if not MODEL_FILE.exists():
            return None
        
        try:
            with open(MODEL_FILE, 'rb') as f:
                models = pickle.load(f)
            
            # Validate format
            if not isinstance(models, tuple) or len(models) != 2:
                print(f"[WARN]  Invalid model format: {type(models)}, expected tuple of length 2")
                return None
            
            model_main, model_validator = models
            return (model_main, model_validator)
            
        except Exception as e:
            print(f"[ERROR] Model load error: {e}")
            return None


# ──────────────────────────────────────────────────────
# UTILITY
# ──────────────────────────────────────────────────────
def exists():
    """Check if model file exists."""
    return MODEL_FILE.exists()


def delete():
    """Delete model file (use with caution)."""
    with _LOCK:
        if MODEL_FILE.exists():
            MODEL_FILE.unlink()
            print("[OK] Model file deleted")
            return True
        return False