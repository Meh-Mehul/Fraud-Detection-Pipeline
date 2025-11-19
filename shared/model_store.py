# pipeline/shared/model_store.py
from pathlib import Path
import pickle
import threading
from datetime import datetime

PERSIST_DIR = Path("pathway_persistence")
PERSIST_DIR.mkdir(parents=True, exist_ok=True)

MODEL_PATH = PERSIST_DIR / "ml_models.pkl"
PROCESSED_PATH = PERSIST_DIR / "processed_trans.json"
STATS_PATH = PERSIST_DIR / "stats_store.json"

_lock = threading.Lock()

class ModelStore:
    def __init__(self, model_path=MODEL_PATH):
        self.model_path = Path(model_path)
        self._last_saved = 0

    def save(self, model_main, model_validator):
        """Atomically save shared models."""
        with _lock:
            tmp = self.model_path.with_suffix(".pkl.tmp")
            with open(tmp, "wb") as f:
                pickle.dump({
                    "model_main": model_main,
                    "model_validator": model_validator,
                    "saved_at": datetime.utcnow().isoformat()
                }, f)
            tmp.replace(self.model_path)

    def load(self):
        with _lock:
            if not self.model_path.exists():
                return None
            try:
                with open(self.model_path, "rb") as f:
                    saved = pickle.load(f)
                return saved.get("model_main"), saved.get("model_validator")
            except Exception:
                return None

model_store = ModelStore()
