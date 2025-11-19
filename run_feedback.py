# run_feedback.py
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent))
from feedback.feedback_writer import run_feedback_writer
if __name__ == "__main__":
    run_feedback_writer()
