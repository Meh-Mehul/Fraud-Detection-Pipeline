"""
Fraud Detection Dashboard - Standalone FastAPI Application

A complete human-in-the-loop fraud detection system with:
- Queue management for fraud alerts
- Human review and labeling interface
- Statistics tracking
- Mock data generation for testing

To run:
1. Install dependencies: pip install fastapi uvicorn pydantic
2. Run: python fraud_dashboard.py
3. Open browser: http://localhost:8000
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List
import json
from pathlib import Path
from datetime import datetime
import random
import uuid

app = FastAPI(title="Fraud Detection Dashboard", version="1.0.0")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuration
QUEUE_SIZE = 10
DATA_DIR = Path("./dashboard_data")
DATA_DIR.mkdir(exist_ok=True)

QUEUE_FILE = DATA_DIR / "queue.json"
LABELS_FILE = DATA_DIR / "labels.json"

# ═══════════════════════════════════════════════════════════
# DATA MODELS
# ═══════════════════════════════════════════════════════════

class AlertMetadata(BaseModel):
    report_id: str
    trans_num: str
    cc_num: int
    amount: float
    merchant: str
    category: str
    risk_score: int
    ml_score: float
    reasons: str
    lat: float
    long: float
    merch_lat: float
    merch_long: float
    timestamp_created: str
    status: str = "pending"

class HumanLabel(BaseModel):
    report_id: str
    label: str
    confidence: int
    notes: Optional[str] = None
    reviewer_name: Optional[str] = "Anonymous"

class LabelRecord(BaseModel):
    report_id: str
    label: str
    confidence: int
    notes: Optional[str]
    reviewer_name: str
    timestamp: str
    trans_num: str
    cc_num: int
    amount: float
    merchant: str
    category: str

# ═══════════════════════════════════════════════════════════
# DATA PERSISTENCE
# ═══════════════════════════════════════════════════════════

def load_queue() -> List[dict]:
    if not QUEUE_FILE.exists():
        return []
    try:
        with open(QUEUE_FILE, "r") as f:
            return json.load(f)
    except:
        return []

def save_queue(queue: List[dict]):
    with open(QUEUE_FILE, "w") as f:
        json.dump(queue, f, indent=2)

def load_labels() -> List[dict]:
    if not LABELS_FILE.exists():
        return []
    try:
        with open(LABELS_FILE, "r") as f:
            return json.load(f)
    except:
        return []

def save_labels(labels: List[dict]):
    with open(LABELS_FILE, "w") as f:
        json.dump(labels, f, indent=2)

# ═══════════════════════════════════════════════════════════
# MOCK DATA GENERATION
# ═══════════════════════════════════════════════════════════

MERCHANTS = [
    "Amazon.com", "Walmart", "Target", "Best Buy", "Apple Store",
    "Starbucks", "McDonald's", "Shell Gas", "Uber", "Netflix",
    "Steam Games", "PlayStation Store", "Nike.com", "Home Depot", "CVS Pharmacy"
]

CATEGORIES = [
    "online_retail", "grocery", "gas_station", "restaurant", "entertainment",
    "electronics", "gaming", "subscription", "clothing", "home_improvement"
]

FRAUD_REASONS = [
    "unusual_location", "high_amount", "velocity_spike", "new_merchant",
    "foreign_transaction", "late_night", "multiple_attempts", "card_not_present"
]

def generate_mock_alert() -> dict:
    """Generate a realistic mock fraud alert"""
    report_id = str(uuid.uuid4())
    trans_num = f"TXN{random.randint(100000, 999999)}"
    cc_num = random.randint(1000, 9999)
    amount = round(random.uniform(50, 5000), 2)
    merchant = random.choice(MERCHANTS)
    category = random.choice(CATEGORIES)
    
    # High-risk transactions have more reasons
    num_reasons = random.randint(2, 4) if random.random() > 0.5 else random.randint(1, 2)
    reasons = "|".join(random.sample(FRAUD_REASONS, num_reasons))
    
    # Risk score correlates with number of reasons and amount
    base_risk = len(reasons.split("|")) * 15 + (amount / 100)
    risk_score = min(100, int(base_risk + random.randint(-10, 20)))
    ml_score = risk_score / 100.0 + random.uniform(-0.1, 0.1)
    
    return {
        "report_id": report_id,
        "trans_num": trans_num,
        "cc_num": cc_num,
        "amount": amount,
        "merchant": merchant,
        "category": category,
        "risk_score": risk_score,
        "ml_score": round(ml_score, 2),
        "reasons": reasons,
        "lat": round(random.uniform(25, 48), 4),
        "long": round(random.uniform(-120, -70), 4),
        "merch_lat": round(random.uniform(25, 48), 4),
        "merch_long": round(random.uniform(-120, -70), 4),
        "timestamp_created": datetime.utcnow().isoformat(),
        "status": "pending"
    }

# ═══════════════════════════════════════════════════════════
# API ENDPOINTS
# ═══════════════════════════════════════════════════════════

@app.get("/")
async def root():
    """Serve the React frontend"""
    html_content = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Fraud Detection Dashboard</title>
    <script crossorigin src="https://unpkg.com/react@18/umd/react.production.min.js"></script>
    <script crossorigin src="https://unpkg.com/react-dom@18/umd/react-dom.production.min.js"></script>
    <script src="https://unpkg.com/@babel/standalone/babel.min.js"></script>
    <script src="https://cdn.tailwindcss.com"></script>
</head>
<body>
    <div id="root"></div>
    <script type="text/babel">
        const { useState, useEffect } = React;

        const API_BASE = '';

        function FraudDashboard() {
          const [queue, setQueue] = useState([]);
          const [stats, setStats] = useState(null);
          const [selectedReport, setSelectedReport] = useState(null);
          const [label, setLabel] = useState('');
          const [confidence, setConfidence] = useState(50);
          const [notes, setNotes] = useState('');
          const [reviewer, setReviewer] = useState('Anonymous');
          const [loading, setLoading] = useState(false);
          const [submitted, setSubmitted] = useState(false);

          useEffect(() => {
            fetchQueue();
            fetchStats();
            const interval = setInterval(() => {
              fetchQueue();
              fetchStats();
            }, 5000);
            return () => clearInterval(interval);
          }, []);

          const fetchQueue = async () => {
            try {
              const res = await fetch(`${API_BASE}/queue`);
              const data = await res.json();
              setQueue(data.queue || []);
            } catch (err) {
              console.error('Error fetching queue:', err);
            }
          };

          const fetchStats = async () => {
            try {
              const res = await fetch(`${API_BASE}/stats`);
              const data = await res.json();
              setStats(data);
            } catch (err) {
              console.error('Error fetching stats:', err);
            }
          };

          const handleSubmitLabel = async () => {
            if (!label || !selectedReport) {
              alert('Please select a label');
              return;
            }

            setLoading(true);
            try {
              const res = await fetch(`${API_BASE}/reports/${selectedReport.report_id}/label`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                  report_id: selectedReport.report_id,
                  label,
                  confidence: parseInt(confidence),
                  notes,
                  reviewer_name: reviewer
                })
              });

              if (res.ok) {
                setSubmitted(true);
                setTimeout(() => {
                  setSelectedReport(null);
                  setLabel('');
                  setConfidence(50);
                  setNotes('');
                  setSubmitted(false);
                  fetchQueue();
                  fetchStats();
                }, 2000);
              } else {
                alert('Error submitting label');
              }
            } catch (err) {
              alert('Error: ' + err.message);
            } finally {
              setLoading(false);
            }
          };

          const getRiskColor = (score) => {
            if (score >= 90) return 'text-red-700 bg-red-50';
            if (score >= 80) return 'text-orange-700 bg-orange-50';
            if (score >= 70) return 'text-yellow-700 bg-yellow-50';
            return 'text-green-700 bg-green-50';
          };

          if (selectedReport) {
            return (
              <div className="min-h-screen bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900 p-6">
                <div className="max-w-4xl mx-auto">
                  <div className="mb-6">
                    <button
                      onClick={() => setSelectedReport(null)}
                      className="flex items-center gap-2 text-slate-300 hover:text-white transition"
                    >
                      ← Back to Queue
                    </button>
                  </div>

                  <div className="bg-slate-800 rounded-lg border border-slate-700 p-6 mb-6">
                    <div className="grid grid-cols-2 gap-6">
                      <div>
                        <h3 className="text-slate-400 text-sm uppercase tracking-wide">Transaction ID</h3>
                        <p className="text-white text-lg font-mono mt-1">{selectedReport.trans_num}</p>
                      </div>
                      <div>
                        <h3 className="text-slate-400 text-sm uppercase tracking-wide">Amount</h3>
                        <p className="text-white text-lg font-bold mt-1">${selectedReport.amount.toFixed(2)}</p>
                      </div>
                      <div>
                        <h3 className="text-slate-400 text-sm uppercase tracking-wide">Merchant</h3>
                        <p className="text-white text-lg mt-1">{selectedReport.merchant}</p>
                      </div>
                      <div>
                        <h3 className="text-slate-400 text-sm uppercase tracking-wide">Category</h3>
                        <p className="text-white text-lg mt-1">{selectedReport.category}</p>
                      </div>
                    </div>

                    <div className="mt-6 pt-6 border-t border-slate-700">
                      <div className="flex items-center justify-between">
                        <div>
                          <h3 className="text-slate-400 text-sm uppercase tracking-wide">Risk Score</h3>
                          <div className={`mt-2 px-3 py-1 rounded-full inline-block font-bold ${getRiskColor(selectedReport.risk_score)}`}>
                            {selectedReport.risk_score}/100
                          </div>
                        </div>
                        <div>
                          <h3 className="text-slate-400 text-sm uppercase tracking-wide">ML Score</h3>
                          <p className="text-white text-2xl font-bold mt-1">{selectedReport.ml_score.toFixed(1)}</p>
                        </div>
                      </div>
                    </div>

                    <div className="mt-6 pt-6 border-t border-slate-700">
                      <h3 className="text-slate-400 text-sm uppercase tracking-wide mb-3">Detection Indicators</h3>
                      <div className="flex flex-wrap gap-2">
                        {selectedReport.reasons.split('|').map((reason, i) => (
                          <span key={i} className="px-3 py-1 bg-red-500/20 text-red-300 rounded-full text-sm font-mono">
                            {reason}
                          </span>
                        ))}
                      </div>
                    </div>
                  </div>

                  {!submitted ? (
                    <div className="bg-slate-800 rounded-lg border border-slate-700 p-6">
                      <h2 className="text-white text-xl font-bold mb-6">👤 Human Review</h2>

                      <div className="mb-6">
                        <h3 className="text-slate-300 font-semibold mb-3">Is this fraud?</h3>
                        <div className="space-y-2">
                          {['fraud', 'legitimate', 'uncertain'].map((opt) => (
                            <label key={opt} className="flex items-center gap-3 cursor-pointer group">
                              <input
                                type="radio"
                                name="label"
                                value={opt}
                                checked={label === opt}
                                onChange={(e) => setLabel(e.target.value)}
                                className="w-4 h-4"
                              />
                              <span className="text-slate-300 group-hover:text-white transition capitalize">
                                {opt === 'fraud' && '❌ Fraud'}
                                {opt === 'legitimate' && '✅ Legitimate'}
                                {opt === 'uncertain' && '❓ Uncertain'}
                              </span>
                            </label>
                          ))}
                        </div>
                      </div>

                      <div className="mb-6">
                        <div className="flex justify-between items-center mb-2">
                          <h3 className="text-slate-300 font-semibold">Your Confidence</h3>
                          <span className="text-white font-bold text-lg">{confidence}%</span>
                        </div>
                        <input
                          type="range"
                          min="0"
                          max="100"
                          value={confidence}
                          onChange={(e) => setConfidence(e.target.value)}
                          className="w-full h-2 bg-slate-700 rounded-lg appearance-none cursor-pointer"
                        />
                      </div>

                      <div className="mb-6">
                        <h3 className="text-slate-300 font-semibold mb-2">Additional Notes (Optional)</h3>
                        <textarea
                          value={notes}
                          onChange={(e) => setNotes(e.target.value)}
                          placeholder="Add investigation notes..."
                          className="w-full px-4 py-3 bg-slate-700 text-white rounded-lg border border-slate-600 focus:border-blue-500 focus:outline-none resize-none"
                          rows="4"
                        />
                      </div>

                      <div className="mb-6">
                        <h3 className="text-slate-300 font-semibold mb-2">Reviewer Name</h3>
                        <input
                          type="text"
                          value={reviewer}
                          onChange={(e) => setReviewer(e.target.value)}
                          placeholder="Your name"
                          className="w-full px-4 py-2 bg-slate-700 text-white rounded-lg border border-slate-600 focus:border-blue-500 focus:outline-none"
                        />
                      </div>

                      <div className="flex gap-3">
                        <button
                          onClick={() => setSelectedReport(null)}
                          className="flex-1 px-4 py-2 bg-slate-700 text-white rounded-lg hover:bg-slate-600 transition"
                        >
                          Cancel
                        </button>
                        <button
                          onClick={handleSubmitLabel}
                          disabled={!label || loading}
                          className="flex-1 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition disabled:bg-slate-600"
                        >
                          {loading ? 'Submitting...' : 'Submit Label'}
                        </button>
                      </div>
                    </div>
                  ) : (
                    <div className="bg-green-500/10 border border-green-500 rounded-lg p-6 text-center">
                      <div className="text-4xl mb-3">✅</div>
                      <h3 className="text-white text-lg font-bold">Label Submitted!</h3>
                      <p className="text-green-300 mt-2">Redirecting to queue...</p>
                    </div>
                  )}
                </div>
              </div>
            );
          }

          return (
            <div className="min-h-screen bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900 p-6">
              <div className="max-w-7xl mx-auto">
                <div className="mb-8">
                  <h1 className="text-4xl font-bold text-white mb-2">🔍 Fraud Detection Dashboard</h1>
                  <p className="text-slate-400">Human-in-the-Loop Alert Review System</p>
                </div>

                {stats && (
                  <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-8">
                    <div className="bg-slate-800 rounded-lg border border-slate-700 p-4">
                      <h3 className="text-slate-400 text-sm uppercase tracking-wide">Queue Status</h3>
                      <div className="mt-2 flex items-end gap-2">
                        <p className="text-3xl font-bold text-white">{stats.queue.size}/{stats.queue.capacity}</p>
                        <p className="text-slate-400 mb-1">{stats.queue.utilization}</p>
                      </div>
                      <div className="mt-3 w-full bg-slate-700 rounded-full h-2">
                        <div
                          className="bg-blue-500 h-2 rounded-full transition-all"
                          style={{ width: stats.queue.utilization }}
                        />
                      </div>
                    </div>

                    <div className="bg-slate-800 rounded-lg border border-slate-700 p-4">
                      <h3 className="text-slate-400 text-sm uppercase tracking-wide">Labels Today</h3>
                      <p className="text-3xl font-bold text-white mt-2">{stats.labels.count_today}</p>
                    </div>

                    <div className="bg-slate-800 rounded-lg border border-slate-700 p-4">
                      <h3 className="text-slate-400 text-sm uppercase tracking-wide">Total Labels</h3>
                      <p className="text-3xl font-bold text-white mt-2">{stats.labels.total_files}</p>
                    </div>
                  </div>
                )}

                <div className="bg-slate-800 rounded-lg border border-slate-700 p-6">
                  <div className="flex justify-between items-center mb-6">
                    <h2 className="text-2xl font-bold text-white">📋 Review Queue</h2>
                    <button
                      onClick={() => {
                        fetchQueue();
                        fetchStats();
                      }}
                      className="p-2 hover:bg-slate-700 rounded-lg transition"
                    >
                      🔄
                    </button>
                  </div>

                  {queue.length === 0 ? (
                    <div className="text-center py-12">
                      <div className="text-6xl mb-4">✅</div>
                      <p className="text-slate-400 text-lg">No alerts pending review</p>
                    </div>
                  ) : (
                    <div className="space-y-3">
                      {queue.map((alert, idx) => (
                        <button
                          key={idx}
                          onClick={() => setSelectedReport(alert)}
                          className="w-full bg-slate-700 hover:bg-slate-600 rounded-lg p-4 transition text-left border border-slate-600 hover:border-slate-500 group"
                        >
                          <div className="flex items-center justify-between">
                            <div className="flex-1">
                              <div className="flex items-center gap-3">
                                <div className={`w-10 h-10 rounded-lg flex items-center justify-center ${getRiskColor(alert.risk_score)}`}>
                                  {alert.risk_score >= 80 ? '🔴' : alert.risk_score >= 70 ? '🟠' : '🟡'}
                                </div>
                                <div>
                                  <p className="text-white font-semibold">{alert.trans_num}</p>
                                  <p className="text-slate-400 text-sm">{alert.merchant} • ${alert.amount.toFixed(2)}</p>
                                </div>
                              </div>
                            </div>
                            <div className="text-right mr-4">
                              <div className={`px-3 py-1 rounded-full text-sm font-bold ${getRiskColor(alert.risk_score)}`}>
                                {alert.risk_score}/100
                              </div>
                            </div>
                            <span className="text-slate-500 group-hover:text-slate-300 transition">›</span>
                          </div>
                        </button>
                      ))}
                    </div>
                  )}
                </div>

                <div className="mt-8 text-center text-slate-500 text-sm">
                  <p>Last updated: {new Date().toLocaleTimeString()}</p>
                  <p className="mt-1">Queue auto-refreshes every 5 seconds</p>
                </div>
              </div>
            </div>
          );
        }

        ReactDOM.render(<FraudDashboard />, document.getElementById('root'));
    </script>
</body>
</html>
    """
    return HTMLResponse(content=html_content)

@app.get("/queue")
async def get_queue_status():
    """Get current queue with all pending reports"""
    queue = load_queue()
    pending = [a for a in queue if a["status"] == "pending"]
    
    return {
        "queue": pending,
        "current_size": len(queue),
        "capacity": QUEUE_SIZE,
        "available_slots": QUEUE_SIZE - len(queue)
    }

@app.get("/stats")
async def get_system_stats():
    """Get system statistics"""
    queue = load_queue()
    labels = load_labels()
    
    today = datetime.utcnow().date().isoformat()
    labels_today = sum(1 for l in labels if l.get("timestamp", "").startswith(today))
    
    return {
        "queue": {
            "size": len(queue),
            "capacity": QUEUE_SIZE,
            "utilization": f"{(len(queue) / QUEUE_SIZE) * 100:.1f}%"
        },
        "labels": {
            "count_today": labels_today,
            "total_files": len(labels)
        }
    }

@app.post("/reports/{report_id}/label")
async def submit_label(report_id: str, submission: HumanLabel):
    """Submit human label for report"""
    
    if submission.label not in ["fraud", "legitimate", "uncertain"]:
        raise HTTPException(status_code=400, detail="Invalid label")
    
    if not (0 <= submission.confidence <= 100):
        raise HTTPException(status_code=400, detail="Confidence must be 0-100")
    
    queue = load_queue()
    alert = next((a for a in queue if a["report_id"] == report_id), None)
    
    if not alert:
        raise HTTPException(status_code=404, detail="Report not found in queue")
    
    # Create label record
    label_rec = {
        "report_id": report_id,
        "label": submission.label,
        "confidence": submission.confidence,
        "notes": submission.notes,
        "reviewer_name": submission.reviewer_name,
        "timestamp": datetime.utcnow().isoformat(),
        "trans_num": alert["trans_num"],
        "cc_num": alert["cc_num"],
        "amount": alert["amount"],
        "merchant": alert["merchant"],
        "category": alert["category"]
    }
    
    # Save label
    labels = load_labels()
    labels.append(label_rec)
    save_labels(labels)
    
    # Remove from queue
    queue = [a for a in queue if a["report_id"] != report_id]
    save_queue(queue)
    
    print(f"✓ Label submitted: {report_id} → {submission.label} ({submission.confidence}%)")
    
    return {
        "status": "labeled",
        "report_id": report_id,
        "label": submission.label,
        "confidence": submission.confidence
    }

@app.post("/admin/generate-alerts")
async def generate_test_alerts(count: int = 5):
    """Generate mock alerts for testing (Admin endpoint)"""
    queue = load_queue()
    
    generated = 0
    for _ in range(count):
        if len(queue) >= QUEUE_SIZE:
            break
        
        alert = generate_mock_alert()
        queue.append(alert)
        generated += 1
    
    save_queue(queue)
    
    return {
        "status": "success",
        "generated": generated,
        "queue_size": len(queue),
        "message": f"Generated {generated} mock alerts"
    }

@app.delete("/admin/clear-queue")
async def clear_queue():
    """Clear the entire queue (Admin endpoint)"""
    save_queue([])
    return {"status": "cleared", "message": "Queue cleared"}

@app.delete("/admin/clear-labels")
async def clear_labels():
    """Clear all labels (Admin endpoint)"""
    save_labels([])
    return {"status": "cleared", "message": "Labels cleared"}

@app.get("/labels")
async def get_all_labels():
    """Get all labeled reports"""
    labels = load_labels()
    return {
        "count": len(labels),
        "labels": sorted(labels, key=lambda x: x.get("timestamp", ""), reverse=True)
    }

if __name__ == "__main__":
    import uvicorn
    
    print("=" * 60)
    print("🔍 Fraud Detection Dashboard")
    print("=" * 60)
    print("Starting server at http://localhost:8000")
    print()
    print("API Endpoints:")
    print("  • GET  /                     - Dashboard UI")
    print("  • GET  /queue                - Get review queue")
    print("  • GET  /stats                - Get statistics")
    print("  • POST /reports/{id}/label   - Submit label")
    print("  • POST /admin/generate-alerts?count=5 - Generate test data")
    print("  • DELETE /admin/clear-queue  - Clear queue")
    print("  • GET  /labels               - View all labels")
    print()
    print("Quick Start:")
    print("  1. Open http://localhost:8000 in your browser")
    print("  2. Generate test data:")
    print("     curl -X POST http://localhost:8000/admin/generate-alerts?count=5")
    print("=" * 60)
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info"
    )