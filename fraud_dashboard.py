"""
Bank Fraud Detection Dashboard - FIXED
Properly handles JSON string results from inference detector
"""

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import asyncio
import json
from datetime import datetime
from typing import List, Dict, Optional
import nats
from collections import deque

# ============================================================================
# CONFIGURATION
# ============================================================================

NATS_URI = "nats://localhost:4222"
INFERENCE_RESULTS_TOPIC = "fraud.inference_results"
FEEDBACK_TOPIC = "fraud.feedback"

# ============================================================================
# DATA MODELS
# ============================================================================

class FeedbackSubmission(BaseModel):
    trans_num: str
    actual_label: int  # 0 = legitimate, 1 = fraud
    reviewer_notes: Optional[str] = ""
    reviewer_id: str

class TransactionReview:
    """In-memory storage of transactions pending review"""
    
    def __init__(self, max_size=1000):
        self.pending = deque(maxlen=max_size)
        self.reviewed = deque(maxlen=max_size)
        self.by_trans_num = {}
        self.stats = {
            "total_reviewed": 0,
            "confirmed_fraud": 0,
            "false_positives": 0,
            "pending_count": 0
        }
    
    def add_pending(self, transaction: dict):
        """Add new transaction for review"""
        if transaction.get("requires_review"):
            trans_num = transaction.get("trans_num", "UNKNOWN")
            if trans_num not in self.by_trans_num:
                self.pending.append(transaction)
                self.by_trans_num[trans_num] = transaction
                self.stats["pending_count"] = len(self.pending)
    
    def submit_feedback(self, trans_num: str, actual_label: int, notes: str, reviewer_id: str):
        """Mark transaction as reviewed with feedback"""
        if trans_num in self.by_trans_num:
            transaction = self.by_trans_num[trans_num]
            transaction["reviewed"] = True
            transaction["feedback_label"] = actual_label
            transaction["reviewer_notes"] = notes
            transaction["reviewer_id"] = reviewer_id
            transaction["review_timestamp"] = datetime.now().isoformat()
            
            # Move to reviewed
            self.reviewed.append(transaction)
            
            # Update stats
            self.stats["total_reviewed"] += 1
            if actual_label == 1:
                self.stats["confirmed_fraud"] += 1
            else:
                self.stats["false_positives"] += 1
            
            # Remove from pending
            self.pending = deque([t for t in self.pending if t["trans_num"] != trans_num], 
                                maxlen=self.pending.maxlen)
            self.stats["pending_count"] = len(self.pending)
            
            return transaction
        return None
    
    def get_pending(self, limit=50):
        """Get pending transactions for review"""
        return list(self.pending)[:limit]
    
    def get_reviewed(self, limit=50):
        """Get recently reviewed transactions"""
        return list(self.reviewed)[:limit]

# Global storage
review_storage = TransactionReview()

# ============================================================================
# FASTAPI APP
# ============================================================================

app = FastAPI(title="Bank Fraud Detection Dashboard")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================================
# NATS SUBSCRIBER (Background Task)
# ============================================================================

async def subscribe_to_inference_results():
    """Subscribe to inference results and populate dashboard"""
    try:
        nc = await nats.connect(NATS_URI)
        
        async def message_handler(msg):
            try:
                # First parse the outer message
                raw_data = json.loads(msg.data.decode())
                
                # Handle two possible formats:
                # 1. Direct JSON object: {"requires_review": true, "trans_num": "..."}
                # 2. Wrapped in result_json: {"result_json": "{\"requires_review\": true, ...}"}
                
                if isinstance(raw_data, dict):
                    if "result_json" in raw_data:
                        # Format 2: Extract and parse the JSON string
                        data = json.loads(raw_data["result_json"])
                    else:
                        # Format 1: Use directly
                        data = raw_data
                else:
                    # Edge case: raw_data is already a string
                    data = json.loads(raw_data)
                
                # Only process if it requires review
                if data.get("requires_review", False):
                    review_storage.add_pending(data)
                    trans_num = data.get("trans_num", "UNKNOWN")
                    tier = data.get("tier", "?")
                    print(f"📥 New transaction for review: {trans_num} (Tier {tier})")
                
            except json.JSONDecodeError as e:
                print(f"JSON decode error: {e}")
                print(f"Raw data: {msg.data.decode()[:200]}")
            except Exception as e:
                print(f"Error processing message: {e}")
                import traceback
                traceback.print_exc()
        
        await nc.subscribe(INFERENCE_RESULTS_TOPIC, cb=message_handler)
        print(f"✓ Subscribed to {INFERENCE_RESULTS_TOPIC}")
        
        # Keep connection alive
        while True:
            await asyncio.sleep(1)
            
    except Exception as e:
        print(f"NATS connection error: {e}")
        import traceback
        traceback.print_exc()

async def publish_feedback(feedback_data: dict):
    """Publish feedback to NATS for model retraining"""
    try:
        nc = await nats.connect(NATS_URI)
        await nc.publish(FEEDBACK_TOPIC, json.dumps(feedback_data).encode())
        await nc.close()
        print(f"✅ Feedback published: {feedback_data['trans_num']}")
    except Exception as e:
        print(f"Error publishing feedback: {e}")

# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.on_event("startup")
async def startup_event():
    """Start NATS subscriber on startup"""
    asyncio.create_task(subscribe_to_inference_results())

@app.get("/")
async def get_dashboard():
    """Serve the main dashboard HTML"""
    return HTMLResponse(content=DASHBOARD_HTML)

@app.get("/api/pending")
async def get_pending_transactions(limit: int = 50):
    """Get transactions pending review"""
    return {
        "transactions": review_storage.get_pending(limit),
        "count": len(review_storage.pending)
    }

@app.get("/api/reviewed")
async def get_reviewed_transactions(limit: int = 50):
    """Get recently reviewed transactions"""
    return {
        "transactions": review_storage.get_reviewed(limit),
        "count": len(review_storage.reviewed)
    }

@app.get("/api/stats")
async def get_stats():
    """Get dashboard statistics"""
    return review_storage.stats

@app.post("/api/feedback")
async def submit_feedback(feedback: FeedbackSubmission):
    """Submit feedback for a transaction"""
    
    # Update internal storage
    transaction = review_storage.submit_feedback(
        feedback.trans_num,
        feedback.actual_label,
        feedback.reviewer_notes,
        feedback.reviewer_id
    )
    
    if transaction is None:
        return JSONResponse(
            status_code=404,
            content={"error": "Transaction not found"}
        )
    
    # Publish to NATS for model retraining
    feedback_data = {
        "trans_num": transaction["trans_num"],
        "cc_num": transaction["cc_num"],
        "merchant": transaction["merchant"],
        "category": transaction["category"],
        "amt": transaction["amt"],
        "is_fraud": feedback.actual_label,  # CORRECTED LABEL
        "prediction": transaction["prediction"],
        "ml_score": transaction["ml_score"],
        "tier": transaction["tier"],
        "reviewer_id": feedback.reviewer_id,
        "reviewer_notes": feedback.reviewer_notes,
        "review_timestamp": datetime.now().isoformat()
    }
    
    await publish_feedback(feedback_data)
    
    return {
        "success": True,
        "message": "Feedback submitted successfully",
        "transaction": transaction
    }

# ============================================================================
# WEBSOCKET FOR REAL-TIME UPDATES
# ============================================================================

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket for real-time dashboard updates"""
    await websocket.accept()
    
    try:
        while True:
            # Send stats every 2 seconds
            await asyncio.sleep(2)
            stats = review_storage.stats
            await websocket.send_json({
                "type": "stats_update",
                "data": stats
            })
            
    except WebSocketDisconnect:
        print("WebSocket disconnected")

# ============================================================================
# DASHBOARD HTML (Same as before)
# ============================================================================

DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Fraud Detection Dashboard</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }
        
        .container {
            max-width: 1400px;
            margin: 0 auto;
        }
        
        header {
            background: white;
            padding: 30px;
            border-radius: 12px;
            box-shadow: 0 8px 16px rgba(0,0,0,0.1);
            margin-bottom: 30px;
        }
        
        h1 {
            color: #2d3748;
            font-size: 2.5rem;
            margin-bottom: 10px;
        }
        
        .subtitle {
            color: #718096;
            font-size: 1.1rem;
        }
        
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        
        .stat-card {
            background: white;
            padding: 25px;
            border-radius: 12px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }
        
        .stat-label {
            color: #718096;
            font-size: 0.9rem;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 8px;
        }
        
        .stat-value {
            color: #2d3748;
            font-size: 2.5rem;
            font-weight: bold;
        }
        
        .stat-card.pending .stat-value {
            color: #f59e0b;
        }
        
        .stat-card.fraud .stat-value {
            color: #ef4444;
        }
        
        .stat-card.false .stat-value {
            color: #10b981;
        }
        
        .tabs {
            background: white;
            border-radius: 12px;
            padding: 10px;
            margin-bottom: 20px;
            display: flex;
            gap: 10px;
        }
        
        .tab {
            padding: 12px 24px;
            border: none;
            background: transparent;
            border-radius: 8px;
            cursor: pointer;
            font-size: 1rem;
            font-weight: 500;
            color: #718096;
            transition: all 0.3s;
        }
        
        .tab.active {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
        }
        
        .transactions-container {
            background: white;
            border-radius: 12px;
            padding: 30px;
            box-shadow: 0 8px 16px rgba(0,0,0,0.1);
        }
        
        .transaction-card {
            background: #f7fafc;
            border: 2px solid #e2e8f0;
            border-radius: 10px;
            padding: 20px;
            margin-bottom: 20px;
            transition: all 0.3s;
        }
        
        .transaction-card:hover {
            box-shadow: 0 4px 12px rgba(0,0,0,0.1);
            transform: translateY(-2px);
        }
        
        .transaction-card.tier1 {
            border-left: 6px solid #ef4444;
        }
        
        .transaction-card.tier2 {
            border-left: 6px solid #f59e0b;
        }
        
        .transaction-card.tier3 {
            border-left: 6px solid #3b82f6;
        }
        
        .transaction-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 15px;
        }
        
        .transaction-id {
            font-weight: bold;
            font-size: 1.1rem;
            color: #2d3748;
        }
        
        .tier-badge {
            padding: 6px 12px;
            border-radius: 20px;
            font-size: 0.85rem;
            font-weight: bold;
            color: white;
        }
        
        .tier1 .tier-badge {
            background: #ef4444;
        }
        
        .tier2 .tier-badge {
            background: #f59e0b;
        }
        
        .tier3 .tier-badge {
            background: #3b82f6;
        }
        
        .transaction-details {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin-bottom: 15px;
        }
        
        .detail-item {
            display: flex;
            flex-direction: column;
        }
        
        .detail-label {
            color: #718096;
            font-size: 0.85rem;
            margin-bottom: 4px;
        }
        
        .detail-value {
            color: #2d3748;
            font-weight: 600;
            font-size: 1rem;
        }
        
        .amount {
            font-size: 1.3rem;
            color: #ef4444;
        }
        
        .reasons {
            background: #fff5f5;
            border-left: 3px solid #ef4444;
            padding: 10px 15px;
            margin: 15px 0;
            border-radius: 4px;
        }
        
        .reasons-label {
            font-weight: bold;
            color: #c53030;
            margin-bottom: 5px;
        }
        
        .reasons-text {
            color: #742a2a;
        }
        
        .actions {
            display: flex;
            gap: 10px;
            margin-top: 15px;
        }
        
        .btn {
            padding: 10px 20px;
            border: none;
            border-radius: 6px;
            font-size: 1rem;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.3s;
        }
        
        .btn-fraud {
            background: #ef4444;
            color: white;
        }
        
        .btn-fraud:hover {
            background: #dc2626;
        }
        
        .btn-legit {
            background: #10b981;
            color: white;
        }
        
        .btn-legit:hover {
            background: #059669;
        }
        
        .reviewed-badge {
            background: #10b981;
            color: white;
            padding: 8px 16px;
            border-radius: 20px;
            font-size: 0.9rem;
            font-weight: bold;
        }
        
        .empty-state {
            text-align: center;
            padding: 60px 20px;
            color: #718096;
        }
        
        .empty-icon {
            font-size: 4rem;
            margin-bottom: 20px;
        }
        
        .loading {
            text-align: center;
            padding: 40px;
            color: #718096;
        }
        
        .spinner {
            border: 4px solid #f3f4f6;
            border-top: 4px solid #667eea;
            border-radius: 50%;
            width: 50px;
            height: 50px;
            animation: spin 1s linear infinite;
            margin: 0 auto 20px;
        }
        
        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>🛡️ Fraud Detection Dashboard</h1>
            <div class="subtitle">Real-time transaction review and feedback system</div>
        </header>
        
        <div class="stats-grid">
            <div class="stat-card pending">
                <div class="stat-label">Pending Review</div>
                <div class="stat-value" id="pending-count">0</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Total Reviewed</div>
                <div class="stat-value" id="total-reviewed">0</div>
            </div>
            <div class="stat-card fraud">
                <div class="stat-label">Confirmed Fraud</div>
                <div class="stat-value" id="confirmed-fraud">0</div>
            </div>
            <div class="stat-card false">
                <div class="stat-label">False Positives</div>
                <div class="stat-value" id="false-positives">0</div>
            </div>
        </div>
        
        <div class="tabs">
            <button class="tab active" onclick="showTab('pending')">Pending Review</button>
            <button class="tab" onclick="showTab('reviewed')">Recently Reviewed</button>
        </div>
        
        <div class="transactions-container">
            <div id="pending-tab" class="tab-content">
                <div class="loading">
                    <div class="spinner"></div>
                    <div>Loading transactions...</div>
                </div>
            </div>
            <div id="reviewed-tab" class="tab-content" style="display: none;">
                <div class="loading">
                    <div class="spinner"></div>
                    <div>Loading transactions...</div>
                </div>
            </div>
        </div>
    </div>
    
    <script>
        let currentTab = 'pending';
        
        // WebSocket for real-time updates
        const ws = new WebSocket('ws://' + window.location.host + '/ws');
        
        ws.onmessage = function(event) {
            const data = JSON.parse(event.data);
            if (data.type === 'stats_update') {
                updateStats(data.data);
            }
        };
        
        function updateStats(stats) {
            document.getElementById('pending-count').textContent = stats.pending_count || 0;
            document.getElementById('total-reviewed').textContent = stats.total_reviewed || 0;
            document.getElementById('confirmed-fraud').textContent = stats.confirmed_fraud || 0;
            document.getElementById('false-positives').textContent = stats.false_positives || 0;
        }
        
        function showTab(tab) {
            currentTab = tab;
            
            // Update tab buttons
            document.querySelectorAll('.tab').forEach(btn => {
                btn.classList.remove('active');
            });
            event.target.classList.add('active');
            
            // Update content
            document.getElementById('pending-tab').style.display = tab === 'pending' ? 'block' : 'none';
            document.getElementById('reviewed-tab').style.display = tab === 'reviewed' ? 'block' : 'none';
            
            loadTransactions();
        }
        
        async function loadTransactions() {
            const endpoint = currentTab === 'pending' ? '/api/pending' : '/api/reviewed';
            const container = document.getElementById(currentTab + '-tab');
            
            try {
                const response = await fetch(endpoint);
                const data = await response.json();
                
                if (data.transactions.length === 0) {
                    container.innerHTML = `
                        <div class="empty-state">
                            <div class="empty-icon">✨</div>
                            <h2>No transactions ${currentTab === 'pending' ? 'pending review' : 'reviewed yet'}</h2>
                        </div>
                    `;
                    return;
                }
                
                container.innerHTML = data.transactions.map(t => createTransactionCard(t)).join('');
                
            } catch (error) {
                console.error('Error loading transactions:', error);
                container.innerHTML = '<div class="empty-state">Error loading transactions</div>';
            }
        }
        
        function createTransactionCard(t) {
            const isReviewed = t.reviewed;
            
            return `
                <div class="transaction-card tier${t.tier}">
                    <div class="transaction-header">
                        <div class="transaction-id">${t.trans_num}</div>
                        <div class="tier-badge">TIER ${t.tier} - ${t.confidence}% Confidence</div>
                    </div>
                    
                    <div class="transaction-details">
                        <div class="detail-item">
                            <div class="detail-label">Customer</div>
                            <div class="detail-value">${t.customer_name}</div>
                        </div>
                        <div class="detail-item">
                            <div class="detail-label">Card</div>
                            <div class="detail-value">****${String(t.cc_num).slice(-4)}</div>
                        </div>
                        <div class="detail-item">
                            <div class="detail-label">Amount</div>
                            <div class="detail-value amount">$${t.amt.toFixed(2)}</div>
                        </div>
                        <div class="detail-item">
                            <div class="detail-label">Merchant</div>
                            <div class="detail-value">${t.merchant}</div>
                        </div>
                        <div class="detail-item">
                            <div class="detail-label">Category</div>
                            <div class="detail-value">${t.category}</div>
                        </div>
                        <div class="detail-item">
                            <div class="detail-label">Location</div>
                            <div class="detail-value">${t.location}</div>
                        </div>
                        <div class="detail-item">
                            <div class="detail-label">Date/Time</div>
                            <div class="detail-value">${t.trans_date}</div>
                        </div>
                        <div class="detail-item">
                            <div class="detail-label">ML Score</div>
                            <div class="detail-value">${t.ml_score}%</div>
                        </div>
                    </div>
                    
                    <div class="reasons">
                        <div class="reasons-label">🚨 Fraud Indicators:</div>
                        <div class="reasons-text">${t.reasons}</div>
                    </div>
                    
                    ${isReviewed ? `
                        <div class="reviewed-badge">
                            ✓ Reviewed - ${t.feedback_label === 1 ? 'Confirmed Fraud' : 'False Positive'}
                        </div>
                    ` : `
                        <div class="actions">
                            <button class="btn btn-fraud" onclick="submitFeedback('${t.trans_num}', 1)">
                                ❌ Confirm Fraud
                            </button>
                            <button class="btn btn-legit" onclick="submitFeedback('${t.trans_num}', 0)">
                                ✓ Mark as Legitimate
                            </button>
                        </div>
                    `}
                </div>
            `;
        }
        
        async function submitFeedback(transNum, label) {
            const reviewerId = prompt('Enter your reviewer ID:', 'analyst_001');
            if (!reviewerId) return;
            
            const notes = prompt('Add notes (optional):') || '';
            
            try {
                const response = await fetch('/api/feedback', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({
                        trans_num: transNum,
                        actual_label: label,
                        reviewer_notes: notes,
                        reviewer_id: reviewerId
                    })
                });
                
                if (response.ok) {
                    alert('✅ Feedback submitted successfully!');
                    loadTransactions();
                } else {
                    alert('❌ Error submitting feedback');
                }
                
            } catch (error) {
                console.error('Error submitting feedback:', error);
                alert('❌ Error submitting feedback');
            }
        }
        
        // Initial load
        loadTransactions();
        
        // Refresh every 5 seconds
        setInterval(loadTransactions, 5000);
    </script>
</body>
</html>
"""

# ============================================================================
# RUN SERVER
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    print("═══════════════════════════════════════════════════════════")
    print("   FRAUD DETECTION DASHBOARD (FIXED)")
    print("═══════════════════════════════════════════════════════════")
    print()
    print("  🌐 Dashboard: http://localhost:8000")
    print("  📊 API Docs: http://localhost:8000/docs")
    print()
    
    uvicorn.run(app, host="0.0.0.0", port=8000)