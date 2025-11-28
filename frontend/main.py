"""
FastAPI Fraud Report Review System
FIXED VERSION: Uses PDF Parsing as fallback (No Mock Data)
"""

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from pathlib import Path
import json
import time
from datetime import datetime
import os
import re

# ---------------------------------------------------------
# TRY TO IMPORT PYPDF FOR PARSING
# ---------------------------------------------------------
try:
    from pypdf import PdfReader
except ImportError:
    print("WARNING: 'pypdf' not installed. PDF parsing will be limited.")
    print("Please run: pip install pypdf")
    PdfReader = None

app = FastAPI(title="Fraud Report Review System")

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuration
REPORTS_DIR = Path("./fraud_reports")
QUEUE_SIZE = 50  # Showing up to 50 files
STATS_FILE = Path("./review_stats.json")

# Global state
review_queue = []
review_stats = {"reviewed": 0, "fraud": 0, "legitimate": 0}
reviewed_reports = set()


class FeedbackRequest(BaseModel):
    filename: str
    is_fraud: int
    trans_num: str
    cc_num: str


def load_stats():
    """Load review statistics from file"""
    global review_stats, reviewed_reports
    if STATS_FILE.exists():
        with open(STATS_FILE, 'r') as f:
            data = json.load(f)
            review_stats = data.get('stats', review_stats)
            reviewed_reports = set(data.get('reviewed_reports', []))


def save_stats():
    """Save review statistics to file"""
    with open(STATS_FILE, 'w') as f:
        json.dump({
            'stats': review_stats,
            'reviewed_reports': list(reviewed_reports)
        }, f, indent=2)


def scan_reports():
    """Scan reports directory and build queue"""
    global review_queue
    
    if not REPORTS_DIR.exists():
        os.makedirs(REPORTS_DIR, exist_ok=True)
        return
    
    reports = []
    # FIX: Detect ALL .pdf files
    for pdf_file in REPORTS_DIR.glob("*.pdf"):
        try:
            timestamp = pdf_file.stat().st_mtime
            parts = pdf_file.stem.split('_')
            cc_suffix = parts[-1] if len(parts) > 1 else "0000"
            
            report_data = {
                'filename': pdf_file.name,
                'filepath': str(pdf_file),
                'timestamp': timestamp,
                'cc_num': cc_suffix,
                'reviewed': pdf_file.name in reviewed_reports
            }
            
            reports.append(report_data)
        except Exception as e:
            print(f"Error processing {pdf_file}: {e}")
            continue
    
    reports.sort(key=lambda x: x['timestamp'], reverse=True)
    review_queue = reports[:QUEUE_SIZE]


def parse_alert_json_from_pdf(pdf_path):
    """
    1. Try to load companion JSON.
    2. If missing, Parse PDF text directly.
    3. If that fails, return safe empty defaults.
    """
    json_file = pdf_path.with_suffix('.json')
    
    # ------------------------------------------------
    # STRATEGY 1: Load JSON (Best Data)
    # ------------------------------------------------
    if json_file.exists():
        try:
            with open(json_file, 'r') as f:
                return json.load(f)
        except:
            pass 

    # ------------------------------------------------
    # STRATEGY 2: Parse PDF Text (Fallback)
    # ------------------------------------------------
    
    # Initialize safe defaults (No random data)
    data = {
        'trans_num': "UNKNOWN",
        'cc_num': "0000",
        'amt': 0.00,
        'merchant': "Unknown Merchant",
        'category': "Uncategorized",
        'location': "Unknown",
        'risk_score': 0,
        'tier': 1,
        'reasons': "Data extracted from PDF",
        'confidence': 0,
        'ml_score': 0,
        'actual_fraud': 0,
        'first': "", 'last': "", 'job': "",
        'street': "", 'city': "", 'state': "", 'zip': "",
        'distance': 0.0
    }

    if PdfReader:
        try:
            reader = PdfReader(pdf_path)
            text = ""
            for page in reader.pages:
                text += page.extract_text() + "\n"
            
            # --- Regex Extraction Logic ---
            # Attempt to find standard patterns in the text
            
            # Merchant (e.g. "Merchant: Amazon")
            m_match = re.search(r"(?:Merchant|Vendor)\s*[:.]?\s*([^\n]+)", text, re.IGNORECASE)
            if m_match: data['merchant'] = m_match.group(1).strip()

            # Amount (e.g. "Amount: $150.00")
            a_match = re.search(r"(?:Amount|Total)\s*[:.]?\s*\$?\s*([\d,]+\.\d{2})", text, re.IGNORECASE)
            if a_match: 
                try: data['amt'] = float(a_match.group(1).replace(',', ''))
                except: pass

            # Risk Score (e.g. "Risk Score: 85")
            r_match = re.search(r"Risk\s*(?:Score)?\s*[:.]?\s*(\d+)", text, re.IGNORECASE)
            if r_match: 
                data['risk_score'] = int(r_match.group(1))
                # Auto-calculate tier based on risk if not found
                data['tier'] = 3 if data['risk_score'] > 80 else 2 if data['risk_score'] > 50 else 1

            # Transaction ID
            t_match = re.search(r"(?:Transaction|ID)\s*[:#]?\s*([A-Z0-9-]+)", text, re.IGNORECASE)
            if t_match: data['trans_num'] = t_match.group(1)

            # Location
            l_match = re.search(r"Location\s*[:.]?\s*([^\n]+)", text, re.IGNORECASE)
            if l_match: data['location'] = l_match.group(1).strip()
            
            # CC Number (Last 4)
            c_match = re.search(r"(?:Card|CC)\s*(?:#|No\.?)?\s*(?:\*{4})?(\d{4})", text)
            if c_match: data['cc_num'] = c_match.group(1)

        except Exception as e:
            print(f"Failed to parse PDF {pdf_path.name}: {e}")
    
    return data


@app.on_event("startup")
async def startup_event():
    load_stats()
    scan_reports()
    print(f"Loaded {len(review_queue)} reports into queue")


@app.get("/", response_class=HTMLResponse)
async def root():
    """Serve main HTML interface"""
    html_content = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Fraud Investigation Center</title>
    <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-gray-50">
    <div class="bg-gradient-to-r from-gray-900 to-gray-800 text-white shadow-lg">
        <div class="max-w-7xl mx-auto px-6 py-4">
            <div class="flex items-center justify-between">
                <div class="flex items-center gap-3">
                    <svg class="w-8 h-8 text-red-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"/>
                    </svg>
                    <div>
                        <h1 class="text-2xl font-bold">Fraud Investigation Center</h1>
                        <p class="text-gray-300 text-sm">Real-time Report Review System</p>
                    </div>
                </div>
                <div class="flex items-center gap-6">
                    <div class="text-right">
                        <div class="text-2xl font-bold text-green-400" id="stat-reviewed">0</div>
                        <div class="text-xs text-gray-400">Reviewed</div>
                    </div>
                    <div class="text-right">
                        <div class="text-2xl font-bold text-red-400" id="stat-fraud">0</div>
                        <div class="text-xs text-gray-400">Fraud</div>
                    </div>
                    <div class="text-right">
                        <div class="text-2xl font-bold text-blue-400" id="stat-legitimate">0</div>
                        <div class="text-xs text-gray-400">Legitimate</div>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <div class="max-w-7xl mx-auto px-6 py-6">
        <div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
            <div class="lg:col-span-1 bg-white rounded-lg shadow-md border border-gray-200">
                <div class="p-4 border-b border-gray-200 bg-gray-50">
                    <div class="flex items-center justify-between">
                        <div class="flex items-center gap-2">
                            <svg class="w-5 h-5 text-blue-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z"/>
                            </svg>
                            <h2 class="font-bold text-gray-900">Review Queue</h2>
                            <span class="px-2 py-1 bg-blue-100 text-blue-700 rounded-full text-xs font-semibold" id="queue-count">0</span>
                        </div>
                        <button id="refresh-btn" class="p-2 rounded-lg bg-green-100 text-green-700 hover:bg-green-200 transition-colors">
                            <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"/>
                            </svg>
                        </button>
                    </div>
                    <div class="text-xs text-gray-500 mt-1" id="last-update">Updated: --:--:--</div>
                </div>

                <div id="queue-list" class="overflow-y-auto max-h-96">
                    <div class="p-8 text-center text-gray-500">
                        <p>Loading reports...</p>
                    </div>
                </div>
            </div>

            <div id="details-panel" class="lg:col-span-2 bg-white rounded-lg shadow-md border border-gray-200">
                <div class="flex flex-col items-center justify-center h-96 text-gray-500">
                    <p class="text-lg font-semibold">Select a report to review</p>
                    <p class="text-sm">Choose a report from the queue to begin investigation</p>
                </div>
            </div>
        </div>
    </div>

    <script>
        let selectedReport = null;
        let autoRefresh = true;

        function getRiskColor(score) {
            if (score >= 90) return 'text-red-700 bg-red-50 border-red-300';
            if (score >= 80) return 'text-orange-700 bg-orange-50 border-orange-300';
            if (score >= 70) return 'text-yellow-700 bg-yellow-50 border-yellow-300';
            return 'text-blue-700 bg-blue-50 border-blue-300';
        }

        function getTierBadge(tier) {
            const colors = {1: 'bg-red-600', 2: 'bg-orange-500', 3: 'bg-yellow-500'};
            return colors[tier] || 'bg-gray-500';
        }

        async function fetchQueue() {
            try {
                const response = await fetch('/api/queue');
                const data = await response.json();
                
                document.getElementById('stat-reviewed').textContent = data.stats.reviewed;
                document.getElementById('stat-fraud').textContent = data.stats.fraud;
                document.getElementById('stat-legitimate').textContent = data.stats.legitimate;
                
                const unreviewed = data.queue.filter(r => !r.reviewed).length;
                document.getElementById('queue-count').textContent = unreviewed;
                document.getElementById('last-update').textContent = 'Updated: ' + new Date().toLocaleTimeString();
                
                renderQueue(data.queue);
            } catch (error) {
                console.error('Error fetching queue:', error);
            }
        }

        function renderQueue(queue) {
            const queueList = document.getElementById('queue-list');
            
            if (queue.length === 0) {
                queueList.innerHTML = '<div class="p-8 text-center text-gray-500"><p>No reports in queue</p></div>';
                return;
            }
            
            queueList.innerHTML = queue.map(report => {
                const selected = selectedReport && selectedReport.filename === report.filename;
                const ccDisplay = report.cc_num ? report.cc_num.toString().slice(-4) : '0000';
                
                return `
                    <button onclick="selectReport('${report.filename}')" 
                        class="w-full p-4 border-b border-gray-100 hover:bg-gray-50 transition-colors text-left ${report.reviewed ? 'opacity-50' : ''} ${selected ? 'bg-blue-50 border-l-4 border-l-blue-600' : ''}">
                        <div class="flex items-start justify-between mb-2">
                            <div class="flex items-center gap-2">
                                <span class="px-2 py-1 rounded text-xs font-bold text-white ${getTierBadge(report.tier)}">T${report.tier}</span>
                                <span class="px-2 py-1 rounded text-xs font-semibold border ${getRiskColor(report.risk_score)}">${report.risk_score}</span>
                            </div>
                            ${report.reviewed ? '<span class="text-xs text-green-600 font-semibold">✓ Reviewed</span>' : ''}
                        </div>
                        <div class="text-sm font-semibold text-gray-900 mb-1">****${ccDisplay}</div>
                        <div class="text-xs text-gray-600 mb-1">$${report.amt.toFixed(2)} • ${report.merchant}</div>
                        <div class="text-xs text-gray-500">${new Date(report.timestamp).toLocaleString()}</div>
                    </button>
                `;
            }).join('');
        }

        async function selectReport(filename) {
            try {
                const response = await fetch('/api/report/' + filename);
                const data = await response.json();
                selectedReport = data;
                renderReportDetails(data);
            } catch (error) {
                console.error('Error fetching report:', error);
            }
        }

        function renderReportDetails(report) {
            const panel = document.getElementById('details-panel');
            const data = report.data;
            const ccDisplay = data.cc_num ? data.cc_num.toString().slice(-4) : '0000';
            
            const actionButtons = !report.reviewed && report.in_queue ? `
                <div class="flex gap-3">
                    <button onclick="submitFeedback(true)" class="flex-1 px-6 py-3 bg-red-600 text-white rounded-lg hover:bg-red-700 transition-colors font-bold">✗ CONFIRM FRAUD</button>
                    <button onclick="submitFeedback(false)" class="flex-1 px-6 py-3 bg-green-600 text-white rounded-lg hover:bg-green-700 transition-colors font-bold">✓ MARK LEGITIMATE</button>
                </div>
            ` : report.reviewed ? '<div class="px-4 py-3 rounded-lg font-bold text-center bg-green-100 text-green-700">✓ Reviewed</div>' : '<div class="px-4 py-3 bg-yellow-100 text-yellow-800 rounded-lg text-sm text-center">⚠️ Outside active queue</div>';
            
            panel.innerHTML = `
                <div class="flex flex-col h-full">
                    <div class="p-6 border-b border-gray-200 bg-gradient-to-r from-gray-50 to-white">
                        <div class="flex items-start justify-between mb-4">
                            <div>
                                <div class="flex items-center gap-3 mb-2">
                                    <span class="px-3 py-1 rounded-lg text-sm font-bold text-white ${getTierBadge(data.tier)}">TIER ${data.tier}</span>
                                    <span class="px-3 py-1 rounded-lg text-sm font-bold border-2 ${getRiskColor(data.risk_score)}">RISK: ${data.risk_score}/100</span>
                                    ${data.actual_fraud === 1 ? '<span class="px-3 py-1 bg-red-600 text-white rounded-lg text-sm font-bold">CONFIRMED FRAUD</span>' : ''}
                                </div>
                                <h2 class="text-2xl font-bold text-gray-900">Transaction ${data.trans_num}</h2>
                                <p class="text-gray-600">Card: ****-****-****-${ccDisplay}</p>
                            </div>
                            <a href="${report.pdf_url}" target="_blank" class="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors">View PDF</a>
                        </div>
                        ${actionButtons}
                    </div>
                    <div class="flex-1 overflow-y-auto p-6">
                        <div class="space-y-6">
                            <div>
                                <h3 class="text-lg font-bold text-gray-900 mb-3">Transaction Details</h3>
                                <div class="grid grid-cols-2 gap-4 bg-gray-50 p-4 rounded-lg">
                                    <div><div class="text-sm text-gray-600">Amount</div><div class="text-xl font-bold">$${data.amt.toFixed(2)}</div></div>
                                    <div><div class="text-sm text-gray-600">ML Score</div><div class="text-xl font-bold">${data.ml_score}</div></div>
                                    <div><div class="text-sm text-gray-600">Merchant</div><div class="font-semibold">${data.merchant}</div></div>
                                    <div><div class="text-sm text-gray-600">Category</div><div class="font-semibold">${data.category}</div></div>
                                    <div><div class="text-sm text-gray-600">Location</div><div class="font-semibold">${data.location}</div></div>
                                    <div><div class="text-sm text-gray-600">Distance</div><div class="font-semibold">${data.distance} km</div></div>
                                </div>
                            </div>
                            <div>
                                <h3 class="text-lg font-bold text-gray-900 mb-3">Customer Information</h3>
                                <div class="grid grid-cols-2 gap-4 bg-gray-50 p-4 rounded-lg">
                                    <div><div class="text-sm text-gray-600">Name</div><div class="font-semibold">${data.first} ${data.last}</div></div>
                                    <div><div class="text-sm text-gray-600">Job</div><div class="font-semibold">${data.job}</div></div>
                                    <div class="col-span-2"><div class="text-sm text-gray-600">Address</div><div class="font-semibold">${data.street}, ${data.city}, ${data.state} ${data.zip}</div></div>
                                </div>
                            </div>
                            <div>
                                <h3 class="text-lg font-bold text-gray-900 mb-3">Fraud Indicators</h3>
                                <div class="bg-red-50 border border-red-200 p-4 rounded-lg">
                                    <div class="text-sm font-mono text-gray-700 break-all">${data.reasons}</div>
                                    <div class="mt-3 text-sm text-gray-600"><strong>Confidence:</strong> ${data.confidence}%</div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            `;
        }

        async function submitFeedback(isFraud) {
            if (!selectedReport) return;
            
            try {
                const response = await fetch('/api/feedback', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({
                        filename: selectedReport.filename,
                        is_fraud: isFraud ? 1 : 0,
                        trans_num: selectedReport.data.trans_num,
                        cc_num: selectedReport.data.cc_num.toString()
                    })
                });

                if (response.ok) {
                    await fetchQueue();
                    alert(isFraud ? '✓ Marked as FRAUD' : '✓ Marked as LEGITIMATE');
                    
                    const queueData = await fetch('/api/queue').then(r => r.json());
                    const nextReport = queueData.queue.find(r => !r.reviewed);
                    if (nextReport) {
                        setTimeout(() => selectReport(nextReport.filename), 500);
                    }
                }
            } catch (error) {
                console.error('Error submitting feedback:', error);
                alert('Failed to submit feedback');
            }
        }

        setInterval(() => { if (autoRefresh) fetchQueue(); }, 5000);
        fetchQueue();
        document.getElementById('refresh-btn').addEventListener('click', fetchQueue);
    </script>
</body>
</html>
"""
    return HTMLResponse(content=html_content)


@app.get("/api/queue")
async def get_queue():
    """Get current review queue with stats"""
    scan_reports()
    
    queue_data = []
    for report in review_queue:
        pdf_path = Path(report['filepath'])
        data = parse_alert_json_from_pdf(pdf_path)
        
        queue_data.append({
            'filename': report['filename'],
            'timestamp': datetime.fromtimestamp(report['timestamp']).isoformat(),
            'cc_num': data.get('cc_num', report['cc_num']),
            'amt': data.get('amt', 0.0),
            'merchant': data.get('merchant', 'Unknown'),
            'risk_score': data.get('risk_score', 0),
            'tier': data.get('tier', 0),
            'reviewed': report['reviewed']
        })
    
    return {
        'queue': queue_data,
        'stats': review_stats,
        'queue_size': QUEUE_SIZE
    }


@app.get("/api/report/{filename}")
async def get_report(filename: str):
    """Get detailed report data"""
    pdf_path = REPORTS_DIR / filename
    
    if not pdf_path.exists():
        raise HTTPException(status_code=404, detail="Report not found")
    
    data = parse_alert_json_from_pdf(pdf_path)
    in_queue = any(r['filename'] == filename for r in review_queue)
    
    return {
        'filename': filename,
        'data': data,
        'pdf_url': f"/api/pdf/{filename}",
        'reviewed': filename in reviewed_reports,
        'in_queue': in_queue
    }


@app.get("/api/pdf/{filename}")
async def get_pdf(filename: str):
    """Serve PDF file"""
    pdf_path = REPORTS_DIR / filename
    
    if not pdf_path.exists():
        raise HTTPException(status_code=404, detail="PDF not found")
    
    return FileResponse(pdf_path, media_type="application/pdf")


@app.post("/api/feedback")
async def submit_feedback(feedback: FeedbackRequest):
    """Submit fraud/legitimate feedback"""
    global review_stats, reviewed_reports
    
    reviewed_reports.add(feedback.filename)
    review_stats['reviewed'] += 1
    
    if feedback.is_fraud == 1:
        review_stats['fraud'] += 1
    else:
        review_stats['legitimate'] += 1
    
    save_stats()
    
    print(f"Feedback: {feedback.filename} -> {'FRAUD' if feedback.is_fraud else 'LEGITIMATE'}")
    
    scan_reports()
    
    return {
        'success': True,
        'message': 'Feedback recorded',
        'stats': review_stats
    }


if __name__ == "__main__":
    import uvicorn
    
    print("=" * 60)
    print("  FRAUD REPORT REVIEW SYSTEM (FIXED - NO MOCK DATA)")
    print("=" * 60)
    print(f"  Reports directory: {REPORTS_DIR}")
    print(f"  Queue size: {QUEUE_SIZE}")
    print(f"  Starting server on http://localhost:8000")
    print("=" * 60)
    
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")