"""
FastAPI Fraud Report Review System
Enhanced Version: Diverse queue of 10 reports + improved UI
"""

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from pathlib import Path
import json
from datetime import datetime
import re
from typing import Optional, List, Set

# PDF Parser
try:
    from pypdf import PdfReader
except ImportError:
    print("WARNING: 'pypdf' not installed. PDF parsing will be limited.")
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
STATS_FILE = Path("./review_stats.json")
QUEUE_FILE = Path("./frontend_queue.json")
NEGATIVE_FILE = Path("./negative_transactions.json")
MAX_QUEUE_SIZE = 10

# Global state
review_stats = {"reviewed": 0, "fraud": 0, "legitimate": 0}
reviewed_reports = set()
frontend_queue = []  # List of 10 diverse reports
all_reports_cache = []  # All reports for background training


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


def load_queue():
    """Load frontend queue from file"""
    global frontend_queue
    if QUEUE_FILE.exists():
        with open(QUEUE_FILE, 'r') as f:
            frontend_queue = json.load(f)


def save_queue():
    """Save frontend queue to file"""
    with open(QUEUE_FILE, 'w') as f:
        json.dump(frontend_queue, f, indent=2)


def get_indicator_signature(reasons: str) -> str:
    """Extract unique pattern signature from reasons string"""
    if not reasons:
        return "UNKNOWN"
    parts = reasons.split('|')
    # Extract just the base indicator codes
    bases = sorted([p.split('(')[0] for p in parts])
    return "|".join(bases)


def parse_report_from_pdf(pdf_path: Path) -> dict:
    """Enhanced PDF parser that extracts all fields from fraud reports"""
    json_file = pdf_path.with_suffix('.json')
    
    # Strategy 1: Load companion JSON (Best)
    if json_file.exists():
        try:
            with open(json_file, 'r') as f:
                return json.load(f)
        except:
            pass
    
    # Strategy 2: Parse PDF text
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
        'actual_fraud': 0,
        'first': "", 'last': "", 'job': "",
        'street': "", 'city': "", 'state': "", 'zip': "",
    }
    
    if not PdfReader:
        return data
    
    try:
        reader = PdfReader(pdf_path)
        text = ""
        for page in reader.pages:
            text += page.extract_text() + "\n"
        
        # Extract fields using regex patterns
        tid_match = re.search(r"Transaction ID\s+([a-f0-9]{32}|[A-Z0-9-]+)", text, re.IGNORECASE)
        if tid_match:
            data['trans_num'] = tid_match.group(1)
        
        cc_match = re.search(r"Customer ID\s+\*+\-\*+\-\*+\-(\d{4})", text)
        if cc_match:
            data['cc_num'] = cc_match.group(1)
        
        amt_match = re.search(r"Amount\s+\$([0-9,]+\.\d{2})", text)
        if amt_match:
            try:
                data['amt'] = float(amt_match.group(1).replace(',', ''))
            except:
                pass
        
        merch_match = re.search(r"Merchant\s+([^\n]+)", text)
        if merch_match:
            data['merchant'] = merch_match.group(1).strip()
        
        cat_match = re.search(r"Category\s+([^\n]+)", text)
        if cat_match:
            data['category'] = cat_match.group(1).strip()
        
        loc_match = re.search(r"Location\s+([^\n]+)", text)
        if loc_match:
            data['location'] = loc_match.group(1).strip()
            loc_parts = data['location'].split(',')
            if len(loc_parts) >= 2:
                data['city'] = loc_parts[0].strip()
                data['state'] = loc_parts[1].strip()
        
        risk_match = re.search(r"RISK SCORE\s+(\d+)/100", text)
        if risk_match:
            data['risk_score'] = int(risk_match.group(1))
        
        tier_match = re.search(r"Detection Tier:\s+TIER\s+(\d+)", text)
        if tier_match:
            data['tier'] = int(tier_match.group(1))
        
        conf_match = re.search(r"Confidence Level\s+(\d+)%", text)
        if conf_match:
            data['confidence'] = int(conf_match.group(1))
        
        if "CONFIRMED FRAUD" in text:
            data['actual_fraud'] = 1
        
        reason_match = re.search(r"Raw Detection String:\s+([^\n]+)", text)
        if reason_match:
            data['reasons'] = reason_match.group(1).strip()
        
    except Exception as e:
        print(f"Failed to parse PDF {pdf_path.name}: {e}")
    
    return data


def build_diverse_queue(all_reports: List[dict]) -> List[dict]:
    """
    Build a diverse queue of up to 10 reports with unique indicator patterns
    Prioritize unreviewed reports with different fraud patterns
    """
    seen_patterns: Set[str] = set()
    diverse_queue = []
    
    # Sort: unreviewed first, then by risk score (descending), then by timestamp
    sorted_reports = sorted(
        all_reports,
        key=lambda x: (
            x['reviewed'],  # False (unreviewed) comes before True
            -x['risk_score'],  # Higher risk first
            -x['timestamp']  # Newer first
        )
    )
    
    for report in sorted_reports:
        if len(diverse_queue) >= MAX_QUEUE_SIZE:
            break
        
        pattern = get_indicator_signature(report.get('reasons', ''))
        
        # Add report if pattern is new or if we haven't filled the queue yet
        if pattern not in seen_patterns:
            diverse_queue.append(report)
            seen_patterns.add(pattern)
    
    # If we still have space and haven't reached 10, add more reports
    if len(diverse_queue) < MAX_QUEUE_SIZE:
        for report in sorted_reports:
            if len(diverse_queue) >= MAX_QUEUE_SIZE:
                break
            if report not in diverse_queue:
                diverse_queue.append(report)
    
    return diverse_queue


def scan_all_reports():
    """Scan ALL reports and update both cache and queue"""
    global all_reports_cache, frontend_queue
    
    if not REPORTS_DIR.exists():
        REPORTS_DIR.mkdir(exist_ok=True)
        return []
    
    reports = []
    pdf_files = list(REPORTS_DIR.glob("*.pdf"))
    
    print(f"📂 Scanning {len(pdf_files)} PDF reports...")
    
    for pdf_file in pdf_files:
        try:
            data = parse_report_from_pdf(pdf_file)
            timestamp = pdf_file.stat().st_mtime
            
            report_data = {
                'filename': pdf_file.name,
                'filepath': str(pdf_file),
                'timestamp': timestamp,
                'cc_num': data.get('cc_num', '0000'),
                'amt': data.get('amt', 0.0),
                'merchant': data.get('merchant', 'Unknown'),
                'risk_score': data.get('risk_score', 0),
                'tier': data.get('tier', 1),
                'reviewed': pdf_file.name in reviewed_reports,
                'trans_num': data.get('trans_num', 'UNKNOWN'),
                'category': data.get('category', 'unknown'),
                'location': data.get('location', 'Unknown'),
                'reasons': data.get('reasons', ''),
            }
            
            reports.append(report_data)
        except Exception as e:
            print(f"❌ Error processing {pdf_file}: {e}")
            continue
    
    # Update cache with all reports
    all_reports_cache = reports
    
    # Build diverse queue for frontend
    frontend_queue = build_diverse_queue(reports)
    save_queue()
    
    print(f"✅ Total reports: {len(reports)}")
    print(f"✅ Diverse queue: {len(frontend_queue)} reports")
    
    return reports


@app.on_event("startup")
async def startup_event():
    load_stats()
    scan_all_reports()
    print(f"📊 Total reports: {len(all_reports_cache)}")
    print(f"📊 Frontend queue: {len(frontend_queue)}")
    print(f"📊 Reviewed: {len(reviewed_reports)}")


@app.get("/", response_class=HTMLResponse)
async def root():
    """Serve main HTML interface - UPDATED UI"""
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
                        <p class="text-gray-300 text-sm">Diverse Case Review System</p>
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
                    <a href="http://localhost:3000/d/fraud-detection-pipeline/fraud-detection-pipeline-latency-and-metrics?orgId=1&from=now-15m&to=now&timezone=browser&refresh=10s" target="_blank" class="flex items-center gap-2 px-4 py-2 bg-orange-500 hover:bg-orange-600 text-white rounded-lg font-semibold transition-colors">
                        <svg class="w-5 h-5" viewBox="0 0 24 24" fill="currentColor">
                            <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-1 17.93c-3.95-.49-7-3.85-7-7.93 0-.62.08-1.21.21-1.79L9 15v1c0 1.1.9 2 2 2v1.93zm6.9-2.54c-.26-.81-1-1.39-1.9-1.39h-1v-3c0-.55-.45-1-1-1H8v-2h2c.55 0 1-.45 1-1V7h2c1.1 0 2-.9 2-2v-.41c2.93 1.19 5 4.06 5 7.41 0 2.08-.8 3.97-2.1 5.39z"/>
                        </svg>
                        Grafana
                    </a>
                </div>
            </div>
        </div>
    </div>

    <!-- Tab Navigation -->
    <div class="max-w-7xl mx-auto px-6 pt-4">
        <div class="flex gap-2 border-b border-gray-200">
            <button id="tab-alerts" onclick="switchTab('alerts')" class="px-6 py-3 font-semibold text-red-600 border-b-2 border-red-600 bg-white rounded-t-lg">
                🚨 Fraud Alerts
            </button>
            <button id="tab-negatives" onclick="switchTab('negatives')" class="px-6 py-3 font-semibold text-gray-500 hover:text-gray-700 border-b-2 border-transparent">
                🔍 False Negative Review
            </button>
        </div>
    </div>

    <!-- Tab Content: Fraud Alerts (Original) -->
    <div id="content-alerts" class="max-w-7xl mx-auto px-6 py-6">
        <div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
            <div class="lg:col-span-1 bg-white rounded-lg shadow-md border border-gray-200">
                <div class="p-4 border-b border-gray-200 bg-gray-50">
                    <div class="flex items-center justify-between">
                        <div class="flex items-center gap-2">
                            <svg class="w-5 h-5 text-blue-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z"/>
                            </svg>
                            <h2 class="font-bold text-gray-900">Diverse Review Queue</h2>
                            <span class="px-2 py-1 bg-blue-100 text-blue-700 rounded-full text-xs font-semibold" id="queue-count">0</span>
                        </div>
                        <button id="refresh-btn" class="p-2 rounded-lg bg-green-100 text-green-700 hover:bg-green-200 transition-colors">
                            <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"/>
                            </svg>
                        </button>
                    </div>
                    <div class="mt-2 text-xs text-gray-500">
                        Showing 10 diverse fraud patterns • <span id="last-update">Updated: --:--:--</span>
                    </div>
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
                    <p class="text-sm">Choose from 10 diverse fraud patterns</p>
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
                        <div class="text-xs text-gray-500">${new Date(report.timestamp * 1000).toLocaleString()}</div>
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
            
            const actionButtons = !report.reviewed ? `
                <div class="flex gap-3">
                    <button onclick="submitFeedback(true)" class="flex-1 px-6 py-3 bg-red-600 text-white rounded-lg hover:bg-red-700 transition-colors font-bold">✗ CONFIRM FRAUD</button>
                    <button onclick="submitFeedback(false)" class="flex-1 px-6 py-3 bg-green-600 text-white rounded-lg hover:bg-green-700 transition-colors font-bold">✓ MARK LEGITIMATE</button>
                </div>
            ` : '<div class="px-4 py-3 rounded-lg font-bold text-center bg-green-100 text-green-700">✓ Reviewed</div>';
            
            // Parse fraud indicators
            const indicators = data.reasons ? data.reasons.split('|') : [];
            const indicatorsList = indicators.map(ind => {
                const parts = ind.split('(');
                const base = parts[0];
                const value = parts.length > 1 ? parts[1].replace(')', '') : '';
                
                // Decode indicators into readable descriptions
                const descriptions = {
                    'EXTREME_DIST': 'Extreme distance from home location',
                    'VERY_FAR': 'Transaction very far from usual locations',
                    'FRAUD_HISTORY': 'Account has history of fraudulent activity',
                    'HIGH_MERCH_RISK': 'High-risk merchant location',
                    'HIGH_CAT_RISK': 'High-risk transaction category',
                    'LATE_NIGHT': 'Transaction during unusual hours (1-5 AM)',
                    'HUGE_AMT': 'Transaction amount significantly above average',
                    'EXTREME_AMT': 'Extreme transaction amount detected',
                    'ONLINE': 'Online transaction with elevated risk',
                    'ML_HIGH': 'Machine learning model detected high fraud probability',
                    'ML_MODERATE': 'Machine learning model detected moderate risk'
                };
                
                const desc = descriptions[base] || base;
                return `<div class="flex items-start gap-2 p-2 bg-red-50 rounded border-l-4 border-red-500">
                    <span class="text-red-600 font-bold text-lg">•</span>
                    <div class="flex-1">
                        <div class="font-semibold text-gray-900">${desc}</div>
                        ${value ? `<div class="text-xs text-gray-600 mt-1">Value: ${value}</div>` : ''}
                    </div>
                </div>`;
            }).join('');
            
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
                                    <div><div class="text-sm text-gray-600">Confidence</div><div class="text-xl font-bold">${data.confidence}%</div></div>
                                    <div><div class="text-sm text-gray-600">Merchant</div><div class="font-semibold">${data.merchant}</div></div>
                                    <div><div class="text-sm text-gray-600">Category</div><div class="font-semibold">${data.category}</div></div>
                                    <div class="col-span-2"><div class="text-sm text-gray-600">Location</div><div class="font-semibold">${data.location}</div></div>
                                </div>
                            </div>
                            <div>
                                <h3 class="text-lg font-bold text-gray-900 mb-3">Fraud Indicators (${indicators.length})</h3>
                                <div class="space-y-2">
                                    ${indicatorsList || '<p class="text-gray-500">No specific indicators detected</p>'}
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
    
    # Add second tab content (False Negative Review)
    negative_tab_html = """
    <!-- Tab Content: False Negative Review -->
    <div id="content-negatives" class="max-w-7xl mx-auto px-6 py-6 hidden">
        <div class="bg-white rounded-lg shadow-md border border-gray-200">
            <div class="p-4 border-b border-gray-200 bg-yellow-50">
                <div class="flex items-center justify-between">
                    <div class="flex items-center gap-2">
                        <svg class="w-6 h-6 text-yellow-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"/>
                        </svg>
                        <h2 class="text-xl font-bold text-gray-900">False Negative Review</h2>
                        <span class="px-3 py-1 bg-yellow-200 text-yellow-800 rounded-full text-sm font-semibold" id="negative-count">0</span>
                    </div>
                    <button onclick="fetchNegatives()" class="px-4 py-2 bg-yellow-500 text-white rounded-lg hover:bg-yellow-600 font-semibold">
                        🔄 Refresh
                    </button>
                </div>
                <p class="text-sm text-gray-600 mt-2">Review transactions marked as legitimate - they might be missed frauds (false negatives)</p>
            </div>
            <div class="p-4 max-h-[600px] overflow-y-auto">
                <table class="w-full text-sm">
                    <thead class="bg-gray-100 sticky top-0">
                        <tr>
                            <th class="p-2 text-left">Trans #</th>
                            <th class="p-2 text-left">Amount</th>
                            <th class="p-2 text-left">Merchant</th>
                            <th class="p-2 text-left">Category</th>
                            <th class="p-2 text-left">ML Score</th>
                            <th class="p-2 text-left">Time</th>
                            <th class="p-2 text-center">Action</th>
                        </tr>
                    </thead>
                    <tbody id="negatives-table">
                        <tr><td colspan="7" class="p-4 text-center text-gray-500">Loading negative transactions...</td></tr>
                    </tbody>
                </table>
            </div>
        </div>
    </div>

    <script>
        function switchTab(tab) {
            const alertsTab = document.getElementById('tab-alerts');
            const negativesTab = document.getElementById('tab-negatives');
            const alertsContent = document.getElementById('content-alerts');
            const negativesContent = document.getElementById('content-negatives');
            
            if (tab === 'alerts') {
                alertsTab.className = 'px-6 py-3 font-semibold text-red-600 border-b-2 border-red-600 bg-white rounded-t-lg';
                negativesTab.className = 'px-6 py-3 font-semibold text-gray-500 hover:text-gray-700 border-b-2 border-transparent';
                alertsContent.classList.remove('hidden');
                negativesContent.classList.add('hidden');
            } else {
                alertsTab.className = 'px-6 py-3 font-semibold text-gray-500 hover:text-gray-700 border-b-2 border-transparent';
                negativesTab.className = 'px-6 py-3 font-semibold text-yellow-600 border-b-2 border-yellow-600 bg-white rounded-t-lg';
                alertsContent.classList.add('hidden');
                negativesContent.classList.remove('hidden');
                fetchNegatives();
            }
        }
        
        async function fetchNegatives() {
            try {
                const response = await fetch('/api/negatives');
                const data = await response.json();
                
                document.getElementById('negative-count').textContent = data.count || 0;
                
                const tbody = document.getElementById('negatives-table');
                if (!data.transactions || data.transactions.length === 0) {
                    tbody.innerHTML = '<tr><td colspan="7" class="p-4 text-center text-gray-500">No negative transactions collected yet. Start the negative_collector.py script.</td></tr>';
                    return;
                }
                
                tbody.innerHTML = data.transactions.slice(0, 100).map(txn => `
                    <tr class="border-b hover:bg-gray-50">
                        <td class="p-2 font-mono text-xs">${txn.trans_num}</td>
                        <td class="p-2 font-bold">$${parseFloat(txn.amt).toFixed(2)}</td>
                        <td class="p-2">${txn.merchant}</td>
                        <td class="p-2">${txn.category}</td>
                        <td class="p-2"><span class="px-2 py-1 rounded ${parseFloat(txn.ml_score) > 30 ? 'bg-yellow-100 text-yellow-800' : 'bg-green-100 text-green-800'}">${parseFloat(txn.ml_score).toFixed(1)}%</span></td>
                        <td class="p-2 text-xs text-gray-500">${txn.timestamp}</td>
                        <td class="p-2 text-center">
                            <button onclick="markNegativeAsFraud('${txn.trans_num}', '${txn.cc_num}')" class="px-3 py-1 bg-red-500 text-white rounded hover:bg-red-600 text-xs font-semibold">
                                🚨 Fraud
                            </button>
                        </td>
                    </tr>
                `).join('');
            } catch (error) {
                console.error('Error fetching negatives:', error);
            }
        }
        
        async function markNegativeAsFraud(transNum, ccNum) {
            if (!confirm('Mark this transaction as FRAUD? This will send feedback to train the model.')) return;
            
            try {
                const response = await fetch('/api/negative-feedback', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({
                        trans_num: transNum,
                        cc_num: ccNum,
                        is_fraud: 1
                    })
                });
                
                if (response.ok) {
                    alert('✓ Marked as FALSE NEGATIVE - Feedback sent for training!');
                    fetchNegatives();
                }
            } catch (error) {
                console.error('Error submitting negative feedback:', error);
            }
        }
    </script>
"""
    
    # Insert the negative tab before </body>
    html_content = html_content.replace('</body>', negative_tab_html + '</body>')
    
    return HTMLResponse(content=html_content)


@app.get("/api/queue")
async def get_queue():
    """Get diverse queue of 10 reports - auto-rescans for new reports"""
    # Auto-rescan for new reports
    scan_all_reports()
    return {
        'queue': frontend_queue,
        'stats': review_stats,
        'total_reports': len(all_reports_cache)
    }


@app.post("/api/refresh")
async def refresh_queue():
    """Rescan reports directory and rebuild queue"""
    scan_all_reports()
    return {
        'success': True,
        'queue_size': len(frontend_queue),
        'total_reports': len(all_reports_cache)
    }


@app.get("/api/report/{filename}")
async def get_report(filename: str):
    """Get detailed report data"""
    pdf_path = REPORTS_DIR / filename
    
    if not pdf_path.exists():
        raise HTTPException(status_code=404, detail="Report not found")
    
    data = parse_report_from_pdf(pdf_path)
    
    return {
        'filename': filename,
        'data': data,
        'pdf_url': f"/api/pdf/{filename}",
        'reviewed': filename in reviewed_reports
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
    """Submit feedback and update queue"""
    global review_stats, reviewed_reports
    
    reviewed_reports.add(feedback.filename)
    review_stats['reviewed'] += 1
    
    if feedback.is_fraud == 1:
        review_stats['fraud'] += 1
    else:
        review_stats['legitimate'] += 1
    
    save_stats()
    
    # Rebuild queue with diverse reports
    scan_all_reports()
    
    print(f"✓ Feedback: {feedback.filename} -> {'FRAUD' if feedback.is_fraud else 'LEGITIMATE'}")
    print(f"  Queue rebuilt: {len(frontend_queue)} diverse reports")
    
    return {
        'success': True,
        'message': 'Feedback recorded',
        'stats': review_stats
    }


class NegativeFeedbackRequest(BaseModel):
    trans_num: str
    cc_num: str
    is_fraud: int


@app.get("/api/negatives")
async def get_negatives():
    """Get latest negative transactions for false negative review"""
    if not NEGATIVE_FILE.exists():
        return {'count': 0, 'transactions': [], 'message': 'No negative transactions collected yet'}
    
    try:
        with open(NEGATIVE_FILE, 'r') as f:
            data = json.load(f)
        return data
    except Exception as e:
        return {'count': 0, 'transactions': [], 'error': str(e)}


@app.post("/api/negative-feedback")
async def negative_feedback(feedback: NegativeFeedbackRequest):
    """Submit feedback for a negative transaction (potential false negative)"""
    import nats
    import asyncio
    
    try:
        nc = await nats.connect("nats://localhost:4222")
        
        feedback_data = {
            "trans_num": feedback.trans_num,
            "cc_num": feedback.cc_num,
            "is_fraud": feedback.is_fraud,
            "source": "negative_review",
            "timestamp": datetime.now().isoformat()
        }
        
        await nc.publish("fraud.feedback", json.dumps(feedback_data).encode())
        await nc.close()
        
        label = "FRAUD (False Negative!)" if feedback.is_fraud else "LEGITIMATE"
        print(f"✓ Negative Review: {feedback.trans_num} -> {label}")
        
        return {'success': True, 'message': f'Feedback recorded: {label}'}
    except Exception as e:
        return {'success': False, 'error': str(e)}


if __name__ == "__main__":
    import uvicorn
    
    print("=" * 60)
    print("  FRAUD REPORT REVIEW SYSTEM (DIVERSE QUEUE)")
    print("=" * 60)
    print(f"  Reports directory: {REPORTS_DIR}")
    print(f"  Queue size: {MAX_QUEUE_SIZE} diverse reports")
    print(f"  All other reports continue training in background")
    print(f"  Starting server on http://localhost:8000")
    print("=" * 60)
    
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")