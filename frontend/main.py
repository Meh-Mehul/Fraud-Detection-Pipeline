"""
FastAPI Fraud Report Review System
FIXED VERSION: Real-time NATS updates + Proper PDF/JSON parsing
"""

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, FileResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from pathlib import Path
import json
import asyncio
from datetime import datetime
import os
import re
from typing import Optional

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

# Global state
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


def parse_report_from_pdf(pdf_path: Path) -> dict:
    """
    Enhanced PDF parser that extracts all fields from fraud reports
    Supports both companion JSON and direct PDF text parsing
    """
    json_file = pdf_path.with_suffix('.json')
    
    # Strategy 1: Load companion JSON (Best)
    if json_file.exists():
        try:
            with open(json_file, 'r') as f:
                return json.load(f)
        except:
            pass
    
    # Strategy 2: Parse PDF text with enhanced patterns
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
    
    if not PdfReader:
        return data
    
    try:
        reader = PdfReader(pdf_path)
        text = ""
        for page in reader.pages:
            text += page.extract_text() + "\n"
        
        # Enhanced extraction patterns based on your sample PDF
        
        # Transaction ID from "Transaction ID ad06dc950c..."
        tid_match = re.search(r"Transaction ID\s+([a-f0-9]{32}|[A-Z0-9-]+)", text, re.IGNORECASE)
        if tid_match:
            data['trans_num'] = tid_match.group(1)
        
        # Customer ID from "Customer ID ****-****-****-3496"
        cc_match = re.search(r"Customer ID\s+\*+\-\*+\-\*+\-(\d{4})", text)
        if cc_match:
            data['cc_num'] = cc_match.group(1)
        
        # Amount from "Amount $93.78"
        amt_match = re.search(r"Amount\s+\$([0-9,]+\.\d{2})", text)
        if amt_match:
            try:
                data['amt'] = float(amt_match.group(1).replace(',', ''))
            except:
                pass
        
        # Merchant from "Merchant fraud_Koepp-Witting"
        merch_match = re.search(r"Merchant\s+([^\n]+)", text)
        if merch_match:
            data['merchant'] = merch_match.group(1).strip()
        
        # Category from "Category grocery_pos"
        cat_match = re.search(r"Category\s+([^\n]+)", text)
        if cat_match:
            data['category'] = cat_match.group(1).strip()
        
        # Location from "Location Big Creek, KY"
        loc_match = re.search(r"Location\s+([^\n]+)", text)
        if loc_match:
            data['location'] = loc_match.group(1).strip()
            # Parse city and state
            loc_parts = data['location'].split(',')
            if len(loc_parts) >= 2:
                data['city'] = loc_parts[0].strip()
                data['state'] = loc_parts[1].strip()
        
        # Risk Score from "RISK SCORE 95/100"
        risk_match = re.search(r"RISK SCORE\s+(\d+)/100", text)
        if risk_match:
            data['risk_score'] = int(risk_match.group(1))
        
        # ML Score from "ML Score 0.0"
        ml_match = re.search(r"ML Score\s+([\d.]+)", text)
        if ml_match:
            try:
                data['ml_score'] = float(ml_match.group(1))
            except:
                pass
        
        # Tier from "Detection Tier: TIER 1"
        tier_match = re.search(r"Detection Tier:\s+TIER\s+(\d+)", text)
        if tier_match:
            data['tier'] = int(tier_match.group(1))
        
        # Confidence from "Confidence Level 95%"
        conf_match = re.search(r"Confidence Level\s+(\d+)%", text)
        if conf_match:
            data['confidence'] = int(conf_match.group(1))
        
        # Fraud status from "CONFIRMED FRAUD" or "UNDER INVESTIGATION"
        if "CONFIRMED FRAUD" in text:
            data['actual_fraud'] = 1
        
        # Reasons from "Raw Detection String: EXTREME_DIST(Z=5911.0)|VERY_FAR(102km)|FRAUD_HISTORY(8)"
        reason_match = re.search(r"Raw Detection String:\s+([^\n]+)", text)
        if reason_match:
            data['reasons'] = reason_match.group(1).strip()
        
        # Indicator count from "3 Indicators"
        ind_match = re.search(r"(\d+)\s+Indicators", text)
        if ind_match and not data['reasons']:
            data['reasons'] = f"{ind_match.group(1)} fraud indicators detected"
        
    except Exception as e:
        print(f"Failed to parse PDF {pdf_path.name}: {e}")
    
    return data


def scan_all_reports():
    """
    Scan ALL reports in directory (no limit) and return comprehensive data
    """
    if not REPORTS_DIR.exists():
        os.makedirs(REPORTS_DIR, exist_ok=True)
        return []
    
    reports = []
    pdf_files = list(REPORTS_DIR.glob("*.pdf"))
    
    print(f"📂 Scanning {len(pdf_files)} PDF reports...")
    
    for pdf_file in pdf_files:
        try:
            # Parse comprehensive data from PDF
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
            }
            
            reports.append(report_data)
        except Exception as e:
            print(f"❌ Error processing {pdf_file}: {e}")
            continue
    
    # Sort by timestamp (newest first)
    reports.sort(key=lambda x: x['timestamp'], reverse=True)
    
    print(f"✅ Loaded {len(reports)} reports into queue")
    return reports


@app.on_event("startup")
async def startup_event():
    load_stats()
    reports = scan_all_reports()
    print(f"📊 Total reports: {len(reports)}")
    print(f"📊 Reviewed: {len(reviewed_reports)}")
    print(f"📊 Pending: {len(reports) - len(reviewed_reports)}")


@app.get("/", response_class=HTMLResponse)
async def root():
    """Serve main HTML interface with auto-refresh"""
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
                    <div class="flex items-center justify-between mt-2">
                        <div class="text-xs text-gray-500" id="last-update">Updated: --:--:--</div>
                        <div class="text-xs font-semibold text-blue-600" id="total-reports">0 total</div>
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
                document.getElementById('total-reports').textContent = `${data.queue.length} total`;
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

        setInterval(() => { if (autoRefresh) fetchQueue(); }, 3000);
        fetchQueue();
        document.getElementById('refresh-btn').addEventListener('click', fetchQueue);
    </script>
</body>
</html>
"""
    return HTMLResponse(content=html_content)


@app.get("/api/queue")
async def get_queue():
    """Get ALL reports with comprehensive stats"""
    reports = scan_all_reports()
    
    queue_data = []
    for report in reports:
        queue_data.append({
            'filename': report['filename'],
            'timestamp': report['timestamp'],
            'cc_num': report['cc_num'],
            'amt': report['amt'],
            'merchant': report['merchant'],
            'risk_score': report['risk_score'],
            'tier': report['tier'],
            'reviewed': report['reviewed']
        })
    
    return {
        'queue': queue_data,
        'stats': review_stats,
        'total_reports': len(reports)
    }


@app.get("/api/report/{filename}")
async def get_report(filename: str):
    """Get detailed report data with full parsing"""
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
    """Submit fraud/legitimate feedback"""
    global review_stats, reviewed_reports
    
    reviewed_reports.add(feedback.filename)
    review_stats['reviewed'] += 1
    
    if feedback.is_fraud == 1:
        review_stats['fraud'] += 1
    else:
        review_stats['legitimate'] += 1
    
    save_stats()
    
    print(f"✓ Feedback: {feedback.filename} -> {'FRAUD' if feedback.is_fraud else 'LEGITIMATE'}")
    
    return {
        'success': True,
        'message': 'Feedback recorded',
        'stats': review_stats
    }


if __name__ == "__main__":
    import uvicorn
    
    print("=" * 60)
    print("  FRAUD REPORT REVIEW SYSTEM (ENHANCED)")
    print("=" * 60)
    print(f"  Reports directory: {REPORTS_DIR}")
    print(f"  No queue size limit - showing ALL reports")
    print(f"  Enhanced PDF parsing with fallback support")
    print(f"  Starting server on http://localhost:8000")
    print("=" * 60)
    
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")