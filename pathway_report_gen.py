"""
PATHWAY FRAUD REPORT GENERATOR - ENHANCED
Professional PDF reports for fraud alerts with comprehensive analysis

Features:
- Subscribes to fraud alert stream
- Generates bank-grade PDF investigation reports
- Comprehensive fraud indicator decoding
- Detailed risk assessment and investigation protocols
- Tracks unique fraud patterns


Requirements:
pip install pathway reportlab
"""

import pathway as pw
import os
import json
from datetime import datetime
from pathlib import Path

# Try to import ReportLab
PDF_AVAILABLE = False
try:
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import inch
    from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer, Table, 
                                    TableStyle, PageBreak, Flowable)
    from reportlab.lib import colors
    from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY, TA_RIGHT
    from reportlab.pdfgen import canvas
    PDF_AVAILABLE = True
except ImportError:
    print("⚠️  Install reportlab: pip install reportlab")


print("═══════════════════════════════════════════════════════════")
print("  INTELLIGENT FRAUD REPORT GENERATOR v4.0 (Pathway)")
print("═══════════════════════════════════════════════════════════")

if PDF_AVAILABLE:
    print("✓ PDF generation enabled (ReportLab)")
else:
    print("⚠️  PDF generation disabled - install reportlab")

print("✓ Real-time Pathway streaming")
print("✓ Comprehensive fraud analysis")
print()


# ============================================================================
# REPORT GENERATION
# ============================================================================

class HeaderFooterCanvas(canvas.Canvas):
    """Custom canvas for professional headers and footers"""
    
    def __init__(self, *args, **kwargs):
        canvas.Canvas.__init__(self, *args, **kwargs)
        self.pages = []
        
    def showPage(self):
        self.pages.append(dict(self.__dict__))
        self._startPage()
        
    def save(self):
        page_count = len(self.pages)
        for page_num, page_dict in enumerate(self.pages, 1):
            self.__dict__.update(page_dict)
            self.draw_header_footer(page_num, page_count)
            canvas.Canvas.showPage(self)
        canvas.Canvas.save(self)
        
    def draw_header_footer(self, page_num, page_count):
        """Draw professional header and footer on each page"""
        self.saveState()
        
        # Header background
        self.setFillColor(colors.HexColor('#1a1a2e'))
        self.rect(0, letter[1] - 0.75*inch, letter[0], 0.75*inch, fill=1, stroke=0)
        
        # Header text
        self.setFillColor(colors.white)
        self.setFont('Helvetica-Bold', 16)
        self.drawString(0.75*inch, letter[1] - 0.45*inch, "FRAUD INVESTIGATION REPORT")
        
        self.setFont('Helvetica', 9)
        self.setFillColor(colors.HexColor('#e0e0e0'))
        self.drawString(0.75*inch, letter[1] - 0.6*inch, "Financial Crime Investigation Unit")
        
        # Red accent line
        self.setStrokeColor(colors.HexColor('#c41e3a'))
        self.setLineWidth(3)
        self.line(0, letter[1] - 0.77*inch, letter[0], letter[1] - 0.77*inch)
        
        # Footer
        self.setStrokeColor(colors.HexColor('#c41e3a'))
        self.setLineWidth(2)
        self.line(0.75*inch, 0.7*inch, letter[0] - 0.75*inch, 0.7*inch)
        
        self.setFont('Helvetica', 8)
        self.setFillColor(colors.HexColor('#666666'))
        self.drawString(0.75*inch, 0.5*inch, 
                       f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        self.drawRightString(letter[0] - 0.75*inch, 0.5*inch, 
                            f"Page {page_num} of {page_count}")
        
        self.setFont('Helvetica-Bold', 7)
        self.setFillColor(colors.HexColor('#c41e3a'))
        conf_text = "CONFIDENTIAL - FOR AUTHORIZED PERSONNEL ONLY"
        text_width = self.stringWidth(conf_text, 'Helvetica-Bold', 7)
        self.drawString((letter[0] - text_width) / 2, 0.35*inch, conf_text)
        
        self.restoreState()


class StatusBox(Flowable):
    """Custom flowable for alert status box"""
    
    def __init__(self, status_text, status_color, width=6*inch, height=0.8*inch):
        Flowable.__init__(self)
        self.status_text = status_text
        self.status_color = status_color
        self.width = width
        self.height = height
        
    def draw(self):
        self.canv.setFillColor(self.status_color)
        self.canv.roundRect(0, 0, self.width, self.height, 8, fill=1, stroke=0)
        
        self.canv.setFillColor(colors.white)
        self.canv.setFont('Helvetica-Bold', 14)
        text_width = self.canv.stringWidth(self.status_text, 'Helvetica-Bold', 14)
        self.canv.drawString((self.width - text_width) / 2, self.height / 2 - 5, 
                            self.status_text)


class ReportGenerator:
    """Generate PDF reports from alerts"""
    
    def __init__(self):
        self.reports_dir = Path("fraud_reports")
        self.reports_dir.mkdir(exist_ok=True)
        self.seen_patterns = set()
        self.report_count = 0
        self.total_alerts = 0
        
        print(f"✓ Reports directory: {self.reports_dir}/")
        print()
    
    def get_pattern_signature(self, reasons):
        """Extract unique pattern from reasons"""
        if not reasons:
            return tuple()
        parts = reasons.split('|')
        normalized = tuple(sorted([r.split('(')[0] for r in parts]))
        return normalized
    
    def decode_fraud_indicators(self, reasons, tier):
        """Decode fraud indicators into human-readable explanations"""
        
        indicators = reasons.split('|')
        
        # Comprehensive indicator dictionary
        indicator_details = {
            # Velocity/Burst Indicators
            'EXTREME_BURST': {
                'name': 'Extreme Transaction Burst',
                'severity': 'CRITICAL',
                'description': '4+ transactions within 5 minutes - typical card testing or fraud spree',
                'risk': 'This pattern is highly indicative of automated fraud tools or stolen card testing'
            },
            'MAJOR_BURST': {
                'name': 'Major Transaction Burst',
                'severity': 'CRITICAL',
                'description': '5+ transactions within 10 minutes - indicates automated fraud or stolen card',
                'risk': 'Rapid succession suggests unauthorized access with intent to maximize damage before detection'
            },
            'BURST': {
                'name': 'Transaction Burst',
                'severity': 'HIGH',
                'description': '3+ transactions within 5 minutes - abnormal spending velocity',
                'risk': 'Human shoppers rarely complete multiple purchases this quickly'
            },
            'FastBurst': {
                'name': 'Fast Burst Pattern',
                'severity': 'HIGH',
                'description': '4+ transactions within 10 minutes - rapid succession pattern',
                'risk': 'Suggests coordinated fraud attack or card testing operation'
            },
            'Rapid': {
                'name': 'Rapid Transactions',
                'severity': 'MEDIUM',
                'description': '5+ transactions within 15 minutes - elevated transaction frequency',
                'risk': 'Above normal velocity indicating possible unauthorized use'
            },
            'Fast': {
                'name': 'Fast Transaction Pattern',
                'severity': 'MEDIUM',
                'description': '2+ transactions within 10 minutes - above normal velocity',
                'risk': 'Faster than typical customer behavior'
            },
            
            # Amount Anomalies
            'MASSIVE_AMT': {
                'name': 'Massive Amount Spike',
                'severity': 'CRITICAL',
                'description': 'Transaction 4.5+ standard deviations above customer average',
                'risk': 'Extreme deviation suggests fraudster attempting to maximize stolen card value'
            },
            'HUGE_AMT': {
                'name': 'Huge Amount Anomaly',
                'severity': 'CRITICAL',
                'description': 'Transaction 3.8+ standard deviations above norm, over $500',
                'risk': 'Significantly higher than customer norm, likely unauthorized large purchase'
            },
            'VeryHighAmt': {
                'name': 'Very High Amount',
                'severity': 'HIGH',
                'description': 'Transaction 3.5+ standard deviations above customer average',
                'risk': 'Purchase amount drastically outside normal spending pattern'
            },
            'HighAmt': {
                'name': 'High Amount',
                'severity': 'HIGH',
                'description': 'Transaction 3+ standard deviations above customer average',
                'risk': 'Unusually large transaction for this customer'
            },
            'UnusualAmt': {
                'name': 'Unusual Amount',
                'severity': 'MEDIUM',
                'description': 'Transaction 2.5+ standard deviations above norm, top 10% of customer history',
                'risk': 'In the highest tier of customer spending, warrants verification'
            },
            'NewMerch+High': {
                'name': 'New Merchant with High Amount',
                'severity': 'HIGH',
                'description': 'First-time transaction at merchant with amount over $500',
                'risk': 'Fraudsters often test stolen cards at new merchants with high-value purchases'
            },
            'RareCat+High': {
                'name': 'Rare Category with High Amount',
                'severity': 'HIGH',
                'description': 'Unusual category (< 5% of history) with transaction over $600',
                'risk': 'Customer suddenly making large purchase in unfamiliar category suggests card misuse'
            },
            
            # Distance/Location Anomalies
            'EXTREME_DIST': {
                'name': 'Extreme Distance',
                'severity': 'CRITICAL',
                'description': 'Transaction 4+ standard deviations from home location',
                'risk': 'Geographically impossible or highly unlikely location for this customer'
            },
            'VERY_FAR': {
                'name': 'Very Far Location',
                'severity': 'CRITICAL',
                'description': 'Transaction 3.5+ standard deviations from typical locations, over 100km',
                'risk': 'Purchase location far outside customer normal geographic range'
            },
            'VeryFar': {
                'name': 'Very Far Distance',
                'severity': 'HIGH',
                'description': 'Transaction 3.5+ standard deviations from customer home',
                'risk': 'Location significantly distant from customer home base'
            },
            'Far': {
                'name': 'Far Location',
                'severity': 'HIGH',
                'description': 'Transaction 3+ standard deviations from typical location range',
                'risk': 'Outside customer normal shopping area'
            },
            'UnusualDist': {
                'name': 'Unusual Distance',
                'severity': 'MEDIUM',
                'description': 'Transaction 2.5+ standard deviations from norm, top 10% distance',
                'risk': 'Farther than 90% of customer transactions'
            },
            'FarLoc': {
                'name': 'Far Location Transaction',
                'severity': 'MEDIUM',
                'description': 'Transaction significantly further than customer norm',
                'risk': 'May indicate card used by unauthorized party in different location'
            },
            
            # Merchant Risk
            'FRAUD_MERCHANT': {
                'name': 'High-Fraud Merchant',
                'severity': 'CRITICAL',
                'description': 'Merchant has 40%+ fraud rate across 50+ transactions',
                'risk': 'This merchant is a known fraud hotspot with very high historical fraud rate'
            },
            'BAD_MERCHANT': {
                'name': 'Risky Merchant',
                'severity': 'HIGH',
                'description': 'Merchant has 35%+ fraud rate across 40+ transactions',
                'risk': 'Merchant has elevated fraud risk based on historical patterns'
            },
            'RiskyMerch': {
                'name': 'Risky Merchant Category',
                'severity': 'HIGH',
                'description': 'Merchant has 30%+ fraud rate across 40+ transactions',
                'risk': 'Merchant associated with above-average fraudulent transactions'
            },
            
            # Customer History
            'FRAUD_HISTORY': {
                'name': 'Multiple Fraud History',
                'severity': 'CRITICAL',
                'description': 'Customer has 3+ confirmed previous fraud incidents',
                'risk': 'Account has been compromised multiple times - may indicate repeat targeting or vulnerability'
            },
            'REPEAT_FRAUD': {
                'name': 'Repeat Fraud Pattern',
                'severity': 'CRITICAL',
                'description': '2+ previous frauds with current high amount anomaly',
                'risk': 'Pattern matches previous fraud incidents on this account'
            },
            'PrevFraud': {
                'name': 'Previous Fraud',
                'severity': 'HIGH',
                'description': 'Customer has 2+ confirmed fraud incidents in history',
                'risk': 'Account has been compromised before, making it higher risk for repeat fraud'
            },
            
            # Pattern Breaks
            'NewMerch': {
                'name': 'New Merchant',
                'severity': 'MEDIUM',
                'description': 'First transaction ever at this merchant',
                'risk': 'Fraudsters often use stolen cards at merchants customer has never visited'
            },
            'RareCat': {
                'name': 'Rare Category',
                'severity': 'MEDIUM',
                'description': 'Unusual spending category for this customer (< 5% of history)',
                'risk': 'Purchase in category customer rarely uses'
            },
            'UnusualHour': {
                'name': 'Unusual Hour',
                'severity': 'MEDIUM',
                'description': 'Transaction at atypical time for customer (< 3% of history)',
                'risk': 'Time of day inconsistent with customer shopping habits'
            },
            'LateOnline': {
                'name': 'Late Night Online Purchase',
                'severity': 'MEDIUM',
                'description': 'Online purchase between 1-5 AM with amount over $400',
                'risk': 'Late-night online purchases are common in card-not-present fraud'
            },
            
            # Combination Patterns
            'Burst+Amt': {
                'name': 'Burst with Amount Spike',
                'severity': 'HIGH',
                'description': '3+ transactions in 10 minutes AND amount 2.5+ standard deviations above norm',
                'risk': 'Combination of velocity and amount anomalies strongly suggests fraud'
            },
            'Amt+Dist': {
                'name': 'Amount + Distance Anomaly',
                'severity': 'HIGH',
                'description': 'Both amount AND distance 2.5+ standard deviations above customer norm',
                'risk': 'Dual anomaly (location + spending) is classic fraud indicator'
            },
            
            # ML Detection
            'ML': {
                'name': 'Machine Learning Detection',
                'severity': 'VARIES',
                'description': 'ML models detected anomaly pattern',
                'risk': 'Advanced algorithms identified suspicious patterns not obvious to rule-based systems'
            }
        }
        
        decoded = []
        severity_counts = {'CRITICAL': 0, 'HIGH': 0, 'MEDIUM': 0}
        
        for indicator in indicators:
            parts = indicator.split('(')
            base = parts[0]
            value = parts[1].rstrip(')') if len(parts) > 1 else None
            
            if base in indicator_details:
                detail = indicator_details[base]
                severity_counts[detail['severity']] += 1
                
                decoded_info = {
                    'indicator': indicator,
                    'base': base,
                    'value': value,
                    'name': detail['name'],
                    'severity': detail['severity'],
                    'description': detail['description'],
                    'risk': detail['risk']
                }
                decoded.append(decoded_info)
            else:
                if base.startswith('ML'):
                    score = base[2:] if len(base) > 2 else value
                    decoded.append({
                        'indicator': indicator,
                        'base': 'ML',
                        'value': score,
                        'name': 'ML Detection',
                        'severity': 'HIGH' if score and float(score) > 80 else 'MEDIUM',
                        'description': f'Machine learning model confidence: {score}%' if score else 'ML anomaly detected',
                        'risk': 'AI detected behavioral patterns inconsistent with legitimate transactions'
                    })
                else:
                    decoded.append({
                        'indicator': indicator,
                        'base': base,
                        'value': value,
                        'name': f'Unknown: {base}',
                        'severity': 'UNKNOWN',
                        'description': 'System detected anomaly',
                        'risk': 'Requires manual investigation'
                    })
        
        # Tier explanation
        tier_info = {
            1: {
                'name': 'TIER 1 - ABSOLUTE CERTAINTY',
                'description': '2+ extreme signals OR 1 extreme signal + high ML confidence (80+%)',
                'action': 'IMMEDIATE INVESTIGATION REQUIRED - Block card and contact customer'
            },
            2: {
                'name': 'TIER 2 - STRONG EVIDENCE',
                'description': 'Score-based detection: 75+ risk points from multiple fraud indicators',
                'action': 'HIGH PRIORITY - Contact customer within 24 hours for verification'
            },
            3: {
                'name': 'TIER 3 - ML-BASED DETECTION',
                'description': 'High ML confidence (82%+) with 2+ supporting behavioral anomalies',
                'action': 'INVESTIGATION RECOMMENDED - Review and verify transaction'
            }
        }
        
        return {
            'decoded_indicators': decoded,
            'severity_summary': severity_counts,
            'tier_info': tier_info.get(tier, {'name': 'Unknown Tier', 'description': 'N/A', 'action': 'Review required'}),
            'total_indicators': len(decoded)
        }
    
    def generate_pdf(self, alert_data):
        """Generate professional PDF report with comprehensive analysis"""
        if not PDF_AVAILABLE:
            return None
        
        try:
            # Decode fraud indicators
            indicator_analysis = self.decode_fraud_indicators(alert_data['reasons'], alert_data['tier'])
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            reason_sig = ''.join(alert_data['reasons'].split('|')[0][:20].split('(')[0]) if alert_data['reasons'] else 'UNKNOWN'
            
            filename = f"FRAUD_{timestamp}_{reason_sig}_CC{str(alert_data['cc_num'])[-4:]}.pdf"
            filepath = self.reports_dir / filename
            
            doc = SimpleDocTemplate(
                str(filepath), 
                pagesize=letter,
                topMargin=1*inch,
                bottomMargin=0.9*inch,
                leftMargin=0.75*inch,
                rightMargin=0.75*inch
            )
            
            story = []
            styles = getSampleStyleSheet()
            
            # Custom styles
            section_header_style = ParagraphStyle(
                'SectionHeader',
                parent=styles['Heading2'],
                fontSize=13,
                textColor=colors.white,
                spaceAfter=12,
                spaceBefore=20,
                fontName='Helvetica-Bold',
                backColor=colors.HexColor('#1a1a2e'),
                leftIndent=10,
                rightIndent=10,
                leading=18,
                borderPadding=(8, 8, 8, 8)
            )
            
            subsection_style = ParagraphStyle(
                'SubSection',
                parent=styles['Heading3'],
                fontSize=11,
                textColor=colors.HexColor('#c41e3a'),
                spaceAfter=8,
                spaceBefore=12,
                fontName='Helvetica-Bold'
            )
            
            body_style = ParagraphStyle(
                'ReportBody',
                parent=styles['BodyText'],
                fontSize=10,
                leading=14,
                alignment=TA_JUSTIFY,
                textColor=colors.HexColor('#2a2a2a')
            )
            
            bullet_style = ParagraphStyle(
                'BulletPoint',
                parent=body_style,
                leftIndent=20,
                bulletIndent=10,
                spaceAfter=6
            )
            
            # Status box
            fraud_status = 'CONFIRMED FRAUD' if alert_data['actual_fraud'] == 1 else 'UNDER INVESTIGATION'
            status_color = colors.HexColor('#c41e3a') if alert_data['actual_fraud'] == 1 else colors.HexColor('#e67e22')
            
            story.append(Spacer(1, 0.1*inch))
            story.append(StatusBox(fraud_status, status_color))
            story.append(Spacer(1, 0.3*inch))
            
            # Metadata
            case_id = f"FR-{timestamp}-{alert_data['trans_num']}"
            
            metadata_data = [
                [Paragraph('<b>Case ID:</b>', body_style), Paragraph(case_id, body_style)],
                [Paragraph('<b>Generated:</b>', body_style), 
                 Paragraph(datetime.now().strftime('%B %d, %Y at %H:%M:%S'), body_style)],
                [Paragraph('<b>Detection Tier:</b>', body_style), 
                 Paragraph(f"<b>TIER {alert_data['tier']}</b>", body_style)],
                [Paragraph('<b>Classification:</b>', body_style), 
                 Paragraph(f'<font color="#c41e3a"><b>{fraud_status}</b></font>', body_style)],
            ]
            
            metadata_table = Table(metadata_data, colWidths=[1.8*inch, 4.2*inch])
            metadata_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#f8f9fa')),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('TOPPADDING', (0, 0), (-1, -1), 8),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
                ('BOX', (0, 0), (-1, -1), 1.5, colors.HexColor('#dee2e6')),
            ]))
            
            story.append(metadata_table)
            story.append(Spacer(1, 0.25*inch))
            
            # ===== RISK ASSESSMENT SECTION =====
            story.append(Paragraph('RISK ASSESSMENT', section_header_style))
            story.append(Spacer(1, 0.1*inch))
            
            risk_score = alert_data['risk_score']
            risk_level = 'EXTREME' if risk_score >= 90 else 'CRITICAL' if risk_score >= 80 else 'HIGH' if risk_score >= 70 else 'ELEVATED'
            risk_color = colors.HexColor('#8b0000') if risk_score >= 90 else colors.HexColor('#c41e3a') if risk_score >= 80 else colors.HexColor('#e67e22') if risk_score >= 70 else colors.HexColor('#f39c12')
            
            risk_data = [
                [Paragraph('<b>RISK SCORE</b>', body_style), 
                 Paragraph(f'<font size="20" color="{risk_color.hexval()}"><b>{risk_score}</b></font><font size="14">/100</font>', body_style),
                 Paragraph(f'<font color="{risk_color.hexval()}"><b>{risk_level} RISK</b></font>', body_style)],
                [Paragraph('<b>Confidence Level</b>', body_style), 
                 Paragraph(f'{alert_data.get("confidence", "N/A")}%', body_style),
                 Paragraph(f'<b>{indicator_analysis["total_indicators"]}</b> Indicators', body_style)],
            ]
            
            risk_table = Table(risk_data, colWidths=[2*inch, 2*inch, 2*inch])
            risk_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#fff5f5')),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('ALIGN', (1, 0), (1, 0), 'CENTER'),
                ('ALIGN', (2, 0), (2, 0), 'CENTER'),
                ('TOPPADDING', (0, 0), (-1, -1), 10),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 10),
                ('BOX', (0, 0), (-1, -1), 2, risk_color),
            ]))
            
            story.append(risk_table)
            story.append(Spacer(1, 0.15*inch))
            
            # Severity breakdown
            severity_data = [
                ['Severity Level', 'Count', 'Description'],
                [Paragraph('<font color="#8b0000">● CRITICAL</font>', body_style), 
                 str(indicator_analysis['severity_summary']['CRITICAL']),
                 'Extreme anomalies requiring immediate action'],
                [Paragraph('<font color="#e67e22">● HIGH</font>', body_style), 
                 str(indicator_analysis['severity_summary']['HIGH']),
                 'Significant deviations from normal patterns'],
                [Paragraph('<font color="#f39c12">● MEDIUM</font>', body_style), 
                 str(indicator_analysis['severity_summary']['MEDIUM']),
                 'Notable irregularities warranting review'],
            ]
            
            severity_table = Table(severity_data, colWidths=[1.5*inch, 0.8*inch, 3.7*inch])
            severity_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1a1a2e')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 9),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('TOPPADDING', (0, 0), (-1, -1), 7),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 7),
                ('BOX', (0, 0), (-1, -1), 1, colors.HexColor('#dee2e6')),
            ]))
            
            story.append(severity_table)
            story.append(Spacer(1, 0.2*inch))
            
            # Transaction details
            story.append(Paragraph('TRANSACTION DETAILS', section_header_style))
            story.append(Spacer(1, 0.1*inch))
            
            trans_data = [
                ['Field', 'Value'],
                ['Transaction ID', str(alert_data['trans_num'])],
                ['Customer ID', f"****-****-****-{str(alert_data['cc_num'])[-4:]}"],
                ['Amount', f"${alert_data['amt']:,.2f}"],
                ['Merchant', alert_data['merchant']],
                ['Category', alert_data['category']],
                ['Location', alert_data['location']],
                ['ML Score', f"{alert_data.get('ml_score', 'N/A')}"],
            ]
            
            trans_table = Table(trans_data, colWidths=[2*inch, 4*inch])
            trans_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1a1a2e')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTNAME', (0, 1), (0, -1), 'Helvetica-Bold'),
                ('BACKGROUND', (0, 1), (0, -1), colors.HexColor('#f8f9fa')),
                ('FONTSIZE', (0, 0), (-1, -1), 10),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('TOPPADDING', (0, 0), (-1, -1), 8),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
                ('BOX', (0, 0), (-1, -1), 1.5, colors.HexColor('#dee2e6')),
            ]))
            
            story.append(trans_table)
            story.append(Spacer(1, 0.25*inch))
            
            # ===== FRAUD INDICATORS =====
            story.append(PageBreak())
            story.append(Paragraph('FRAUD INDICATORS ANALYSIS', section_header_style))
            story.append(Spacer(1, 0.1*inch))
            
            story.append(Paragraph(f'<b>Raw Detection String:</b> <font face="Courier" size="8" color="#666666">{alert_data["reasons"]}</font>', body_style))
            story.append(Spacer(1, 0.15*inch))
            
            # Detailed indicators table
            indicator_headers = [
                [Paragraph('<b>Indicator</b>', body_style), 
                 Paragraph('<b>Name</b>', body_style), 
                 Paragraph('<b>Severity</b>', body_style), 
                 Paragraph('<b>Description</b>', body_style)]
            ]
            
            indicator_rows = []
            for ind in indicator_analysis['decoded_indicators']:
                severity_color = '#8b0000' if ind['severity'] == 'CRITICAL' else '#e67e22' if ind['severity'] == 'HIGH' else '#f39c12'
                indicator_rows.append([
                    Paragraph(f'<font face="Courier" size="8">{ind["indicator"]}</font>', body_style),
                    Paragraph(ind['name'], body_style),
                    Paragraph(f'<font color="{severity_color}"><b>{ind["severity"]}</b></font>', body_style),
                    Paragraph(ind['description'], body_style)
                ])
            
            indicators_table = Table(indicator_headers + indicator_rows, 
                                    colWidths=[1.1*inch, 1.5*inch, 0.8*inch, 2.6*inch])
            indicators_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#c41e3a')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 8),
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('TOPPADDING', (0, 0), (-1, -1), 6),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
                ('BOX', (0, 0), (-1, -1), 1.5, colors.HexColor('#c41e3a')),
                ('INNERGRID', (0, 1), (-1, -1), 0.5, colors.HexColor('#e0e0e0')),
            ]))
            
            story.append(indicators_table)
            story.append(Spacer(1, 0.2*inch))
            
            # Risk explanations
            story.append(Paragraph('Risk Assessment Details', subsection_style))
            for ind in indicator_analysis['decoded_indicators'][:5]:
                story.append(Paragraph(f'<b>• {ind["name"]}:</b> {ind["risk"]}', bullet_style))
            
            story.append(Spacer(1, 0.2*inch))
            
            # ===== DETECTION METHODOLOGY =====
            story.append(Paragraph('DETECTION METHODOLOGY', section_header_style))
            story.append(Spacer(1, 0.1*inch))
            
            story.append(Paragraph(f'<b>{indicator_analysis["tier_info"]["name"]}</b>', subsection_style))
            story.append(Paragraph(indicator_analysis['tier_info']['description'], body_style))
            story.append(Spacer(1, 0.1*inch))
            
            story.append(Paragraph('<b>Recommended Action:</b>', body_style))
            story.append(Paragraph(indicator_analysis['tier_info']['action'], 
                                 ParagraphStyle('ActionText', parent=body_style, 
                                              textColor=colors.HexColor('#c41e3a'),
                                              fontName='Helvetica-Bold',
                                              leftIndent=20)))
            story.append(Spacer(1, 0.2*inch))
            
            # ===== INVESTIGATION RECOMMENDATIONS =====
            story.append(PageBreak())
            story.append(Paragraph('INVESTIGATION PROTOCOL', section_header_style))
            story.append(Spacer(1, 0.1*inch))
            
            story.append(Paragraph('Immediate Actions (0-4 Hours)', subsection_style))
            immediate_actions = [
                'Attempt customer contact via all verified phone numbers and email addresses',
                'Place temporary authorization hold on card if transaction not yet settled',
                'Review account for additional suspicious transactions in past 72 hours',
                'Flag account for enhanced monitoring and velocity controls',
                'Document all attempted customer contact and system actions taken'
            ]
            for action in immediate_actions:
                story.append(Paragraph(f'• {action}', bullet_style))
            
            story.append(Spacer(1, 0.15*inch))
            story.append(Paragraph('Short-Term Actions (4-48 Hours)', subsection_style))
            short_term_actions = [
                'Conduct comprehensive transaction history analysis for behavioral patterns',
                'Verify merchant legitimacy and check merchant fraud risk profile',
                'If customer confirms fraud, initiate Regulation E claim process immediately',
                'Complete all regulatory compliance documentation requirements',
                'Escalate to specialized fraud investigation unit if pattern suggests organized crime'
            ]
            for action in short_term_actions:
                story.append(Paragraph(f'• {action}', bullet_style))
            
            story.append(Spacer(1, 0.2*inch))
            
            # Customer verification questions
            story.append(Paragraph('Customer Verification Questions', subsection_style))
            questions = [
                f'Did you authorize a transaction at <b>{alert_data["merchant"]}</b> for <b>${alert_data["amt"]:.2f}</b>?',
                f'Have you recently traveled to or been in the vicinity of <b>{alert_data["location"]}</b>?',
                'Do you currently have physical possession of your payment card?',
                'Have you shared your card number, CVV, or PIN with anyone recently?',
                'Have you noticed any other unauthorized transactions on your account?',
                'Have you received any suspicious calls, emails, or texts requesting card information?'
            ]
            for q in questions:
                story.append(Paragraph(f'• {q}', bullet_style))
            
            story.append(Spacer(1, 0.2*inch))
            
            # ===== RISK MITIGATION =====
            story.append(Paragraph('RISK MITIGATION STRATEGY', section_header_style))
            story.append(Spacer(1, 0.1*inch))
            
            mitigation_data = [
                ['Category', 'Mitigation Actions'],
                ['Account Security', 
                 '• Issue EMV chip card replacement\n• Enable real-time SMS/email transaction alerts\n• Implement step-up authentication for high-risk transactions\n• Update customer contact information if outdated'],
                ['Transaction Controls', 
                 '• Set temporary velocity limits (max 3 transactions per hour)\n• Implement geographic transaction restrictions\n• Lower daily authorization limits pending review\n• Enable merchant category blocking for high-risk categories'],
                ['Monitoring', 
                 '• Flag account for enhanced monitoring (30-day period)\n• Set alerts for similar fraud indicators or patterns\n• Monitor for account takeover indicators (password changes, contact updates)\n• Cross-reference with other accounts showing similar patterns'],
            ]
            
            mitigation_table = Table(mitigation_data, colWidths=[1.5*inch, 4.5*inch])
            mitigation_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1a1a2e')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTNAME', (0, 1), (0, -1), 'Helvetica-Bold'),
                ('BACKGROUND', (0, 1), (0, -1), colors.HexColor('#f8f9fa')),
                ('FONTSIZE', (0, 0), (-1, -1), 9),
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('TOPPADDING', (0, 0), (-1, -1), 10),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 10),
                ('BOX', (0, 0), (-1, -1), 1.5, colors.HexColor('#dee2e6')),
            ]))
            
            story.append(mitigation_table)
            story.append(Spacer(1, 0.2*inch))
            
            # ===== CASE DISPOSITION =====
            story.append(Paragraph('CASE DISPOSITION GUIDANCE', section_header_style))
            story.append(Spacer(1, 0.1*inch))
            
            disposition_scenarios = [
                ('If Fraud Confirmed', [
                    'Immediately cancel card and issue replacement with new account number',
                    'Process Regulation E claim per federal guidelines (provisional credit within 10 business days)',
                    'Update fraud databases and adjust merchant/category risk scores',
                    'File Suspicious Activity Report (SAR) if loss exceeds $5,000 or shows organized fraud patterns',
                    'Investigate for related fraudulent activity across customer base',
                    'Coordinate with law enforcement if criminal referral threshold met'
                ]),
                ('If False Positive', [
                    'Apologize for inconvenience and explain fraud detection process',
                    'Update customer behavioral profile to reduce future false alerts',
                    'Document legitimate transaction for machine learning model refinement',
                    'Consider customer feedback for detection algorithm tuning',
                    'Offer fraud prevention education and account security best practices'
                ]),
                ('If Unable to Reach Customer', [
                    'Escalate to senior fraud investigation team for enhanced review',
                    'Continue monitoring for additional suspicious activity patterns',
                    'Attempt contact through alternative channels (mailed letter, branch visit)',
                    'Consider temporary card suspension if risk score exceeds critical threshold',
                    'Document all contact attempts for regulatory compliance'
                ])
            ]
            
            for scenario_title, actions in disposition_scenarios:
                story.append(Paragraph(scenario_title, subsection_style))
                for action in actions:
                    story.append(Paragraph(f'• {action}', bullet_style))
                story.append(Spacer(1, 0.1*inch))
            
            story.append(Spacer(1, 0.2*inch))
            
            # ===== CONCLUSION =====
            story.append(Paragraph('CONCLUSION & NEXT STEPS', section_header_style))
            story.append(Spacer(1, 0.1*inch))
            
            conclusion_text = f"""This transaction has been flagged with a risk score of <b>{alert_data['risk_score']}/100</b>, 
            classified as <b>{risk_level} RISK</b>, and requires immediate attention from the fraud investigation team. 
            The detection system identified <b>{indicator_analysis['total_indicators']}</b> distinct fraud indicators, 
            including <b>{indicator_analysis['severity_summary']['CRITICAL']}</b> critical-severity anomalies.
            <br/><br/>
            <b>Case Priority:</b> <font color="#c41e3a">{'URGENT' if alert_data['risk_score'] > 85 else 'HIGH' if alert_data['risk_score'] > 70 else 'MEDIUM'}</font>
            <br/><br/>
            <b>Immediate Next Step:</b> {indicator_analysis['tier_info']['action']}
            <br/><br/>
            The combination of behavioral anomalies detected suggests {'a high probability of fraudulent activity' if alert_data['actual_fraud'] == 1 else 'potential unauthorized account access'}. 
            Prompt investigation and customer verification are essential to minimize potential losses and ensure account security.
            """
            
            story.append(Paragraph(conclusion_text, body_style))
            story.append(Spacer(1, 0.2*inch))
            
            # Final disclaimer box
            disclaimer_text = """<b>LEGAL DISCLAIMER:</b> This report is generated by automated fraud detection systems 
            and is intended for investigative purposes only. Final fraud determination requires human review, 
            customer verification, and adherence to all applicable federal regulations including Regulation E 
            (Electronic Fund Transfers) and the Fair Credit Billing Act. All customer interactions must comply 
            with the Fair Debt Collection Practices Act and institution policies regarding fraud investigation procedures."""
            
            disclaimer_style = ParagraphStyle(
                'Disclaimer',
                parent=body_style,
                fontSize=8,
                textColor=colors.HexColor('#666666'),
                alignment=TA_JUSTIFY,
                borderWidth=1,
                borderColor=colors.HexColor('#dee2e6'),
                borderPadding=10,
                backColor=colors.HexColor('#f8f9fa')
            )
            
            story.append(Paragraph(disclaimer_text, disclaimer_style))
            
            # Build PDF with custom canvas
            doc.build(story, canvasmaker=HeaderFooterCanvas)
            
            print(f"   ✓ PDF generated: {filename}")
            return str(filepath)
            
        except Exception as e:
            print(f"   ❌ Error generating PDF: {e}")
            import traceback
            traceback.print_exc()
            return None


# Global generator instance
generator = ReportGenerator()


# ============================================================================
# PATHWAY SCHEMA
# ============================================================================

class AlertSchema(pw.Schema):
    alert_json: str


# ============================================================================
# REPORT PROCESSOR UDF
# ============================================================================

@pw.udf
def process_alert(alert_json: str) -> str:
    """Process fraud alert and generate report if needed"""
    
    try:
        # Parse JSON string
        alert_data = json.loads(alert_json)
        
        # Check if it's an alert
        if not alert_data.get('is_alert', False):
            return json.dumps({'processed': False, 'reason': 'not_alert'})
        
        generator.total_alerts += 1
        
        pattern_sig = generator.get_pattern_signature(alert_data.get('reasons', ''))
        
        if pattern_sig not in generator.seen_patterns:
            generator.seen_patterns.add(pattern_sig)
            generator.report_count += 1
            
            print(f"\n{'='*60}")
            print(f"🎯 NEW FRAUD PATTERN DETECTED #{generator.report_count}")
            print(f"{'='*60}")
            print(f"Transaction: {alert_data['trans_num']}")
            print(f"Customer: ****{str(alert_data['cc_num'])[-4:]}")
            print(f"Pattern: {alert_data.get('reasons', 'N/A')}")
            print(f"Amount: ${alert_data['amt']:.2f}")
            print(f"Risk: {alert_data['risk_score']}/100")
            print()
            print("📝 Generating comprehensive fraud report...")
            
            filepath = generator.generate_pdf(alert_data)
            
            if filepath:
                print(f"✅ Report #{generator.report_count} completed!")
                print(f"   Status: {'CONFIRMED FRAUD ❌' if alert_data['actual_fraud'] == 1 else 'FALSE POSITIVE ✓'}")
                print()
                
                return json.dumps({
                    'processed': True,
                    'report_path': filepath,
                    'pattern': alert_data.get('reasons', '')
                })
        
        if generator.total_alerts % 50 == 0:
            print(f"📊 Progress: {generator.total_alerts} alerts | "
                  f"{generator.report_count} unique patterns")
        
        return json.dumps({'processed': False, 'reason': 'duplicate_pattern'})
    
    except Exception as e:
        print(f"[ERROR] {e}")
        return json.dumps({'processed': False, 'error': str(e)})


# ============================================================================
# MAIN REPORT GENERATOR
# ============================================================================

def run_report_generator():
    """Main report generation pipeline"""
    
    print("─" * 60)
    print("  🚀 FRAUD REPORT GENERATOR ACTIVE")
    print("─" * 60)
    print("  Listening for unique fraud patterns...")
    print()
    
    # Read alerts from detector
    alerts = pw.io.jsonlines.read(
        'pathway_streams/fraud_alerts.jsonl',
        schema=AlertSchema,
        mode='streaming'
    )
    
    print("✓ Subscribed to: pathway_streams/fraud_alerts.jsonl")
    print()
    
    # Process alerts
    reports = alerts.select(
        report_result=process_alert(pw.this.alert_json)
    )
    
    # Write report metadata
    pw.io.jsonlines.write(reports, 'pathway_streams/generated_reports.jsonl')
    
    # Run pipeline
    try:
        pw.run()
    except KeyboardInterrupt:
        print("\n\n═══════════════════════════════════════════════════════════")
        print("    SHUTDOWN COMPLETE")
        print("═══════════════════════════════════════════════════════════")
        print(f"Total Alerts Processed: {generator.total_alerts:,}")
        print(f"Unique Patterns Found: {generator.report_count}")
        print(f"Reports Directory: {generator.reports_dir}/")
        print()


if __name__ == "__main__":
    run_report_generator()