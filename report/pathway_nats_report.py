"""
Features:
- Subscribes to fraud alert stream via NATS
- Generates bank-grade PDF investigation reports
- Comprehensive fraud indicator decoding (Loaded from Shared JSON)
- Detailed risk assessment and investigation protocols
- Tracks unique fraud patterns
"""

import pathway as pw
import os
import json
import sys
from datetime import datetime
from pathlib import Path
from shared.metrics import initialize_metrics, get_metrics_manager, record_pipeline_latency, get_timestamp_ms


METRICS_PORT = 8004


# ----------------------------------------------------------------------------
# SETUP: Import Shared Rules Loader
# ----------------------------------------------------------------------------
# Add the parent directory to sys.path to allow importing from 'shared'
# This assumes structure:
#   /report/pathway_nats_report.py
#   /shared/rules_loader.py
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent
shared_path = project_root / "shared"

if str(shared_path) not in sys.path:
    sys.path.append(str(shared_path))

try:
    from rules_loader import get_rules_loader
    print("✓ Successfully imported shared rules loader")
except ImportError:
    print(f"❌ Could not import rules_loader from {shared_path}")
    print("Please ensure 'rules_loader.py' is in the 'shared' folder.")
    sys.exit(1)

# ----------------------------------------------------------------------------
# NATS Configuration
# ----------------------------------------------------------------------------
NATS_URI = os.environ.get("NATS_URI", "nats://localhost:4222")
NATS_ALERTS_TOPIC = "fraud.alerts"
NATS_REPORTS_TOPIC = "fraud.reports"

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
print("  INTELLIGENT FRAUD REPORT GENERATOR")
print("═══════════════════════════════════════════════════════════")

if PDF_AVAILABLE:
    print("✓ PDF generation enabled (ReportLab)")
else:
    print("⚠️  PDF generation disabled - install reportlab")

print("✓ Real-time NATS streaming")
print("✓ Rules loaded from external JSON")
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
        
        # Initialize the shared rules loader
        self.rules_loader = get_rules_loader()
        
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
        """Decode fraud indicators using the shared Rules Loader"""
        
        indicators = reasons.split('|')
        decoded = []
        severity_counts = {'CRITICAL': 0, 'HIGH': 0, 'MEDIUM': 0, 'UNKNOWN': 0}
        
        for indicator in indicators:
            parts = indicator.split('(')
            base = parts[0]
            value = parts[1].rstrip(')') if len(parts) > 1 else None
            
            # Use the Loader to get details
            if base.startswith('ML'):
                 # Special handling for ML using the loader's method
                decoded_info = self.rules_loader.decode_ml_indicator(base, value)
                decoded.append(decoded_info)
                severity_counts[decoded_info['severity']] = severity_counts.get(decoded_info['severity'], 0) + 1
            else:
                detail = self.rules_loader.get_indicator(base)
                
                if detail:
                    # Found in JSON
                    severity = detail.get('severity', 'UNKNOWN')
                    if severity in severity_counts:
                        severity_counts[severity] += 1
                    else:
                        severity_counts['UNKNOWN'] += 1
                    
                    decoded_info = {
                        'indicator': indicator,
                        'base': base,
                        'value': value,
                        'name': detail.get('name', base),
                        'severity': severity,
                        'description': detail.get('description', ''),
                        'risk': detail.get('risk', '')
                    }
                    decoded.append(decoded_info)
                else:
                    # Fallback for unknown
                    decoded.append({
                        'indicator': indicator,
                        'base': base,
                        'value': value,
                        'name': f'Unknown: {base}',
                        'severity': 'UNKNOWN',
                        'description': 'System detected anomaly',
                        'risk': 'Requires manual investigation'
                    })
                    severity_counts['UNKNOWN'] += 1
        
        # Get Tier Info from Loader
        tier_details = self.rules_loader.get_tier(tier)
        
        return {
            'decoded_indicators': decoded,
            'severity_summary': severity_counts,
            'tier_info': tier_details,
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
                 Paragraph(f"<b>{indicator_analysis['tier_info']['name']}</b>", body_style)],
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
            # Use loader to get risk level text (though we still need colors here)
            risk_level = self.rules_loader.get_risk_level(risk_score)
            
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
            
            # LOAD FROM JSON/LOADER
            story.append(Paragraph('Immediate Actions (0-4 Hours)', subsection_style))
            immediate_actions = self.rules_loader.get_immediate_actions()
            for action in immediate_actions:
                story.append(Paragraph(f'• {action}', bullet_style))
            
            story.append(Spacer(1, 0.15*inch))
            story.append(Paragraph('Short-Term Actions (4-48 Hours)', subsection_style))
            short_term_actions = self.rules_loader.get_short_term_actions()
            for action in short_term_actions:
                story.append(Paragraph(f'• {action}', bullet_style))
            
            story.append(Spacer(1, 0.2*inch))
            
            # Customer verification questions (Formatted using loader)
            story.append(Paragraph('Customer Verification Questions', subsection_style))
            questions = self.rules_loader.format_verification_questions(
                merchant=alert_data["merchant"],
                amount=alert_data["amt"],
                location=alert_data["location"]
            )
            for q in questions:
                story.append(Paragraph(f'• {q}', bullet_style))
            
            story.append(Spacer(1, 0.2*inch))
            
            # ===== RISK MITIGATION =====
            story.append(Paragraph('RISK MITIGATION STRATEGY', section_header_style))
            story.append(Spacer(1, 0.1*inch))
            
            # LOAD FROM JSON/LOADER
            mitigation_info = self.rules_loader.get_risk_mitigation()
            mitigation_data = [['Category', 'Mitigation Actions']]
            
            for category, actions in mitigation_info.items():
                action_text = "\n".join([f"• {a}" for a in actions])
                mitigation_data.append([category, action_text])
            
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
            
            # LOAD FROM JSON/LOADER
            disposition_scenarios = self.rules_loader.get_disposition_scenarios()
            
            for scenario_title, actions in disposition_scenarios.items():
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
            
            # Save companion JSON file for frontend parsing
            json_filepath = filepath.with_suffix('.json')
            json_data = {
                'trans_num': alert_data['trans_num'],
                'cc_num': str(alert_data['cc_num']),
                'amt': alert_data['amt'],
                'merchant': alert_data['merchant'],
                'category': alert_data['category'],
                'location': alert_data['location'],
                'risk_score': alert_data['risk_score'],
                'tier': alert_data['tier'],
                'reasons': alert_data.get('reasons', ''),
                'confidence': alert_data.get('confidence', 0),
                'actual_fraud': alert_data.get('actual_fraud', 0),
                'ml_score': alert_data.get('ml_score', 0),
                'first': alert_data.get('first', ''),
                'last': alert_data.get('last', ''),
                'city': alert_data.get('city', ''),
                'state': alert_data.get('state', ''),
            }
            with open(json_filepath, 'w') as f:
                json.dump(json_data, f, indent=2)
            
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
    alert_json: str = pw.column_definition(dtype=str)


# ============================================================================
# REPORT PROCESSOR UDF
# ============================================================================

@pw.udf
def process_alert(alert_json: str) -> str:
    """Process fraud alert and generate report if needed"""
    
    try:
        # Calculate timestamps
        report_timestamp_ms = get_timestamp_ms()
        
        # Parse JSON string
        alert_data = json.loads(alert_json)
        
        # Calculate Detector→Report latency from detector_timestamp_ms
        detector_timestamp_ms = alert_data.get('detector_timestamp_ms', 0)
        if detector_timestamp_ms and detector_timestamp_ms > 0:
            det_to_report_latency = (report_timestamp_ms - detector_timestamp_ms) / 1000.0  # to seconds
            if det_to_report_latency > 0 and det_to_report_latency < 60:  # sanity check
                record_pipeline_latency("detector_to_report", det_to_report_latency)
        
        # Calculate TRUE END-TO-END latency: Publisher → Report Generator
        # Uses publish_timestamp_ms that was set when the transaction was first published
        latency_ms_data = alert_data.get('latency_ms', {})
        publish_timestamp_ms = latency_ms_data.get('publish_timestamp_ms', 0) if isinstance(latency_ms_data, dict) else 0
        
        # Fallback: get from the nested latency structure passed through detector
        if not publish_timestamp_ms:
            # The detector passes publish_timestamp_ms in the alert data
            publish_timestamp_ms = alert_data.get('publish_timestamp_ms', 0)
        
        if publish_timestamp_ms and publish_timestamp_ms > 0:
            end_to_end_latency = (report_timestamp_ms - publish_timestamp_ms) / 1000.0  # to seconds
            if end_to_end_latency > 0 and end_to_end_latency < 120:  # sanity check
                record_pipeline_latency("publisher_to_report", end_to_end_latency)
        
        # Check if it's an alert
        if not alert_data.get('is_alert', False):
            return json.dumps({'processed': False, 'reason': 'not_alert'})
        
        generator.total_alerts += 1
        
        print(f"\n{'='*60}")
        print(f"🎯 NEW FRAUD ALERT DETECTED #{generator.total_alerts}")
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
            generator.report_count += 1
            print(f"✅ Report #{generator.report_count} generated successfully!")
            print(f"   Status: {'CONFIRMED FRAUD ❌' if alert_data['actual_fraud'] == 1 else 'UNDER INVESTIGATION 🟠'}")
            print()
            
            return json.dumps({
                'processed': True,
                'report_path': filepath,
                'pattern': alert_data.get('reasons', '')
            })
        
        return json.dumps({'processed': False, 'reason': 'pdf_generation_failed'})
    
    except Exception as e:
        print(f"[ERROR] {e}")
        return json.dumps({'processed': False, 'error': str(e)})


# ============================================================================
# MAIN REPORT GENERATOR
# ============================================================================

metrics_manager = initialize_metrics("report_generator", port=METRICS_PORT)



def run_report_generator():
    """Main report generation pipeline with NATS"""
    
    print("─" * 60)
    print("  🚀 FRAUD REPORT GENERATOR ACTIVE (NATS)")
    print("─" * 60)
    print(f"  NATS URI: {NATS_URI}")
    print(f"  Input: {NATS_ALERTS_TOPIC}")
    print(f"  Output: {NATS_REPORTS_TOPIC}")
    print()
    print("  Listening for unique fraud patterns...")
    print()
    
    # Read alerts from NATS
    alerts = pw.io.nats.read(
        uri=NATS_URI,
        topic=NATS_ALERTS_TOPIC,
        schema=AlertSchema,
        format='json'
    )
    
    print(f"✓ Subscribed to: nats://{NATS_URI}/{NATS_ALERTS_TOPIC}")
    print()
    
    # Process alerts
    reports = alerts.select(
        report_result=process_alert(pw.this.alert_json)
    )
    
    # Write report metadata to NATS
    pw.io.nats.write(
        reports, 
        uri=NATS_URI,
        topic=NATS_REPORTS_TOPIC
    )
    
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