"""
Fraud Report Generator - MODULAR VERSION
Uses shared rules configuration with detector
"""

import pathway as pw
import json
from datetime import datetime
from pathlib import Path
from fraud_rules_loader import FraudRulesManager  # Shared rules!

# NATS Configuration
NATS_URI = "nats://localhost:4222"
NATS_ALERTS_TOPIC = "fraud.alerts"
NATS_REPORTS_TOPIC = "fraud.reports"

# Initialize rules manager (SHARED with detector)
RULES_MANAGER = FraudRulesManager("fraud_detection_rules.json")
print(f"✓ Loaded shared rules from: fraud_detection_rules.json")

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
print("  INTELLIGENT FRAUD REPORT GENERATOR v2.0 - MODULAR")
print("═══════════════════════════════════════════════════════════")

if PDF_AVAILABLE:
    print("✓ PDF generation enabled (ReportLab)")
else:
    print("⚠️  PDF generation disabled")

print("✓ Real-time NATS streaming")
print("✓ Shared rules configuration")
print()


# ============================================================================
# PDF REPORT COMPONENTS (same as before)
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


# ============================================================================
# REPORT GENERATOR - NOW USES RULES MANAGER
# ============================================================================

class ReportGenerator:
    """Generate PDF reports using shared rules configuration"""
    
    def __init__(self):
        self.reports_dir = Path("fraud_reports")
        self.reports_dir.mkdir(exist_ok=True)
        self.seen_patterns = set()
        self.report_count = 0
        self.total_alerts = 0
        
        print(f"✓ Reports directory: {self.reports_dir}/")
        print()
    
    def decode_fraud_indicators(self, reasons, tier):
        """
        Decode fraud indicators using RULES MANAGER
        Now pulls from shared JSON configuration!
        """
        
        indicators = reasons.split('|')
        decoded = []
        severity_counts = {'CRITICAL': 0, 'HIGH': 0, 'MEDIUM': 0}
        
        # Get all indicator definitions from rules
        all_indicators = RULES_MANAGER.get_all_indicators()
        
        for indicator in indicators:
            parts = indicator.split('(')
            base = parts[0]
            value = parts[1].rstrip(')') if len(parts) > 1 else None
            
            # Look up indicator in rules
            indicator_details = RULES_MANAGER.get_indicator_details(base)
            
            if indicator_details:
                severity_counts[indicator_details['severity']] += 1
                
                decoded_info = {
                    'indicator': indicator,
                    'base': base,
                    'value': value,
                    'name': indicator_details['name'],
                    'severity': indicator_details['severity'],
                    'description': indicator_details['description'],
                    'risk': indicator_details['risk']
                }
                decoded.append(decoded_info)
            else:
                # Handle ML or unknown indicators
                if base.startswith('ML'):
                    score = base[2:] if len(base) > 2 else value
                    ml_details = RULES_MANAGER.get_indicator_details('ML')
                    decoded.append({
                        'indicator': indicator,
                        'base': 'ML',
                        'value': score,
                        'name': 'ML Detection',
                        'severity': 'HIGH' if score and float(score) > 80 else 'MEDIUM',
                        'description': f'Machine learning model confidence: {score}%' if score else 'ML anomaly detected',
                        'risk': ml_details['risk'] if ml_details else 'AI detected behavioral patterns'
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
        
        # Get tier info from rules
        tier_info = RULES_MANAGER.get_tier_info(tier)
        
        return {
            'decoded_indicators': decoded,
            'severity_summary': severity_counts,
            'tier_info': tier_info,
            'total_indicators': len(decoded)
        }
    
    def generate_pdf(self, alert_data):
        """Generate PDF report using rules manager"""
        if not PDF_AVAILABLE:
            return None
        
        try:
            # Decode indicators using rules
            indicator_analysis = self.decode_fraud_indicators(
                alert_data['reasons'], 
                alert_data['tier']
            )
            
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
            
            # === TIER INFORMATION FROM RULES ===
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
            
            # === INVESTIGATION PROTOCOLS FROM RULES ===
            story.append(Spacer(1, 0.2*inch))
            story.append(Paragraph('INVESTIGATION PROTOCOL', section_header_style))
            story.append(Spacer(1, 0.1*inch))
            
            # Get protocols from rules manager
            protocols = RULES_MANAGER.get_investigation_protocols()
            
            story.append(Paragraph('Immediate Actions', subsection_style))
            for action in protocols['immediate_actions']:
                story.append(Paragraph(f'• {action}', bullet_style))
            
            story.append(Spacer(1, 0.15*inch))
            story.append(Paragraph('Short-Term Actions', subsection_style))
            for action in protocols['short_term_actions']:
                story.append(Paragraph(f'• {action}', bullet_style))
            
            story.append(Spacer(1, 0.2*inch))
            story.append(Paragraph('Customer Verification Questions', subsection_style))
            for question_template in protocols['verification_questions']:
                # Format template with actual data
                question = question_template.format(
                    merchant=alert_data['merchant'],
                    amt=f"{alert_data['amt']:.2f}",
                    location=alert_data['location']
                )
                story.append(Paragraph(f'• {question}', bullet_style))
            
            # === FRAUD INDICATORS FROM RULES ===
            story.append(PageBreak())
            story.append(Paragraph('FRAUD INDICATORS ANALYSIS', section_header_style))
            story.append(Spacer(1, 0.1*inch))
            
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
            
            # Build PDF
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
# PATHWAY SCHEMA & UDF
# ============================================================================

class AlertSchema(pw.Schema):
    alert_json: str = pw.column_definition(dtype=str)


@pw.udf
def process_alert(alert_json: str) -> str:
    """Process fraud alert and generate report"""
    
    try:
        alert_data = json.loads(alert_json)
        
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
        print("📝 Generating report using shared rules...")
        
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
# MAIN
# ============================================================================

def run_report_generator():
    """Main report generation pipeline"""
    
    print("─" * 60)
    print("  🚀 FRAUD REPORT GENERATOR ACTIVE")
    print("─" * 60)
    print(f"  NATS URI: {NATS_URI}")
    print(f"  Input: {NATS_ALERTS_TOPIC}")
    print(f"  Output: {NATS_REPORTS_TOPIC}")
    print()
    
    alerts = pw.io.nats.read(
        uri=NATS_URI,
        topic=NATS_ALERTS_TOPIC,
        schema=AlertSchema,
        format='json'
    )
    
    print(f"✓ Subscribed to: nats://{NATS_URI}/{NATS_ALERTS_TOPIC}")
    print()
    
    reports = alerts.select(
        report_result=process_alert(pw.this.alert_json)
    )
    
    pw.io.nats.write(reports, uri=NATS_URI, topic=NATS_REPORTS_TOPIC)
    
    try:
        pw.run()
    except KeyboardInterrupt:
        print("\n\n═══════════════════════════════════════════════════════════")
        print("    SHUTDOWN COMPLETE")
        print("═══════════════════════════════════════════════════════════")
        print(f"Total Alerts Processed: {generator.total_alerts:,}")
        print(f"Reports Generated: {generator.report_count}")
        print(f"Reports Directory: {generator.reports_dir}/")
        print()


if __name__ == "__main__":
    run_report_generator()