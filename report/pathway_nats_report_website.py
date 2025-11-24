"""
Fixed Fraud Report Generator
- Simplified processing without queue complexity
- Better error handling and debugging
- Works with updated detector output
"""

import pathway as pw
import json
from datetime import datetime
from pathlib import Path

# NATS Configuration
NATS_URI = "nats://localhost:4222"
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
print("✓ Comprehensive fraud analysis")
print()


# ============================================================================
# REPORT GENERATION CLASSES
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
        self.report_count = 0
        self.total_alerts = 0
        
        print(f"✓ Reports directory: {self.reports_dir}/")
        print()
    
    def decode_fraud_indicators(self, reasons, tier):
        """Decode fraud indicators into human-readable explanations"""
        
        indicators = reasons.split('|') if reasons else []
        
        # Comprehensive indicator dictionary (abbreviated for artifact)
        indicator_details = {
            'EXTREME_BURST': {'name': 'Extreme Transaction Burst', 'severity': 'CRITICAL',
                            'description': '4+ transactions within 5 minutes', 
                            'risk': 'Highly indicative of automated fraud tools'},
            'HUGE_AMT': {'name': 'Huge Amount Anomaly', 'severity': 'CRITICAL',
                        'description': 'Transaction 3.8+ std dev above norm',
                        'risk': 'Likely unauthorized large purchase'},
            'FRAUD_HISTORY': {'name': 'Multiple Fraud History', 'severity': 'CRITICAL',
                            'description': 'Customer has 3+ previous fraud incidents',
                            'risk': 'Account compromised multiple times'},
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
                decoded.append({
                    'indicator': indicator,
                    'base': base,
                    'value': value,
                    'name': detail['name'],
                    'severity': detail['severity'],
                    'description': detail['description'],
                    'risk': detail['risk']
                })
            else:
                decoded.append({
                    'indicator': indicator,
                    'base': base,
                    'value': value,
                    'name': f'Indicator: {base}',
                    'severity': 'MEDIUM',
                    'description': 'System detected anomaly',
                    'risk': 'Requires investigation'
                })
        
        tier_info = {
            1: {'name': 'TIER 1 - ABSOLUTE CERTAINTY', 
                'description': '2+ extreme signals OR 1 extreme + high ML',
                'action': 'IMMEDIATE INVESTIGATION - Block card'},
            2: {'name': 'TIER 2 - STRONG EVIDENCE',
                'description': '75+ risk points from multiple indicators',
                'action': 'HIGH PRIORITY - Contact within 24 hours'},
            3: {'name': 'TIER 3 - ML-BASED DETECTION',
                'description': 'High ML confidence (82%+)',
                'action': 'INVESTIGATION RECOMMENDED'}
        }
        
        return {
            'decoded_indicators': decoded,
            'severity_summary': severity_counts,
            'tier_info': tier_info.get(tier, {'name': 'Unknown', 'description': 'N/A', 'action': 'Review'}),
            'total_indicators': len(decoded)
        }
    
    def generate_pdf(self, alert_data):
        """Generate professional PDF report"""
        if not PDF_AVAILABLE:
            print("   ⚠️  PDF generation skipped (reportlab not installed)")
            return None
        
        try:
            # Validate required fields
            required = ['trans_num', 'cc_num', 'amt', 'merchant', 'category', 
                       'risk_score', 'reasons', 'tier']
            missing = [f for f in required if f not in alert_data]
            
            if missing:
                print(f"   ❌ Missing fields: {missing}")
                print(f"   Available: {list(alert_data.keys())}")
                return None
            
            # Decode indicators
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
                leading=18
            )
            
            body_style = ParagraphStyle(
                'ReportBody',
                parent=styles['BodyText'],
                fontSize=10,
                leading=14,
                alignment=TA_JUSTIFY,
                textColor=colors.HexColor('#2a2a2a')
            )
            
            # Status box
            fraud_status = 'CONFIRMED FRAUD' if alert_data.get('actual_fraud', 0) == 1 else 'UNDER INVESTIGATION'
            status_color = colors.HexColor('#c41e3a') if alert_data.get('actual_fraud', 0) == 1 else colors.HexColor('#e67e22')
            
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
            
            # Risk Assessment
            story.append(Paragraph('RISK ASSESSMENT', section_header_style))
            story.append(Spacer(1, 0.1*inch))
            
            risk_score = alert_data['risk_score']
            risk_level = 'EXTREME' if risk_score >= 90 else 'CRITICAL' if risk_score >= 80 else 'HIGH' if risk_score >= 70 else 'ELEVATED'
            
            story.append(Paragraph(f'<b>Risk Score:</b> {risk_score}/100 ({risk_level})', body_style))
            story.append(Paragraph(f'<b>ML Score:</b> {alert_data.get("ml_score", "N/A")}', body_style))
            story.append(Paragraph(f'<b>Indicators:</b> {indicator_analysis["total_indicators"]}', body_style))
            story.append(Spacer(1, 0.2*inch))
            
            # Transaction Details
            story.append(Paragraph('TRANSACTION DETAILS', section_header_style))
            story.append(Spacer(1, 0.1*inch))
            
            trans_data = [
                ['Field', 'Value'],
                ['Transaction ID', str(alert_data['trans_num'])],
                ['Customer ID', f"****-****-****-{str(alert_data['cc_num'])[-4:]}"],
                ['Amount', f"${alert_data['amt']:,.2f}"],
                ['Merchant', alert_data['merchant']],
                ['Category', alert_data['category']],
                ['Location', alert_data.get('location', 'N/A')],
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
            
            # Fraud Indicators
            story.append(Paragraph('FRAUD INDICATORS', section_header_style))
            story.append(Spacer(1, 0.1*inch))
            
            story.append(Paragraph(f'<b>Raw:</b> {alert_data["reasons"]}', body_style))
            story.append(Spacer(1, 0.1*inch))
            
            for ind in indicator_analysis['decoded_indicators'][:5]:
                story.append(Paragraph(
                    f'<b>• {ind["name"]}:</b> {ind["description"]}', 
                    body_style
                ))
            
            story.append(Spacer(1, 0.2*inch))
            
            # Investigation Protocol
            story.append(Paragraph('RECOMMENDED ACTIONS', section_header_style))
            story.append(Spacer(1, 0.1*inch))
            
            story.append(Paragraph(
                f'<b>{indicator_analysis["tier_info"]["name"]}</b>', 
                body_style
            ))
            story.append(Paragraph(
                indicator_analysis['tier_info']['action'], 
                body_style
            ))
            
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
# PATHWAY SCHEMA
# ============================================================================

class AlertSchema(pw.Schema):
    alert_json: str = pw.column_definition(dtype=str)


# ============================================================================
# SIMPLIFIED REPORT PROCESSOR
# ============================================================================

@pw.udf
def process_alert(alert_json: str) -> str:
    """
    Simplified alert processor with better error handling
    """
    
    try:
        # Parse JSON
        alert_data = json.loads(alert_json)
        
        # DEBUG: Print received data
        print(f"\n{'='*60}")
        print(f"📥 RECEIVED ALERT")
        print(f"{'='*60}")
        print(f"Keys: {list(alert_data.keys())}")
        
        # Check if it's an alert
        if not alert_data.get('is_alert', False):
            print("⚠️  Not an alert, skipping")
            return json.dumps({'processed': False, 'reason': 'not_alert'})
        
        generator.total_alerts += 1
        
        print(f"🎯 ALERT #{generator.total_alerts}")
        print(f"   Transaction: {alert_data.get('trans_num', 'N/A')}")
        print(f"   Customer: ****{str(alert_data.get('cc_num', '????'))[-4:]}")
        print(f"   Amount: ${alert_data.get('amt', 0):.2f}")
        print(f"   Risk: {alert_data.get('risk_score', 0)}/100")
        print(f"   Reasons: {alert_data.get('reasons', 'N/A')}")
        
        # Validate required fields
        required_fields = ['trans_num', 'cc_num', 'amt', 'merchant', 'category', 
                          'risk_score', 'reasons', 'tier']
        missing = [f for f in required_fields if f not in alert_data]
        
        if missing:
            print(f"\n❌ MISSING FIELDS: {missing}")
            print(f"   Cannot generate report without these fields")
            print(f"   FIX: Update detector to include all fields in output")
            return json.dumps({
                'processed': False, 
                'reason': 'missing_fields',
                'missing': missing
            })
        
        print("\n📝 Generating PDF report...")
        
        # Generate PDF
        filepath = generator.generate_pdf(alert_data)
        
        if filepath:
            generator.report_count += 1
            print(f"✅ Report #{generator.report_count} generated!")
            print(f"   File: {Path(filepath).name}")
            print()
            
            return json.dumps({
                'processed': True,
                'report_path': filepath,
                'report_id': Path(filepath).stem,
                'pattern': alert_data.get('reasons', '')
            })
        else:
            print("❌ PDF generation failed")
            return json.dumps({'processed': False, 'reason': 'pdf_failed'})
    
    except json.JSONDecodeError as e:
        print(f"❌ JSON Parse Error: {e}")
        print(f"   Raw: {alert_json[:200]}")
        return json.dumps({'processed': False, 'error': 'json_decode'})
    
    except KeyError as e:
        print(f"❌ Missing Key: {e}")
        return json.dumps({'processed': False, 'error': f'missing_key_{e}'})
    
    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
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
    
    # Read alerts from NATS
    alerts = pw.io.nats.read(
        uri=NATS_URI,
        topic=NATS_ALERTS_TOPIC,
        schema=AlertSchema,
        format='json'
    )
    
    print(f"✓ Subscribed to: {NATS_ALERTS_TOPIC}")
    print("✓ Waiting for alerts...")
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
        print(f"Reports Generated: {generator.report_count}")
        print(f"Reports Directory: {generator.reports_dir}/")
        print()