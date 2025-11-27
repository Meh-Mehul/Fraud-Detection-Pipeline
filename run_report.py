# from report.pathway_nats_report_website import run_report_generator
from report.pathway_nats_report import run_report_generator
## Main function to call to start reading from fraud stream and generate reports.
if __name__ == "__main__":
    try:
        run_report_generator()
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("\nMake sure:")
        print("  1. NATS server is running: nats-server")
        print("  2. Detector is publishing to fraud.alerts topic")
        print("  3. ReportLab is installed: pip install reportlab")
        import traceback
        traceback.print_exc()