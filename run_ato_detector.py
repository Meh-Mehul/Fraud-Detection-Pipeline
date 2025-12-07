"""
ATO Fraud Detection Pipeline - Main Runner
Integrates with existing fraud detection infrastructure
"""

import pathway as pw
import sys
from pathlib import Path

# Add project root to path
ROOT = Path(__file__).resolve().parent
sys.path.append(str(ROOT))

from ato.ato_schema import LoginAttemptSchema, UserAccountProfileSchema, TransactionSchema
from ato.enrichment_layer import StreamingEnrichmentLayer
from ato.detection_agents import apply_detection_agents, apply_transaction_agents
from ato.intelligent_routing import IntelligentRouter

# Import shared configuration
from shared.config import (
    NATS_URI,
    ATO_LOGIN_ATTEMPTS_TOPIC,
    ATO_USER_PROFILES_TOPIC,
    ATO_FRAUD_ALERTS_TOPIC,
    ATO_MANUAL_REVIEW_TOPIC,
    ATO_APPROVED_LOGINS_TOPIC,
    ATO_FEEDBACK_LOOP_TOPIC,
    ATO_CHECKPOINT_CONFIG,
    METRICS_ATO_DETECTOR
)

# Import metrics system
from shared.metrics import initialize_metrics, record_fraud_alert, record_latency


def run_ato_detection_pipeline():
    """Main ATO detection pipeline"""
    
    print("═" * 70)
    print("  🚀 ATO FRAUD DETECTION PIPELINE ACTIVE (TRANSACTION MODE)")
    print("═" * 70)
    
    # Initialize metrics
    metrics_manager = initialize_metrics("ato_detector", port=METRICS_ATO_DETECTOR)
    
    # Read input streams
    transactions = pw.io.csv.read(
        path="transactions.csv",
        schema=TransactionSchema,
        mode="static"
    )
    
    print("✓ Connected to CSV input stream")
    
    # Build enrichment pipeline
    enriched_transactions = StreamingEnrichmentLayer.build_transaction_enrichment_pipeline(
        transaction_stream=transactions
    )
    
    print("✓ Enrichment layer initialized")
    
    # Apply detection agents
    agent_results = apply_transaction_agents(
        enriched_stream=enriched_transactions
    )
    
    print("✓ Detection agents active")
    
    # Intelligent routing
    detection_results = IntelligentRouter.process_and_route_transactions(
        agent_results=agent_results,
        enriched_data=enriched_transactions
    )
    
    print("✓ Intelligent routing configured")
    
    # Split and write to output topics
    routed_streams = IntelligentRouter.split_by_routing(detection_results)
    
    # For this demo, we can just print the alerts or write to a file
    pw.io.csv.write(
        routed_streams['alerts'],
        filename="fraud_alerts.csv"
    )
    
    print("✓ Output configured to fraud_alerts.csv")
    print()
    print("🎯 ATO detection pipeline running...")
    print("   Press Ctrl+C to stop")
    print()
    
    # Run pipeline
    pw.run(
        monitoring_level=pw.MonitoringLevel.ALL
    )


def main():
    try:
        run_ato_detection_pipeline()
    except KeyboardInterrupt:
        print("\n\n🛑 ATO detector shutting down...")
    except Exception as e:
        print(f"\n❌ Error in ATO detector: {e}")
        raise


if __name__ == "__main__":
    main()