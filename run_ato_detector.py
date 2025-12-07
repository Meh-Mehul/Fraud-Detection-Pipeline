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

from ato.ato_schema import TransactionSchema
from ato.enrichment_layer import StreamingEnrichmentLayer
from ato.detection_agents import apply_detection_agents
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
    
    # Initialize metrics
    metrics_manager = initialize_metrics("ato_detector", port=METRICS_ATO_DETECTOR)
    
    # Read input stream from NATS (TransactionSchema only)
    transactions = pw.io.nats.read(
        uri=NATS_URI,
        topic=ATO_LOGIN_ATTEMPTS_TOPIC,
        schema=TransactionSchema,
        format="json",
        name="ato_transaction_reader"
    )
    
    # Build enrichment pipeline (no profile stream needed)
    enriched_transactions = StreamingEnrichmentLayer.build_enrichment_pipeline(
        login_stream=transactions
    )
    
    # Apply detection agents (no login/profile streams needed)
    agent_results = apply_detection_agents(
        enriched_stream=enriched_transactions
    )
    
    # Intelligent routing (using transaction-specific method)
    detection_results = IntelligentRouter.process_and_route_transactions(
        agent_results=agent_results,
        enriched_data=enriched_transactions
    )
    
    # Split and write to output topics
    routed_streams = IntelligentRouter.split_by_routing(detection_results)
    
    # Write to NATS topics
    pw.io.nats.write(
        routed_streams['alerts'],
        uri=NATS_URI,
        topic=ATO_FRAUD_ALERTS_TOPIC
    )
    
    pw.io.nats.write(
        routed_streams['manual_review'],
        uri=NATS_URI,
        topic=ATO_MANUAL_REVIEW_TOPIC
    )
    
    pw.io.nats.write(
        routed_streams['approved'],
        uri=NATS_URI,
        topic=ATO_APPROVED_LOGINS_TOPIC
    )
    
    pw.io.nats.write(
        routed_streams['feedback'],
        uri=NATS_URI,
        topic=ATO_FEEDBACK_LOOP_TOPIC
    )
    
    # Run pipeline
    pw.run(
        monitoring_level=pw.MonitoringLevel.ALL,
        persistence_config=ATO_CHECKPOINT_CONFIG
    )


def main():
    try:
        run_ato_detection_pipeline()
    except KeyboardInterrupt:
        print("\n\nATO detector shutting down...")
    except Exception as e:
        print(f"\nError in ATO detector: {e}")
        raise


if __name__ == "__main__":
    main()