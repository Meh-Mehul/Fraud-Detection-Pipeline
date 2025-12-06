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

from ato.ato_schema import LoginAttemptSchema, UserAccountProfileSchema
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
    
    print("ATO FRAUD DETECTION PIPELINE ACTIVE")
    print(f"NATS URI: {NATS_URI}")
    print(f"Login Attempts Topic: {ATO_LOGIN_ATTEMPTS_TOPIC}")
    print(f"User Profiles Topic: {ATO_USER_PROFILES_TOPIC}")
    print(f"Alerts Topic: {ATO_FRAUD_ALERTS_TOPIC}")
    print(f"Manual Review Topic: {ATO_MANUAL_REVIEW_TOPIC}")
    print(f"Approved Logins Topic: {ATO_APPROVED_LOGINS_TOPIC}")
    print(f"Metrics Port: {METRICS_ATO_DETECTOR}")
    print()
    
    # Initialize metrics
    metrics_manager = initialize_metrics("ato_detector", port=METRICS_ATO_DETECTOR)
    
    # Read input streams
    login_attempts = pw.io.nats.read(
        uri=NATS_URI,
        topic=ATO_LOGIN_ATTEMPTS_TOPIC,
        schema=LoginAttemptSchema,
        format="json",
        name="ato_login_reader"
    )
    
    user_profiles = pw.io.nats.read(
        uri=NATS_URI,
        topic=ATO_USER_PROFILES_TOPIC,
        schema=UserAccountProfileSchema,
        format="json",
        name="ato_profile_reader"
    )
    
    print("Connected to NATS input streams")
    
    # Build enrichment pipeline
    enriched_logins = StreamingEnrichmentLayer.build_enrichment_pipeline(
        login_stream=login_attempts,
        profile_stream=user_profiles
    )
    
    print("Enrichment layer initialized")
    
    # Apply detection agents
    agent_results = apply_detection_agents(
        enriched_stream=enriched_logins,
        login_stream=login_attempts,
        profile_stream=user_profiles
    )
    
    print("Detection agents active (5 specialized agents)")
    
    # Intelligent routing
    detection_results = IntelligentRouter.process_and_route(
        agent_results=agent_results,
        enriched_data=enriched_logins
    )
    
    print("Intelligent routing configured")
    
    # Split and write to output topics
    routed_streams = IntelligentRouter.split_by_routing(detection_results)
    
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
    
    print("Output streams configured")
    print()
    print("ATO detection pipeline running...")
    print("Press Ctrl+C to stop")
    print()
    
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