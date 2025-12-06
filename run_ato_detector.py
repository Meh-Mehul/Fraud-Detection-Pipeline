"""
ATO Fraud Detection Pipeline - Main Runner
Orchestrates the complete Account Takeover detection pipeline

Architecture Components:
1. NATS Input: login_attempts & user_account_profiles
2. Streaming Enrichment Layer: Activity velocity + feature enrichment
3. Intelligent AI Agents: 5 specialized detection agents (UDFs)
4. Intelligent Routing: Score-based routing to different outputs
5. NATS Outputs: fraud_alerts, manual_review, approved_logins, feedback_loop

Usage:
    python3 run_ato_detector.py
"""

import pathway as pw
from datetime import datetime

from ato.ato_schema import (
    LoginAttemptSchema,
    UserAccountProfileSchema,
    ATODetectionResultSchema
)
from ato.enrichment_layer import StreamingEnrichmentLayer
from ato.detection_agents import apply_detection_agents
from ato.intelligent_routing import IntelligentRouter

from shared.config import (
    NATS_URI,
    ATO_LOGIN_ATTEMPTS_TOPIC,
    ATO_USER_PROFILES_TOPIC,
    ATO_FRAUD_ALERTS_TOPIC,
    ATO_MANUAL_REVIEW_TOPIC,
    ATO_APPROVED_LOGINS_TOPIC,
    ATO_FEEDBACK_LOOP_TOPIC,
    ATO_CHECKPOINT_CONFIG,
    AUTOCOMMIT_DURATION_MS
)


def run_ato_detection_pipeline():
    """
    Main ATO detection pipeline
    Implements the architecture from the diagram
    """
    
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
    
    enriched_logins = StreamingEnrichmentLayer.build_enrichment_pipeline(
        login_stream=login_attempts,
        profile_stream=user_profiles
    )
    
    agent_results = apply_detection_agents(
        enriched_stream=enriched_logins,
        login_stream=login_attempts,
        profile_stream=user_profiles
    )
    
    detection_results = IntelligentRouter.process_and_route(
        agent_results=agent_results,
        enriched_data=enriched_logins
    )
    
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
    
    pw.run(
        monitoring_level=pw.MonitoringLevel.ALL,
        persistence_config=ATO_CHECKPOINT_CONFIG
    )


def main():
    try:
        run_ato_detection_pipeline()
    except KeyboardInterrupt:
        pass
    except Exception as e:
        raise


if __name__ == "__main__":
    main()
