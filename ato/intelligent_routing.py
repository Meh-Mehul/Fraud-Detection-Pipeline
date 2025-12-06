"""
Intelligent Routing Logic for ATO Detection
Routes login attempts based on fraud scores and confidence levels
Implements the routing layer from the architecture diagram
"""

import pathway as pw
import json
from typing import Dict, Any


@pw.udf
def parse_agent_result(result_json: str) -> Dict[str, Any]:
    """Parse agent result JSON string"""
    try:
        return json.loads(result_json)
    except:
        return {"score": 0, "confidence": 0, "reasons": []}


@pw.udf
def extract_score(result_json: str) -> float:
    """Extract score from agent result"""
    try:
        result = json.loads(result_json)
        return float(result.get("score", 0))
    except:
        return 0.0


@pw.udf
def extract_confidence(result_json: str) -> float:
    """Extract confidence from agent result"""
    try:
        result = json.loads(result_json)
        return float(result.get("confidence", 0))
    except:
        return 0.0


@pw.udf
def extract_reasons(result_json: str) -> str:
    """Extract reasons from agent result as JSON string"""
    try:
        result = json.loads(result_json)
        return json.dumps(result.get("reasons", []))
    except:
        return json.dumps([])


@pw.udf
def calculate_final_score(
    location_score: float,
    device_score: float,
    credential_score: float,
    frequency_score: float,
    biometric_score: float,
    location_confidence: float,
    device_confidence: float,
    credential_confidence: float,
    frequency_confidence: float,
    biometric_confidence: float
) -> float:
    """
    Calculate weighted final fraud score based on individual agent scores and confidences

    Strategy: Use confidence as weights for the weighted average
    Higher confidence agents have more influence on final score
    """
    weights = {
        'location': 1.2,
        'device': 1.1,
        'credential': 1.5,
        'frequency': 1.0,
        'biometric': 0.8
    }
    weighted_sum = (
        location_score * location_confidence * weights['location'] +
        device_score * device_confidence * weights['device'] +
        credential_score * credential_confidence * weights['credential'] +
        frequency_score * frequency_confidence * weights['frequency'] +
        biometric_score * biometric_confidence * weights['biometric']
    )
    total_weight = (
        location_confidence * weights['location'] +
        device_confidence * weights['device'] +
        credential_confidence * weights['credential'] +
        frequency_confidence * weights['frequency'] +
        biometric_confidence * weights['biometric']
    )
    if total_weight == 0:
        return 0.0
    final_score = weighted_sum / total_weight
    return min(100, max(0, final_score))


@pw.udf
def calculate_final_confidence(
    location_confidence: float,
    device_confidence: float,
    credential_confidence: float,
    frequency_confidence: float,
    biometric_confidence: float
) -> float:
    """
    Calculate overall confidence in the detection

    Strategy: Average of all agent confidences
    """
    confidences = [
        location_confidence,
        device_confidence,
        credential_confidence,
        frequency_confidence,
        biometric_confidence
    ]
    valid_confidences = [c for c in confidences if c > 0]
    if not valid_confidences:
        return 0.0
    return sum(valid_confidences) / len(valid_confidences)


@pw.udf
def determine_risk_level(final_score: float, final_confidence: float) -> str:
    """
    Determine risk level based on score and confidence

    Risk Levels:
    - high: Score > 70 AND Confidence > 75
    - medium: Score > 40 OR (Score > 30 AND Confidence > 60)
    - low: Everything else
    """
    if final_score > 70 and final_confidence > 75:
        return "high"
    elif final_score > 60 and final_confidence > 70:
        return "high"
    elif final_score > 50 and final_confidence > 80:
        return "high"
    elif final_score > 40 or (final_score > 30 and final_confidence > 60):
        return "medium"
    else:
        return "low"


@pw.udf
def determine_routing(risk_level: str, final_score: float, final_confidence: float) -> str:
    """
    Determine routing decision based on risk level and confidence

    Routing Targets:
    - ato_fraud_alerts: High confidence (>90) high risk
    - manual_review_queue: Medium-high risk (50-90 confidence)
    - approved_logins: Low risk (<50 confidence or low score)
    - agent_feedback_loop: All detections for model retraining
    """
    if risk_level == "high" and final_confidence > 90:
        return "ato_fraud_alerts"
    elif risk_level == "high" and final_confidence > 70:
        return "manual_review_queue"
    elif risk_level == "medium" and final_score > 50:
        return "manual_review_queue"
    elif risk_level == "medium" and final_confidence > 60:
        return "manual_review_queue"
    else:
        return "approved_logins"


@pw.udf
def compile_alert_reasons(
    location_reasons: str,
    device_reasons: str,
    credential_reasons: str,
    frequency_reasons: str,
    biometric_reasons: str,
    location_score: float,
    device_score: float,
    credential_score: float,
    frequency_score: float,
    biometric_score: float
) -> str:
    """
    Compile all triggered reasons from agents into prioritized list
    Only include reasons from agents that contributed significantly (score > 20)
    """
    all_reasons = []
    try:
        if location_score > 20:
            loc_reasons = json.loads(location_reasons)
            all_reasons.extend([f"[Location] {r}" for r in loc_reasons])
        if device_score > 20:
            dev_reasons = json.loads(device_reasons)
            all_reasons.extend([f"[Device] {r}" for r in dev_reasons])
        if credential_score > 20:
            cred_reasons = json.loads(credential_reasons)
            all_reasons.extend([f"[Credential] {r}" for r in cred_reasons])
        if frequency_score > 20:
            freq_reasons = json.loads(frequency_reasons)
            all_reasons.extend([f"[Frequency] {r}" for r in freq_reasons])
        if biometric_score > 20:
            bio_reasons = json.loads(biometric_reasons)
            all_reasons.extend([f"[Biometric] {r}" for r in bio_reasons])
    except:
        pass
    return json.dumps(all_reasons)


@pw.udf
def compile_key_indicators(
    final_score: float,
    final_confidence: float,
    location_score: float,
    device_score: float,
    credential_score: float,
    frequency_score: float,
    biometric_score: float
) -> str:
    """
    Compile key indicators for human review
    Provides structured context about the detection
    """
    indicators = {
        "final_fraud_score": final_score,
        "detection_confidence": final_confidence,
        "top_signals": [],
        "agent_scores": {
            "location_anomaly": location_score,
            "device_fingerprint": device_score,
            "credential_stuffing": credential_score,
            "login_frequency": frequency_score,
            "behavioral_biometrics": biometric_score
        }
    }
    agent_scores = [
        ("Location Anomaly", location_score),
        ("Device Fingerprint", device_score),
        ("Credential Stuffing", credential_score),
        ("Login Frequency", frequency_score),
        ("Behavioral Biometrics", biometric_score)
    ]
    agent_scores.sort(key=lambda x: x[1], reverse=True)
    indicators["top_signals"] = [
        {"agent": name, "score": score}
        for name, score in agent_scores[:3]
        if score > 15
    ]
    return json.dumps(indicators)


@pw.udf
def recommend_action(risk_level: str, final_score: float, credential_score: float, mfa_enabled: bool) -> str:
    """
    Recommend specific action for handling this login attempt

    Actions:
    - block_immediate: Block and lock account
    - challenge_mfa: Require additional authentication
    - challenge_verification: Send email/SMS verification
    - flag_and_allow: Allow but flag for monitoring
    - allow: Approve normally
    """
    if credential_score > 70:
        return "block_immediate"
    if risk_level == "high":
        if final_score > 85:
            return "block_immediate"
        elif not mfa_enabled:
            return "challenge_verification"
        else:
            return "challenge_mfa"
    elif risk_level == "medium":
        if not mfa_enabled and final_score > 60:
            return "challenge_verification"
        elif final_score > 50:
            return "flag_and_allow"
        else:
            return "allow"
    else:
        return "allow"


class IntelligentRouter:
    """
    Implements intelligent routing based on fraud scores and confidence
    Routes to different Kafka topics based on risk assessment
    """

    @staticmethod
    def process_and_route(agent_results: pw.Table, enriched_data: pw.Table) -> pw.Table:
        """
        Process agent results and create routing decisions

        Args:
            agent_results: Table with all agent results (JSON strings)
            enriched_data: Enriched login data for additional context

        Returns:
            Table with final detection results and routing decisions
        """
        processed = agent_results.select(
            *agent_results,
            location_score=extract_score(agent_results.location_result),
            device_score=extract_score(agent_results.device_result),
            credential_score=extract_score(agent_results.credential_result),
            frequency_score=extract_score(agent_results.frequency_result),
            biometric_score=extract_score(agent_results.biometric_result),
            location_confidence=extract_confidence(agent_results.location_result),
            device_confidence=extract_confidence(agent_results.device_result),
            credential_confidence=extract_confidence(agent_results.credential_result),
            frequency_confidence=extract_confidence(agent_results.frequency_result),
            biometric_confidence=extract_confidence(agent_results.biometric_result),
            location_reasons=extract_reasons(agent_results.location_result),
            device_reasons=extract_reasons(agent_results.device_result),
            credential_reasons=extract_reasons(agent_results.credential_result),
            frequency_reasons=extract_reasons(agent_results.frequency_result),
            biometric_reasons=extract_reasons(agent_results.biometric_result),
        )
        with_final_scores = processed.select(
            *processed,
            final_fraud_score=calculate_final_score(
                processed.location_score,
                processed.device_score,
                processed.credential_score,
                processed.frequency_score,
                processed.biometric_score,
                processed.location_confidence,
                processed.device_confidence,
                processed.credential_confidence,
                processed.frequency_confidence,
                processed.biometric_confidence
            ),
            final_confidence=calculate_final_confidence(
                processed.location_confidence,
                processed.device_confidence,
                processed.credential_confidence,
                processed.frequency_confidence,
                processed.biometric_confidence
            )
        )
        with_routing = with_final_scores.select(
            *with_final_scores,
            risk_level=determine_risk_level(
                with_final_scores.final_fraud_score,
                with_final_scores.final_confidence
            )
        )
        final_results = with_routing.select(
            attempt_id=with_routing.attempt_id,
            user_id=with_routing.user_id,
            timestamp=with_routing.timestamp,
            location_anomaly_score=with_routing.location_score,
            device_fingerprint_score=with_routing.device_score,
            credential_stuffing_score=with_routing.credential_score,
            login_frequency_score=with_routing.frequency_score,
            behavioral_biometrics_score=with_routing.biometric_score,
            location_confidence=with_routing.location_confidence,
            device_confidence=with_routing.device_confidence,
            credential_confidence=with_routing.credential_confidence,
            frequency_confidence=with_routing.frequency_confidence,
            biometric_confidence=with_routing.biometric_confidence,
            final_fraud_score=with_routing.final_fraud_score,
            final_confidence=with_routing.final_confidence,
            risk_level=with_routing.risk_level,
            routing_decision=determine_routing(
                with_routing.risk_level,
                with_routing.final_fraud_score,
                with_routing.final_confidence
            ),
            alert_reasons=compile_alert_reasons(
                with_routing.location_reasons,
                with_routing.device_reasons,
                with_routing.credential_reasons,
                with_routing.frequency_reasons,
                with_routing.biometric_reasons,
                with_routing.location_score,
                with_routing.device_score,
                with_routing.credential_score,
                with_routing.frequency_score,
                with_routing.biometric_score
            ),
            key_indicators=compile_key_indicators(
                with_routing.final_fraud_score,
                with_routing.final_confidence,
                with_routing.location_score,
                with_routing.device_score,
                with_routing.credential_score,
                with_routing.frequency_score,
                with_routing.biometric_score
            ),
            recommended_action=recommend_action(
                with_routing.risk_level,
                with_routing.final_fraud_score,
                with_routing.credential_score,
                with_routing.mfa_enabled
            )
        )
        return final_results

    @staticmethod
    def split_by_routing(results: pw.Table) -> Dict[str, pw.Table]:
        """
        Split results into separate tables based on routing decision

        Returns:
            Dictionary with keys: 'alerts', 'manual_review', 'approved', 'feedback'
        """
        alerts = results.filter(results.routing_decision == "ato_fraud_alerts")
        manual_review = results.filter(results.routing_decision == "manual_review_queue")
        approved = results.filter(results.routing_decision == "approved_logins")
        feedback = results
        return {
            'alerts': alerts,
            'manual_review': manual_review,
            'approved': approved,
            'feedback': feedback
        }
