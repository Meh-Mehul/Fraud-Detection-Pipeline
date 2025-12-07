"""
Intelligent AI Agents for ATO Fraud Detection (Pathway UDFs)
Implements 5 specialized detection agents as per architecture:
1. Location Anomaly Detector
2. Device Fingerprint Analyzer
3. Credential Stuffing Detector
4. Login Frequency Profiler
5. Behavioral Biometrics Monitor

Each agent outputs a score (0-100) and confidence level (0-100)
"""

import pathway as pw
from pathway import JoinMode
import json
from typing import Dict, Tuple


class DetectionResult:
    """Container for agent detection results"""
    def __init__(self, score: float, confidence: float, reasons: list):
        self.score = score
        self.confidence = confidence
        self.reasons = reasons


@pw.udf
def location_anomaly_agent(
    distance_from_typical_km: float,
    country: str,
    is_vpn: bool,
    is_proxy: bool,
    is_tor: bool,
    typical_locations_json: str,
    previous_ato_incidents: int
) -> str:
    """
    Detects geographic anomalies in login attempts

    Signals:
    - Large distance from typical locations
    - VPN/Proxy/Tor usage
    - Country different from typical
    - Impossible travel (future: requires time-series)

    Returns: JSON with {"score": float, "confidence": float, "reasons": [str]}
    """
    score = 0.0
    confidence = 80.0
    reasons = []

    if distance_from_typical_km > 5000:
        score += 45
        reasons.append(f"Login from {distance_from_typical_km:.0f}km away (different continent)")
        confidence += 10
    elif distance_from_typical_km > 1000:
        score += 30
        reasons.append(f"Login from {distance_from_typical_km:.0f}km away (unusual location)")
        confidence += 5
    elif distance_from_typical_km > 100:
        score += 15
        reasons.append(f"Login from {distance_from_typical_km:.0f}km away (different city)")

    if is_tor:
        score += 40
        reasons.append("Tor network detected (high anonymity risk)")
        confidence += 15
    elif is_vpn or is_proxy:
        score += 25
        reasons.append("VPN/Proxy detected (location masking)")
        confidence += 10

    if previous_ato_incidents > 0:
        score += 20
        reasons.append(f"Account has {previous_ato_incidents} previous ATO incident(s)")
        confidence += 10

    try:
        typical_locs = json.loads(typical_locations_json) if typical_locations_json else []
        if not typical_locs:
            score += 10
            confidence -= 20
            reasons.append("New account with no location history")
    except:
        pass

    score = min(100, score)
    confidence = min(100, max(20, confidence))

    return json.dumps({
        "score": score,
        "confidence": confidence,
        "reasons": reasons
    })


@pw.udf
def device_fingerprint_agent(
    is_new_device: bool,
    device_type: str,
    os: str,
    browser: str,
    user_agent: str,
    typical_devices_json: str,
    account_age_days: int
) -> str:
    """
    Analyzes device fingerprint for anomalies

    Signals:
    - New/unknown device
    - Inconsistent device information
    - Unusual device for account type
    - Device switching patterns

    Returns: JSON with {"score": float, "confidence": float, "reasons": [str]}
    """
    score = 0.0
    confidence = 75.0
    reasons = []

    if is_new_device:
        if account_age_days > 30:
            score += 35
            reasons.append("Login from new/unrecognized device")
            confidence += 10
        elif account_age_days > 7:
            score += 25
            reasons.append("Login from new device on young account")
            confidence += 5
        else:
            score += 10
            reasons.append("New device on new account (expected)")
            confidence -= 10

    try:
        typical_devices = json.loads(typical_devices_json) if typical_devices_json else []
        device_count = len(typical_devices)

        if device_count > 10:
            score += 25
            reasons.append(f"Excessive device count ({device_count} devices)")
            confidence += 10
        elif device_count > 5:
            score += 15
            reasons.append(f"High device count ({device_count} devices)")
    except:
        pass

    suspicious_keywords = ['bot', 'crawler', 'scraper', 'automated', 'headless']
    user_agent_lower = user_agent.lower()

    for keyword in suspicious_keywords:
        if keyword in user_agent_lower:
            score += 50
            reasons.append(f"Automated/bot user agent detected: {keyword}")
            confidence += 20
            break

    if 'windows' in os.lower() and 'safari' in browser.lower():
        score += 20
        reasons.append("Inconsistent device fingerprint (Windows + Safari)")
        confidence += 5

    score = min(100, score)
    confidence = min(100, max(20, confidence))

    return json.dumps({
        "score": score,
        "confidence": confidence,
        "reasons": reasons
    })


@pw.udf
def credential_stuffing_agent(
    credential_breach_found: bool,
    failed_attempts_5min: int,
    login_status: str,
    login_velocity_5min: int,
    password_hash_prefix: str
) -> str:
    """
    Detects credential stuffing attacks

    Signals:
    - Credentials found in breach databases
    - Rapid multiple login attempts
    - Pattern of failed then successful
    - High velocity from single source

    Returns: JSON with {"score": float, "confidence": float, "reasons": [str]}
    """
    score = 0.0
    confidence = 85.0
    reasons = []

    if credential_breach_found:
        score += 60
        reasons.append("Credentials found in known data breach")
        confidence += 15

    if failed_attempts_5min >= 10:
        score += 45
        reasons.append(f"High failed attempt rate: {failed_attempts_5min} in 5 min")
        confidence += 10
    elif failed_attempts_5min >= 5:
        score += 30
        reasons.append(f"Elevated failed attempts: {failed_attempts_5min} in 5 min")
        confidence += 5
    elif failed_attempts_5min >= 3:
        score += 15
        reasons.append(f"Multiple failed attempts: {failed_attempts_5min} in 5 min")

    if login_velocity_5min >= 20:
        score += 40
        reasons.append(f"Extremely high login velocity: {login_velocity_5min} attempts in 5 min")
        confidence += 15
    elif login_velocity_5min >= 10:
        score += 25
        reasons.append(f"High login velocity: {login_velocity_5min} attempts in 5 min")
        confidence += 10

    common_weak_prefixes = ['5f4d', 'e10a', '9820']
    if password_hash_prefix in common_weak_prefixes:
        score += 20
        reasons.append("Weak/common password detected")
        confidence += 5

    score = min(100, score)
    confidence = min(100, max(20, confidence))

    return json.dumps({
        "score": score,
        "confidence": confidence,
        "reasons": reasons
    })


@pw.udf
def login_frequency_agent(
    is_unusual_hour: bool,
    login_velocity_1hour: int,
    login_velocity_5min: int,
    account_age_days: int,
    typical_hours_json: str
) -> str:
    """
    Analyzes login frequency patterns

    Signals:
    - Login at unusual hours for user
    - Abnormal login frequency
    - Deviation from established patterns
    - Suspicious timing patterns

    Returns: JSON with {"score": float, "confidence": float, "reasons": [str]}
    """
    score = 0.0
    confidence = 70.0
    reasons = []

    if is_unusual_hour:
        if account_age_days > 30:
            score += 25
            reasons.append("Login at unusual hour for this user")
            confidence += 10
        else:
            score += 10
            reasons.append("Login at unusual hour (limited history)")
            confidence -= 5

    if login_velocity_1hour >= 30:
        score += 40
        reasons.append(f"Extreme login frequency: {login_velocity_1hour} logins in 1 hour")
        confidence += 15
    elif login_velocity_1hour >= 15:
        score += 25
        reasons.append(f"High login frequency: {login_velocity_1hour} logins in 1 hour")
        confidence += 10
    elif login_velocity_1hour >= 8:
        score += 15
        reasons.append(f"Elevated login frequency: {login_velocity_1hour} logins in 1 hour")
        confidence += 5

    if login_velocity_5min >= 5 and login_velocity_1hour < 10:
        score += 20
        reasons.append("Burst login pattern detected (sudden activity spike)")
        confidence += 5

    if account_age_days < 7 and login_velocity_1hour > 5:
        score += 30
        reasons.append("High activity on new account")
        confidence += 10

    try:
        typical_hours = json.loads(typical_hours_json) if typical_hours_json else []
        if typical_hours and all(h not in [1, 2, 3, 4, 5] for h in typical_hours):
            pass
    except:
        pass

    score = min(100, score)
    confidence = min(100, max(20, confidence))

    return json.dumps({
        "score": score,
        "confidence": confidence,
        "reasons": reasons
    })


@pw.udf
def behavioral_biometrics_agent(
    typing_pattern_score: float,
    mouse_pattern_score: float,
    session_duration: int,
    login_method: str,
    mfa_enabled: bool
) -> str:
    """
    Monitors behavioral biometric patterns

    Signals:
    - Anomalous typing patterns (keystroke dynamics)
    - Unusual mouse movement patterns
    - Suspicious session behaviors
    - Lack of MFA on high-risk accounts

    Returns: JSON with {"score": float, "confidence": float, "reasons": [str]}
    """
    score = 0.0
    confidence = 65.0
    reasons = []

    if typing_pattern_score > 0.8:
        score += 35
        reasons.append(f"Highly anomalous typing pattern (score: {typing_pattern_score:.2f})")
        confidence += 15
    elif typing_pattern_score > 0.6:
        score += 20
        reasons.append(f"Unusual typing pattern (score: {typing_pattern_score:.2f})")
        confidence += 10
    elif typing_pattern_score > 0.4:
        score += 10
        reasons.append(f"Slightly unusual typing pattern (score: {typing_pattern_score:.2f})")
        confidence += 5

    if mouse_pattern_score > 0.8:
        score += 35
        reasons.append(f"Highly anomalous mouse behavior (score: {mouse_pattern_score:.2f})")
        confidence += 15
    elif mouse_pattern_score > 0.6:
        score += 20
        reasons.append(f"Unusual mouse behavior (score: {mouse_pattern_score:.2f})")
        confidence += 10
    elif mouse_pattern_score > 0.4:
        score += 10
        reasons.append(f"Slightly unusual mouse behavior (score: {mouse_pattern_score:.2f})")
        confidence += 5

    if session_duration < 5:
        score += 25
        reasons.append(f"Extremely short session duration: {session_duration}s (bot-like)")
        confidence += 10
    elif session_duration < 15:
        score += 10
        reasons.append(f"Short session duration: {session_duration}s")
        confidence += 5

    if not mfa_enabled:
        score += 15
        reasons.append("MFA not enabled (higher risk account)")
        confidence += 5

    if login_method in ['api', 'token', 'oauth'] and (typing_pattern_score == 0 or mouse_pattern_score == 0):
        score += 20
        reasons.append(f"Automated login method ({login_method}) with no biometric data")
        confidence -= 10

    score = min(100, score)
    confidence = min(100, max(20, confidence))

    return json.dumps({
        "score": score,
        "confidence": confidence,
        "reasons": reasons
    })


def apply_detection_agents(enriched_stream: pw.Table, login_stream: pw.Table, profile_stream: pw.Table) -> pw.Table:
    """
    Apply all 5 detection agents to the enriched login stream

    Args:
        enriched_stream: Enriched login data with velocity and profile features
        login_stream: Original login attempt stream (for additional fields)
        profile_stream: User profile stream (for additional fields)

    Returns:
        Table with all agent scores and confidence levels
    """

    full_data = enriched_stream.join(
        login_stream,
        enriched_stream.attempt_id == login_stream.attempt_id
    ).select(
        *enriched_stream,
        is_proxy=login_stream.is_proxy,
        is_tor=login_stream.is_tor,
        device_type=login_stream.device_type,
        os=login_stream.os,
        browser=login_stream.browser,
        user_agent=login_stream.user_agent,
        credential_breach_found=login_stream.credential_breach_found,
        password_hash_prefix=login_stream.password_hash_prefix,
        typing_pattern_score=login_stream.typing_pattern_score,
        mouse_pattern_score=login_stream.mouse_pattern_score,
        session_duration=login_stream.session_duration,
        login_method=login_stream.login_method,
    )

    full_data = full_data.join(
        profile_stream,
        full_data.user_id == profile_stream.user_id,
        how=JoinMode.LEFT
    ).select(
        *full_data,
        typical_locations=profile_stream.typical_locations,
        typical_devices=profile_stream.typical_devices,
        typical_hours=profile_stream.typical_login_hours,
    )

    with_agents = full_data.select(
        *full_data,
        location_result=location_anomaly_agent(
            full_data.distance_from_typical_location_km,
            full_data.country,
            full_data.is_vpn,
            full_data.is_proxy,
            full_data.is_tor,
            full_data.typical_locations,
            full_data.previous_ato_incidents
        ),
        device_result=device_fingerprint_agent(
            full_data.is_new_device,
            full_data.device_type,
            full_data.os,
            full_data.browser,
            full_data.user_agent,
            full_data.typical_devices,
            full_data.account_age_days
        ),
        credential_result=credential_stuffing_agent(
            full_data.credential_breach_found,
            full_data.failed_attempts_5min,
            full_data.login_status,
            full_data.login_velocity_5min,
            full_data.password_hash_prefix
        ),
        frequency_result=login_frequency_agent(
            full_data.is_unusual_hour,
            full_data.login_velocity_1hour,
            full_data.login_velocity_5min,
            full_data.account_age_days,
            full_data.typical_hours
        ),
        biometric_result=behavioral_biometrics_agent(
            full_data.typing_pattern_score,
            full_data.mouse_pattern_score,
            full_data.session_duration,
            full_data.login_method,
            full_data.mfa_enabled
        )
    )

    return with_agents


@pw.udf
def high_amount_agent(amount: float) -> str:
    score = 0.0
    confidence = 80.0
    reasons = []
    if amount > 200000:
        score = 90
        reasons.append("Very high transaction amount")
    elif amount > 50000:
        score = 60
        reasons.append("High transaction amount")
    return json.dumps({"score": score, "confidence": confidence, "reasons": reasons})

@pw.udf
def balance_drain_agent(oldbalanceOrg: float, newbalanceOrig: float) -> str:
    score = 0.0
    confidence = 90.0
    reasons = []
    if oldbalanceOrg > 0 and newbalanceOrig == 0:
        score = 95
        reasons.append("Account emptied completely")
    elif oldbalanceOrg > 0 and newbalanceOrig < oldbalanceOrg * 0.1:
        score = 70
        reasons.append("Account nearly emptied")
    return json.dumps({"score": score, "confidence": confidence, "reasons": reasons})

def apply_transaction_agents(enriched_stream: pw.Table) -> pw.Table:
    """
    Apply agents to transaction stream
    """
    with_agents = enriched_stream.select(
        step=enriched_stream.step,
        type=enriched_stream.type,
        amount=enriched_stream.amount,
        nameOrig=enriched_stream.nameOrig,
        oldbalanceOrg=enriched_stream.oldbalanceOrg,
        newbalanceOrig=enriched_stream.newbalanceOrig,
        nameDest=enriched_stream.nameDest,
        isFraud=enriched_stream.isFraud,
        tx_count=enriched_stream.tx_count,
        total_amount_history=enriched_stream.total_amount_history,
        location_result=high_amount_agent(enriched_stream.amount),
        credential_result=balance_drain_agent(enriched_stream.oldbalanceOrg, enriched_stream.newbalanceOrig),
        device_result=pw.apply(lambda _: json.dumps({"score": 0, "confidence": 0, "reasons": []}), enriched_stream.amount),
        frequency_result=pw.apply(lambda _: json.dumps({"score": 0, "confidence": 0, "reasons": []}), enriched_stream.amount),
        biometric_result=pw.apply(lambda _: json.dumps({"score": 0, "confidence": 0, "reasons": []}), enriched_stream.amount)
    )
    return with_agents
