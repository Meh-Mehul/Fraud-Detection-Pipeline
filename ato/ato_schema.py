"""
ATO (Account Takeover) Fraud Detection Schema Definitions
Defines the structure for login attempts and user account profiles
"""

import pathway as pw
from typing import Optional


class LoginAttemptSchema(pw.Schema):
    """
    Schema for login attempt events from Kafka topic: login_attempts
    Represents each authentication attempt in the system
    """
    attempt_id: str = pw.column_definition(dtype=str)
    user_id: str = pw.column_definition(dtype=str)
    username: str = pw.column_definition(dtype=str)
    timestamp: int = pw.column_definition(dtype=int)
    login_status: str = pw.column_definition(dtype=str)
    failure_reason: str = pw.column_definition(dtype=str)
    attempt_count: int = pw.column_definition(dtype=int)
    device_id: str = pw.column_definition(dtype=str)
    device_type: str = pw.column_definition(dtype=str)
    os: str = pw.column_definition(dtype=str)
    browser: str = pw.column_definition(dtype=str)
    browser_version: str = pw.column_definition(dtype=str)
    user_agent: str = pw.column_definition(dtype=str)
    screen_resolution: str = pw.column_definition(dtype=str)
    ip_address: str = pw.column_definition(dtype=str)
    latitude: float = pw.column_definition(dtype=float)
    longitude: float = pw.column_definition(dtype=float)
    city: str = pw.column_definition(dtype=str)
    country: str = pw.column_definition(dtype=str)
    isp: str = pw.column_definition(dtype=str)
    is_vpn: bool = pw.column_definition(dtype=bool)
    is_proxy: bool = pw.column_definition(dtype=bool)
    is_tor: bool = pw.column_definition(dtype=bool)
    password_hash_prefix: str = pw.column_definition(dtype=str)
    credential_breach_found: bool = pw.column_definition(dtype=bool)
    typing_pattern_score: float = pw.column_definition(dtype=float)
    mouse_pattern_score: float = pw.column_definition(dtype=float)
    session_duration: int = pw.column_definition(dtype=int)
    session_id: str = pw.column_definition(dtype=str)
    referrer_url: str = pw.column_definition(dtype=str)
    login_method: str = pw.column_definition(dtype=str)


class UserAccountProfileSchema(pw.Schema):
    """
    Schema for user account profile metadata from Kafka topic: user_account_profiles
    Contains historical user behavior patterns and profile information
    """
    user_id: str = pw.column_definition(dtype=str)
    account_created_at: int = pw.column_definition(dtype=int)
    last_updated: int = pw.column_definition(dtype=int)
    account_status: str = pw.column_definition(dtype=str)
    is_verified: bool = pw.column_definition(dtype=bool)
    mfa_enabled: bool = pw.column_definition(dtype=bool)
    risk_score: float = pw.column_definition(dtype=float)
    typical_login_hours: str = pw.column_definition(dtype=str)
    typical_locations: str = pw.column_definition(dtype=str)
    typical_devices: str = pw.column_definition(dtype=str)
    total_login_count: int = pw.column_definition(dtype=int)
    failed_login_count_24h: int = pw.column_definition(dtype=int)
    successful_login_count_24h: int = pw.column_definition(dtype=int)
    last_successful_login: int = pw.column_definition(dtype=int)
    last_failed_login: int = pw.column_definition(dtype=int)
    primary_country: str = pw.column_definition(dtype=str)
    primary_device_type: str = pw.column_definition(dtype=str)
    average_session_duration: int = pw.column_definition(dtype=int)
    password_last_changed: int = pw.column_definition(dtype=int)
    security_questions_set: bool = pw.column_definition(dtype=bool)
    previous_ato_incidents: int = pw.column_definition(dtype=int)
    flagged_for_review: bool = pw.column_definition(dtype=bool)


class EnrichedLoginSchema(pw.Schema):
    """
    Schema for enriched login data after feature engineering
    Combines login attempts with user profiles and computed features
    """
    attempt_id: str = pw.column_definition(dtype=str)
    user_id: str = pw.column_definition(dtype=str)
    timestamp: int = pw.column_definition(dtype=int)
    login_status: str = pw.column_definition(dtype=str)
    device_id: str = pw.column_definition(dtype=str)
    latitude: float = pw.column_definition(dtype=float)
    longitude: float = pw.column_definition(dtype=float)
    country: str = pw.column_definition(dtype=str)
    is_vpn: bool = pw.column_definition(dtype=bool)
    login_velocity_5min: int = pw.column_definition(dtype=int)
    login_velocity_1hour: int = pw.column_definition(dtype=int)
    failed_attempts_5min: int = pw.column_definition(dtype=int)
    distance_from_typical_location_km: float = pw.column_definition(dtype=float)
    is_new_device: bool = pw.column_definition(dtype=bool)
    is_unusual_hour: bool = pw.column_definition(dtype=bool)
    account_age_days: int = pw.column_definition(dtype=int)
    mfa_enabled: bool = pw.column_definition(dtype=bool)
    previous_ato_incidents: int = pw.column_definition(dtype=int)


class ATODetectionResultSchema(pw.Schema):
    """
    Schema for final ATO detection results after all agents have processed
    Contains aggregated scores and routing decision
    """
    attempt_id: str = pw.column_definition(dtype=str)
    user_id: str = pw.column_definition(dtype=str)
    timestamp: int = pw.column_definition(dtype=int)
    location_anomaly_score: float = pw.column_definition(dtype=float)
    device_fingerprint_score: float = pw.column_definition(dtype=float)
    credential_stuffing_score: float = pw.column_definition(dtype=float)
    login_frequency_score: float = pw.column_definition(dtype=float)
    behavioral_biometrics_score: float = pw.column_definition(dtype=float)
    location_confidence: float = pw.column_definition(dtype=float)
    device_confidence: float = pw.column_definition(dtype=float)
    credential_confidence: float = pw.column_definition(dtype=float)
    frequency_confidence: float = pw.column_definition(dtype=float)
    biometric_confidence: float = pw.column_definition(dtype=float)
    final_fraud_score: float = pw.column_definition(dtype=float)
    final_confidence: float = pw.column_definition(dtype=float)
    risk_level: str = pw.column_definition(dtype=str)
    routing_decision: str = pw.column_definition(dtype=str)
    alert_reasons: str = pw.column_definition(dtype=str)
    key_indicators: str = pw.column_definition(dtype=str)
    recommended_action: str = pw.column_definition(dtype=str)
