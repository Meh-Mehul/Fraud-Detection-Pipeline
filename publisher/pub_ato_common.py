"""
ATO Data Publishers
Publishes login attempts and user profiles to NATS topics
"""

import time
import json
import threading
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))

from ato.sample_data_generator import ATODataGenerator
from shared.config import (
    NATS_URI,
    ATO_LOGIN_ATTEMPTS_TOPIC,
    ATO_USER_PROFILES_TOPIC
)

import pathway as pw


class LoginAttemptSchema(pw.Schema):
    attempt_id: str
    user_id: str
    username: str
    timestamp: int
    login_status: str
    failure_reason: str
    attempt_count: int
    device_id: str
    device_type: str
    os: str
    browser: str
    browser_version: str
    user_agent: str
    screen_resolution: str
    ip_address: str
    latitude: float
    longitude: float
    city: str
    country: str
    isp: str
    is_vpn: bool
    is_proxy: bool
    is_tor: bool
    password_hash_prefix: str
    credential_breach_found: bool
    typing_pattern_score: float
    mouse_pattern_score: float
    session_duration: int
    session_id: str
    referrer_url: str
    login_method: str


class UserProfileSchema(pw.Schema):
    user_id: str
    account_created_at: int
    last_updated: int
    account_status: str
    is_verified: bool
    mfa_enabled: bool
    risk_score: float
    typical_login_hours: str
    typical_locations: str
    typical_devices: str
    total_login_count: int
    failed_login_count_24h: int
    successful_login_count_24h: int
    last_successful_login: int
    last_failed_login: int
    primary_country: str
    primary_device_type: str
    average_session_duration: int
    password_last_changed: int
    security_questions_set: bool
    previous_ato_incidents: int
    flagged_for_review: bool


# ============================================================================
# PUBLISHERS
# ============================================================================

def run_login_attempts_publisher(tps=10):
    """Publish login attempts to NATS"""
    
    print("\n" + "═" * 70)
    print("  📤 ATO LOGIN ATTEMPTS PUBLISHER")
    print("═" * 70)
    print(f"  Target TPS: {tps}")
    print(f"  Topic: {ATO_LOGIN_ATTEMPTS_TOPIC}")
    print("=" * 70)
    
    generator = ATODataGenerator()
    profiles, _ = generator.generate_test_dataset(num_logins=0)
    profile_map = {p["user_id"]: p for p in profiles}
    
    interval = 1.0 / tps
    count = 0
    
    # Create temp file for Pathway to read
    temp_file = Path("./publisher/temp_ato_logins.jsonl")
    temp_file.parent.mkdir(exist_ok=True)
    temp_file.write_text("")
    
    while True:
        t0 = time.time()
        
        # Generate login attempt
        user_id = generator.user_ids[count % len(generator.user_ids)]
        profile = profile_map[user_id]
        
        if count % 10 == 0:  # 10% fraud rate
            attack_type = ["location_anomaly", "new_device", "credential_stuffing"][count % 3]
            login = generator.generate_suspicious_login(user_id, profile, attack_type)
        else:
            login = generator.generate_normal_login(user_id, profile)
        
        # Write to temp file
        with open(temp_file, 'a') as f:
            f.write(json.dumps(login) + '\n')
        
        count += 1
        
        if count % 100 == 0:
            print(f"   Published: {count:,} login attempts")
        
        # Sleep
        elapsed = time.time() - t0
        sleep_time = max(0, interval - elapsed)
        time.sleep(sleep_time)


def run_user_profiles_publisher():
    """Publish user profiles once at startup"""
    
    print("\n" + "═" * 70)
    print("  📤 ATO USER PROFILES PUBLISHER")
    print("═" * 70)
    print(f"  Topic: {ATO_USER_PROFILES_TOPIC}")
    print("=" * 70)
    
    generator = ATODataGenerator()
    profiles, _ = generator.generate_test_dataset(num_logins=0)
    
    # Create temp file
    temp_file = Path("./publisher/temp_ato_profiles.jsonl")
    temp_file.parent.mkdir(exist_ok=True)
    
    with open(temp_file, 'w') as f:
        for profile in profiles:
            f.write(json.dumps(profile) + '\n')
    
    print(f"✓ Published {len(profiles)} user profiles")
    print("✓ Profiles published (static stream)")
    
    # Keep thread alive
    while True:
        time.sleep(60)


# ============================================================================
# PATHWAY PUBLISHERS
# ============================================================================

def run_pathway_login_publisher():
    """Pathway-based login attempts publisher"""
    
    temp_file = "./publisher/temp_ato_logins.jsonl"
    
    logins = pw.io.jsonlines.read(
        temp_file,
        schema=LoginAttemptSchema,
        mode="streaming"
    )
    
    pw.io.nats.write(
        logins,
        uri=NATS_URI,
        topic=ATO_LOGIN_ATTEMPTS_TOPIC
    )
    
    pw.run(monitoring_level=pw.MonitoringLevel.NONE)


def run_pathway_profile_publisher():
    """Pathway-based user profiles publisher"""
    
    temp_file = "./publisher/temp_ato_profiles.jsonl"
    
    profiles = pw.io.jsonlines.read(
        temp_file,
        schema=UserProfileSchema,
        mode="static"
    )
    
    pw.io.nats.write(
        profiles,
        uri=NATS_URI,
        topic=ATO_USER_PROFILES_TOPIC
    )
    
    pw.run(monitoring_level=pw.MonitoringLevel.NONE)


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    print("Starting ATO publishers...")
    
    # Start data generators
    t1 = threading.Thread(target=run_login_attempts_publisher, args=(10,), daemon=True)
    t2 = threading.Thread(target=run_user_profiles_publisher, daemon=True)
    
    t1.start()
    t2.start()
    
    time.sleep(2)  # Let data accumulate
    
    # Start Pathway publishers
    t3 = threading.Thread(target=run_pathway_login_publisher, daemon=True)
    t4 = threading.Thread(target=run_pathway_profile_publisher, daemon=True)
    
    t3.start()
    t4.start()
    
    print("\n✓ All ATO publishers active")
    print("Press Ctrl+C to stop\n")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 Shutting down ATO publishers...")