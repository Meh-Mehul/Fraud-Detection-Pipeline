"""
Sample data generator for testing ATO detection pipeline
Generates synthetic login attempts and user profiles for Kafka topics
"""

import json
import random
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any


class ATODataGenerator:
    """Generates realistic test data for ATO detection pipeline"""

    def __init__(self):
        self.user_ids = [f"user_{i:04d}" for i in range(1, 101)]
        self.cities = [
            {"name": "New York", "country": "USA", "lat": 40.7128, "lon": -74.0060},
            {"name": "London", "country": "UK", "lat": 51.5074, "lon": -0.1278},
            {"name": "Tokyo", "country": "Japan", "lat": 35.6762, "lon": 139.6503},
            {"name": "Sydney", "country": "Australia", "lat": -33.8688, "lon": 151.2093},
            {"name": "Mumbai", "country": "India", "lat": 19.0760, "lon": 72.8777},
            {"name": "Berlin", "country": "Germany", "lat": 52.5200, "lon": 13.4050},
        ]
        self.devices = ["mobile", "desktop", "tablet"]
        self.browsers = ["Chrome", "Firefox", "Safari", "Edge"]
        self.os_list = ["Windows", "macOS", "Linux", "iOS", "Android"]

    def generate_user_profile(self, user_id: str) -> Dict[str, Any]:
        """Generate a user account profile"""
        typical_city = random.choice(self.cities)
        typical_locations = [
            {"lat": typical_city["lat"], "lon": typical_city["lon"]}
        ]
        if random.random() > 0.5:
            other_city = random.choice([c for c in self.cities if c != typical_city])
            typical_locations.append({"lat": other_city["lat"], "lon": other_city["lon"]})
        num_devices = random.randint(1, 3)
        typical_devices = [f"device_{user_id}_{i}" for i in range(num_devices)]
        typical_hours = [9, 10, 11, 12, 13, 14, 15, 16, 17, 18]
        if random.random() > 0.7:
            typical_hours.extend([20, 21, 22])
        account_age_days = random.randint(7, 730)
        created_at = int((datetime.now() - timedelta(days=account_age_days)).timestamp())
        return {
            "user_id": user_id,
            "account_created_at": created_at,
            "last_updated": int(datetime.now().timestamp()),
            "account_status": "active",
            "is_verified": random.random() > 0.2,
            "mfa_enabled": random.random() > 0.4,
            "risk_score": random.uniform(0, 30),
            "typical_login_hours": json.dumps(typical_hours),
            "typical_locations": json.dumps(typical_locations),
            "typical_devices": json.dumps(typical_devices),
            "total_login_count": random.randint(50, 5000),
            "failed_login_count_24h": random.randint(0, 2),
            "successful_login_count_24h": random.randint(1, 10),
            "last_successful_login": int((datetime.now() - timedelta(hours=random.randint(1, 48))).timestamp()),
            "last_failed_login": int((datetime.now() - timedelta(hours=random.randint(24, 168))).timestamp()),
            "primary_country": typical_city["country"],
            "primary_device_type": random.choice(self.devices),
            "average_session_duration": random.randint(300, 3600),
            "password_last_changed": int((datetime.now() - timedelta(days=random.randint(30, 365))).timestamp()),
            "security_questions_set": random.random() > 0.3,
            "previous_ato_incidents": 0 if random.random() > 0.05 else random.randint(1, 2),
            "flagged_for_review": False
        }

    def generate_normal_login(self, user_id: str, profile: Dict[str, Any]) -> Dict[str, Any]:
        """Generate a normal (legitimate) login attempt"""
        typical_locations = json.loads(profile["typical_locations"])
        typical_devices = json.loads(profile["typical_devices"])
        typical_hours = json.loads(profile["typical_login_hours"])
        if random.random() < 0.9 and typical_locations:
            location = random.choice(typical_locations)
            city = next((c for c in self.cities if abs(c["lat"] - location["lat"]) < 1), self.cities[0])
        else:
            city = random.choice(self.cities)
        device_id = random.choice(typical_devices) if (random.random() < 0.95 and typical_devices) else f"device_new_{random.randint(1000, 9999)}"
        if random.random() < 0.9 and typical_hours:
            hour = random.choice(typical_hours)
        else:
            hour = random.randint(0, 23)
        now = datetime.now()
        login_time = now.replace(hour=hour, minute=random.randint(0, 59), second=random.randint(0, 59))
        attempt_id = f"login_{int(time.time() * 1000)}_{random.randint(1000, 9999)}"
        return {
            "attempt_id": attempt_id,
            "user_id": user_id,
            "username": f"{user_id}@example.com",
            "timestamp": int(login_time.timestamp()),
            "login_status": "success",
            "failure_reason": "",
            "attempt_count": 1,
            "device_id": device_id,
            "device_type": random.choice(self.devices),
            "os": random.choice(self.os_list),
            "browser": random.choice(self.browsers),
            "browser_version": f"{random.randint(90, 120)}.0",
            "user_agent": f"Mozilla/5.0 ({random.choice(self.os_list)}) Chrome/{random.randint(90, 120)}.0",
            "screen_resolution": random.choice(["1920x1080", "1366x768", "2560x1440"]),
            "ip_address": f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}",
            "latitude": city["lat"] + random.uniform(-0.1, 0.1),
            "longitude": city["lon"] + random.uniform(-0.1, 0.1),
            "city": city["name"],
            "country": city["country"],
            "isp": random.choice(["Comcast", "Verizon", "AT&T", "BT", "Vodafone"]),
            "is_vpn": False,
            "is_proxy": False,
            "is_tor": False,
            "password_hash_prefix": f"{random.randint(1000, 9999):04x}",
            "credential_breach_found": False,
            "typing_pattern_score": random.uniform(0.0, 0.3),
            "mouse_pattern_score": random.uniform(0.0, 0.3),
            "session_duration": random.randint(120, 1800),
            "session_id": f"session_{random.randint(100000, 999999)}",
            "referrer_url": "https://example.com/login",
            "login_method": "password"
        }

    def generate_suspicious_login(self, user_id: str, profile: Dict[str, Any], attack_type: str) -> Dict[str, Any]:
        """Generate a suspicious/fraudulent login attempt"""
        login = self.generate_normal_login(user_id, profile)
        if attack_type == "location_anomaly":
            typical_country = profile["primary_country"]
            different_city = random.choice([c for c in self.cities if c["country"] != typical_country])
            login["latitude"] = different_city["lat"]
            login["longitude"] = different_city["lon"]
            login["city"] = different_city["name"]
            login["country"] = different_city["country"]
            login["is_vpn"] = random.random() > 0.5
        elif attack_type == "new_device":
            login["device_id"] = f"device_unknown_{random.randint(1000, 9999)}"
            login["device_type"] = random.choice(self.devices)
        elif attack_type == "credential_stuffing":
            login["credential_breach_found"] = True
            login["attempt_count"] = random.randint(3, 10)
            login["login_status"] = "success" if random.random() > 0.7 else "failed"
            login["failure_reason"] = "wrong_password" if login["login_status"] == "failed" else ""
        elif attack_type == "high_velocity":
            login["attempt_count"] = random.randint(5, 20)
            login["session_duration"] = random.randint(2, 10)
        elif attack_type == "bot_behavior":
            login["typing_pattern_score"] = random.uniform(0.7, 0.95)
            login["mouse_pattern_score"] = random.uniform(0.7, 0.95)
            login["session_duration"] = random.randint(1, 5)
            login["user_agent"] = "Python-urllib/3.8 (automated)"
        elif attack_type == "combined":
            different_city = random.choice([c for c in self.cities if c["country"] != profile["primary_country"]])
            login["latitude"] = different_city["lat"]
            login["longitude"] = different_city["lon"]
            login["city"] = different_city["name"]
            login["country"] = different_city["country"]
            login["is_vpn"] = True
            login["device_id"] = f"device_unknown_{random.randint(1000, 9999)}"
            login["credential_breach_found"] = True
            login["typing_pattern_score"] = random.uniform(0.7, 0.95)
            login["session_duration"] = random.randint(1, 5)
        return login

    def generate_test_dataset(self, num_logins: int = 1000, fraud_rate: float = 0.1) -> tuple:
        """
        Generate a complete test dataset

        Args:
            num_logins: Total number of login attempts to generate
            fraud_rate: Proportion of fraudulent logins (0.0 to 1.0)

        Returns:
            (profiles, logins) - Lists of user profiles and login attempts
        """
        profiles = [self.generate_user_profile(uid) for uid in self.user_ids]
        profile_map = {p["user_id"]: p for p in profiles}
        logins = []
        num_fraud = int(num_logins * fraud_rate)
        num_normal = num_logins - num_fraud
        attack_types = ["location_anomaly", "new_device", "credential_stuffing",
                       "high_velocity", "bot_behavior", "combined"]
        for _ in range(num_normal):
            user_id = random.choice(self.user_ids)
            login = self.generate_normal_login(user_id, profile_map[user_id])
            logins.append(login)
        for _ in range(num_fraud):
            user_id = random.choice(self.user_ids)
            attack_type = random.choice(attack_types)
            login = self.generate_suspicious_login(user_id, profile_map[user_id], attack_type)
            logins.append(login)
        random.shuffle(logins)
        return profiles, logins


def save_to_jsonl(data: List[Dict], filename: str):
    """Save data to JSONL format"""
    with open(filename, 'w') as f:
        for item in data:
            f.write(json.dumps(item) + '\n')


def main():
    """Generate sample datasets"""
    print("Generating ATO test datasets...")
    generator = ATODataGenerator()
    profiles, logins = generator.generate_test_dataset(
        num_logins=1000,
        fraud_rate=0.15
    )
    save_to_jsonl(profiles, "ato_user_profiles_sample.jsonl")
    save_to_jsonl(logins, "ato_login_attempts_sample.jsonl")
    print(f"Generated {len(profiles)} user profiles")
    print(f"Generated {len(logins)} login attempts")
    print(f"   - Normal logins: {sum(1 for l in logins if not l.get('credential_breach_found', False))}")
    print(f"   - Suspicious logins: {sum(1 for l in logins if l.get('credential_breach_found', False))}")
    print()
    print("Files created:")
    print("   - ato_user_profiles_sample.jsonl")
    print("   - ato_login_attempts_sample.jsonl")
    print()
    print("Use these files to test the ATO detection pipeline!")


if __name__ == "__main__":
    main()
