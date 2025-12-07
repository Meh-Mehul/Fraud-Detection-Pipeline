"""
Streaming Enrichment Layer for ATO Detection
Implements activity velocity tracking and feature enrichment
Combines login attempts with user profiles and computes behavioral features
"""

import pathway as pw
from pathway import JoinMode
import json
import math
from datetime import datetime
from typing import List, Dict, Any

from ato.ato_schema import LoginAttemptSchema, UserAccountProfileSchema, EnrichedLoginSchema, TransactionSchema


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate the great circle distance between two points on earth (in kilometers)
    """
    try:
        lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
        c = 2 * math.asin(math.sqrt(a))
        r = 6371
        return c * r
    except:
        return 0.0


def parse_json_safe(json_str: str) -> List[Any]:
    """Safely parse JSON string, return empty list on error"""
    try:
        return json.loads(json_str) if json_str else []
    except:
        return []


@pw.udf
def extract_hour_from_timestamp(unix_time: int) -> int:
    """Extract hour of day from Unix timestamp"""
    try:
        return datetime.fromtimestamp(unix_time).hour
    except:
        return 0


@pw.udf
def calculate_distance_from_typical(latitude: float, longitude: float, typical_locations_json: str) -> float:
    """
    Calculate minimum distance from current location to any typical location
    typical_locations_json: JSON array of {"lat": x, "lon": y} objects
    """
    typical_locations = parse_json_safe(typical_locations_json)

    if not typical_locations:
        return 0.0

    min_distance = float('inf')

    for loc in typical_locations:
        try:
            typical_lat = float(loc.get('lat', 0))
            typical_lon = float(loc.get('lon', 0))
            distance = haversine_distance(latitude, longitude, typical_lat, typical_lon)
            min_distance = min(min_distance, distance)
        except:
            continue

    return min_distance if min_distance != float('inf') else 0.0


@pw.udf
def is_new_device(device_id: str, typical_devices_json: str) -> bool:
    """Check if device_id is not in the list of typical devices"""
    typical_devices = parse_json_safe(typical_devices_json)
    return device_id not in typical_devices


@pw.udf
def is_unusual_hour(unix_time: int, typical_hours_json: str) -> bool:
    """
    Check if login hour is unusual based on typical login hours
    typical_hours_json: JSON array of hour integers [9, 10, 11, 14, 15, ...]
    """
    hour = extract_hour_from_timestamp(unix_time)
    typical_hours = parse_json_safe(typical_hours_json)

    if not typical_hours:
        return False

    return pw.apply(lambda h: h not in typical_hours, hour)


@pw.udf
def calculate_account_age_days(created_at: int, current_time: int) -> int:
    """Calculate account age in days"""
    try:
        age_seconds = current_time - created_at
        return max(0, age_seconds // 86400)
    except:
        return 0


class StreamingEnrichmentLayer:
    """
    Implements the Streaming Enrichment Layer from the architecture:
    - Activity Velocity (stateful aggregation)
    - Feature Enrichment (Pathway Join)
    - Combines login data with user metadata & velocity
    """

    @staticmethod
    def compute_activity_velocity(login_stream: pw.Table) -> pw.Table:
        """
        Compute activity velocity metrics using stateful aggregation
        Tracks login rate over different time windows (5 min, 1 hour)

        Note: Simplified version using groupby instead of temporal windows
        since temporal windows require datetime types, not unix timestamps
        """
        velocity_stats = login_stream.groupby(login_stream.user_id).reduce(
            user_id=pw.this.user_id,
            login_velocity_5min=pw.reducers.count(),
            failed_attempts_5min=pw.reducers.sum(
                pw.if_else(pw.this.login_status == "failed", 1, 0)
            ),
            login_velocity_1hour=pw.reducers.count()
        )

        return velocity_stats, velocity_stats

    @staticmethod
    def enrich_with_profile(
        login_stream: pw.Table,
        profile_stream: pw.Table,
        velocity_5min: pw.Table,
        velocity_1hour: pw.Table
    ) -> pw.Table:
        """
        Enrich login attempts with user profile data and velocity metrics
        Implements the Feature Enrichment step from architecture
        """
        enriched = login_stream.join(
            profile_stream,
            login_stream.user_id == profile_stream.user_id,
            how=JoinMode.LEFT
        ).select(
            attempt_id=login_stream.attempt_id,
            user_id=login_stream.user_id,
            timestamp=login_stream.timestamp,
            login_status=login_stream.login_status,
            device_id=login_stream.device_id,
            latitude=login_stream.latitude,
            longitude=login_stream.longitude,
            country=login_stream.country,
            is_vpn=login_stream.is_vpn,
            typical_locations=profile_stream.typical_locations,
            typical_devices=profile_stream.typical_devices,
            typical_hours=profile_stream.typical_login_hours,
            account_created_at=profile_stream.account_created_at,
            mfa_enabled=profile_stream.mfa_enabled,
            previous_ato_incidents=profile_stream.previous_ato_incidents,
        )

        enriched = enriched.select(
            *pw.this,
            distance_from_typical_location_km=calculate_distance_from_typical(
                pw.this.latitude,
                pw.this.longitude,
                pw.this.typical_locations
            ),
            is_new_device=is_new_device(
                pw.this.device_id,
                pw.this.typical_devices
            ),
            is_unusual_hour=is_unusual_hour(
                pw.this.timestamp,
                pw.this.typical_hours
            ),
            account_age_days=calculate_account_age_days(
                pw.this.account_created_at,
                pw.this.timestamp
            )
        )

        enriched = enriched.join(
            velocity_5min,
            enriched.user_id == velocity_5min.user_id,
            how=JoinMode.LEFT
        ).select(
            attempt_id=enriched.attempt_id,
            user_id=enriched.user_id,
            timestamp=enriched.timestamp,
            login_status=enriched.login_status,
            device_id=enriched.device_id,
            latitude=enriched.latitude,
            longitude=enriched.longitude,
            country=enriched.country,
            is_vpn=enriched.is_vpn,
            login_velocity_5min=pw.coalesce(velocity_5min.login_velocity_5min, 0),
            login_velocity_1hour=pw.coalesce(velocity_5min.login_velocity_1hour, 0),
            failed_attempts_5min=pw.coalesce(velocity_5min.failed_attempts_5min, 0),
            distance_from_typical_location_km=enriched.distance_from_typical_location_km,
            is_new_device=enriched.is_new_device,
            is_unusual_hour=enriched.is_unusual_hour,
            account_age_days=enriched.account_age_days,
            mfa_enabled=enriched.mfa_enabled,
            previous_ato_incidents=enriched.previous_ato_incidents
        )

        return enriched

    @staticmethod
    def compute_transaction_velocity(transaction_stream: pw.Table) -> pw.Table:
        """
        Compute transaction velocity metrics
        """
        velocity_stats = transaction_stream.groupby(transaction_stream.nameOrig).reduce(
            nameOrig=pw.this.nameOrig,
            tx_count=pw.reducers.count(),
            total_amount=pw.reducers.sum(pw.this.amount)
        )
        return velocity_stats

    @staticmethod
    def enrich_transaction(
        transaction_stream: pw.Table,
        velocity_stats: pw.Table
    ) -> pw.Table:
        """
        Enrich transactions with velocity
        """
        enriched = transaction_stream.join(
            velocity_stats,
            transaction_stream.nameOrig == velocity_stats.nameOrig,
            how=JoinMode.LEFT
        ).select(
            *transaction_stream,
            tx_count=pw.coalesce(velocity_stats.tx_count, 0),
            total_amount_history=pw.coalesce(velocity_stats.total_amount, 0)
        )
        return enriched

    @staticmethod
    def build_transaction_enrichment_pipeline(
        transaction_stream: pw.Table
    ) -> pw.Table:
        # Simplified for performance: skip heavy aggregation on 6M rows for this demo
        # velocity = StreamingEnrichmentLayer.compute_transaction_velocity(transaction_stream)
        # enriched = StreamingEnrichmentLayer.enrich_transaction(transaction_stream, velocity)
        
        # Just pass through with dummy columns
        enriched = transaction_stream.select(
            *transaction_stream,
            tx_count=pw.apply(lambda x: 0, transaction_stream.step),
            total_amount_history=pw.apply(lambda x: 0.0, transaction_stream.amount)
        )
        return enriched

    @staticmethod
    def build_enrichment_pipeline(
        login_stream: pw.Table,
        profile_stream: pw.Table
    ) -> pw.Table:
        """
        Main entry point: builds complete enrichment pipeline
        Returns enriched login stream with all features
        """
        velocity_5min, velocity_1hour = StreamingEnrichmentLayer.compute_activity_velocity(login_stream)

        enriched_stream = StreamingEnrichmentLayer.enrich_with_profile(
            login_stream,
            profile_stream,
            velocity_5min,
            velocity_1hour
        )

        return enriched_stream
