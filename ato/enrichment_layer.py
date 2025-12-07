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
        Tracks transaction rate for each account (nameOrig)
        """
        velocity_stats = login_stream.groupby(login_stream.nameOrig).reduce(
            nameOrig=pw.this.nameOrig,
            tx_velocity_5min=pw.reducers.count(),
            failed_tx_5min=pw.reducers.sum(
                pw.if_else(pw.this.type == "TRANSFER", 1, 0)
            ),
            tx_velocity_1hour=pw.reducers.count(),
            total_amount=pw.reducers.sum(pw.this.amount),
            avg_amount=pw.reducers.avg(pw.this.amount)
        )

        return velocity_stats, velocity_stats

    @staticmethod
    def enrich_with_profile(
        login_stream: pw.Table,
        velocity_5min: pw.Table,
        velocity_1hour: pw.Table
    ) -> pw.Table:
        """
        Enrich transactions with velocity metrics
        """
        enriched = login_stream.join(
            velocity_5min,
            login_stream.nameOrig == velocity_5min.nameOrig,
            how=JoinMode.LEFT
        ).select(
            step=login_stream.step,
            type=login_stream.type,
            amount=login_stream.amount,
            nameOrig=login_stream.nameOrig,
            oldbalanceOrg=login_stream.oldbalanceOrg,
            newbalanceOrig=login_stream.newbalanceOrig,
            nameDest=login_stream.nameDest,
            oldbalanceDest=login_stream.oldbalanceDest,
            newbalanceDest=login_stream.newbalanceDest,
            isFraud=login_stream.isFraud,
            isFlaggedFraud=login_stream.isFlaggedFraud,
            tx_velocity_5min=pw.coalesce(velocity_5min.tx_velocity_5min, 0),
            tx_velocity_1hour=pw.coalesce(velocity_5min.tx_velocity_1hour, 0),
            failed_tx_5min=pw.coalesce(velocity_5min.failed_tx_5min, 0),
            total_amount_history=pw.coalesce(velocity_5min.total_amount, 0.0),
            avg_amount_history=pw.coalesce(velocity_5min.avg_amount, 0.0)
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

        enriched = transaction_stream.select(
            *transaction_stream,
            tx_count=pw.apply(lambda x: 0, transaction_stream.step),
            total_amount_history=pw.apply(lambda x: 0.0, transaction_stream.amount)
        )
        return enriched

    @staticmethod
    def build_enrichment_pipeline(
        login_stream: pw.Table
    ) -> pw.Table:
        """
        Main entry point: builds complete enrichment pipeline
        Returns enriched transaction stream with velocity features
        """
        velocity_5min, velocity_1hour = StreamingEnrichmentLayer.compute_activity_velocity(login_stream)

        enriched_stream = StreamingEnrichmentLayer.enrich_with_profile(
            login_stream,
            velocity_5min,
            velocity_1hour
        )

        return enriched_stream
