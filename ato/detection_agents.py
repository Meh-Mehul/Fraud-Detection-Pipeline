"""
Transaction Fraud Detection Agents (Pathway UDFs)
Implements 5 specialized detection agents adapted for transaction data:
1. Amount Anomaly Detector
2. Balance Inconsistency Analyzer
3. Transaction Type Pattern Detector
4. Transaction Frequency Profiler
5. Account Behavior Monitor

Each agent outputs a score (0-100) and confidence level (0-100)
"""

import pathway as pw
import json


@pw.udf
def amount_anomaly_agent(
    amount: float,
    avg_amount_history: float,
    oldbalanceOrg: float,
    newbalanceOrig: float,
    tx_type: str
) -> str:
    """
    Detects anomalies in transaction amounts

    Signals:
    - Transaction amount much higher than average
    - Amount exceeds account balance
    - Suspiciously round amounts
    - Zero-balance transactions
    """
    score = 0.0
    confidence = 80.0
    reasons = []

    # Check if amount is anomalously high
    if avg_amount_history > 0:
        ratio = amount / avg_amount_history
        if ratio > 10:
            score += 45
            reasons.append(f"Amount {ratio:.1f}x higher than average")
            confidence += 10
        elif ratio > 5:
            score += 30
            reasons.append(f"Amount significantly higher than average")
            confidence += 5
        elif ratio > 3:
            score += 15
            reasons.append(f"Amount elevated compared to average")

    # Check balance consistency
    expected_new_balance = oldbalanceOrg - amount
    if abs(newbalanceOrig - expected_new_balance) > 0.01 and oldbalanceOrg > 0:
        score += 40
        reasons.append(f"Balance inconsistency detected")
        confidence += 15

    # Suspiciously round amounts
    if amount > 1000 and amount % 1000 == 0:
        score += 20
        reasons.append(f"Suspiciously round amount: ${amount:.2f}")
        confidence += 5

    # Large transaction
    if amount > 200000:
        score += 35
        reasons.append(f"Very large transaction: ${amount:.2f}")
        confidence += 10

    # Empty account after transaction
    if newbalanceOrig == 0 and oldbalanceOrg > 0:
        score += 25
        reasons.append("Account drained to zero")
        confidence += 10

    score = min(100, score)
    confidence = min(100, max(20, confidence))

    return json.dumps({
        "score": score,
        "confidence": confidence,
        "reasons": reasons
    })


@pw.udf
def balance_inconsistency_agent(
    oldbalanceOrg: float,
    newbalanceOrig: float,
    amount: float,
    oldbalanceDest: float,
    newbalanceDest: float
) -> str:
    """
    Analyzes balance inconsistencies in transactions
    """
    score = 0.0
    confidence = 90.0
    reasons = []

    # Check origin balance consistency
    expected_new_balance_orig = oldbalanceOrg - amount
    if abs(newbalanceOrig - expected_new_balance_orig) > 0.01 and oldbalanceOrg > 0:
        score += 50
        reasons.append(f"Origin balance math doesn't add up")
        confidence += 10

    # Check destination balance consistency
    expected_new_balance_dest = oldbalanceDest + amount
    if abs(newbalanceDest - expected_new_balance_dest) > 0.01 and expected_new_balance_dest > 0:
        score += 50
        reasons.append(f"Destination balance math doesn't add up")
        confidence += 10

    # Negative balance
    if newbalanceOrig < 0:
        score += 100
        reasons.append("Negative balance - critical error")
        confidence = 100

    # Account drained
    if oldbalanceOrg > 10000 and newbalanceOrig == 0:
        score += 40
        reasons.append("Large account completely drained")
        confidence += 5

    score = min(100, score)
    confidence = min(100, max(20, confidence))

    return json.dumps({
        "score": score,
        "confidence": confidence,
        "reasons": reasons
    })


@pw.udf
def transaction_type_agent(
    tx_type: str,
    amount: float,
    tx_velocity_5min: int
) -> str:
    """
    Detects suspicious transaction type patterns
    """
    score = 0.0
    confidence = 75.0
    reasons = []

    # TRANSFER and CASH_OUT are higher risk
    if tx_type in ["TRANSFER", "CASH_OUT"]:
        score += 20
        reasons.append(f"High-risk transaction type: {tx_type}")
        confidence += 10

    # Large CASH_OUT
    if tx_type == "CASH_OUT" and amount > 100000:
        score += 40
        reasons.append("Large cash-out transaction")
        confidence += 15

    # High velocity TRANSFERs
    if tx_type == "TRANSFER" and tx_velocity_5min > 5:
        score += 35
        reasons.append(f"Rapid transfer pattern: {tx_velocity_5min} in 5 min")
        confidence += 10

    score = min(100, score)
    confidence = min(100, max(20, confidence))

    return json.dumps({
        "score": score,
        "confidence": confidence,
        "reasons": reasons
    })


@pw.udf
def transaction_frequency_agent(
    tx_velocity_5min: int,
    tx_velocity_1hour: int,
    total_amount_history: float
) -> str:
    """
    Analyzes transaction frequency patterns
    """
    score = 0.0
    confidence = 70.0
    reasons = []

    if tx_velocity_1hour >= 30:
        score += 40
        reasons.append(f"Extreme transaction frequency: {tx_velocity_1hour} txs in 1 hour")
        confidence += 15
    elif tx_velocity_1hour >= 15:
        score += 25
        reasons.append(f"High transaction frequency: {tx_velocity_1hour} txs in 1 hour")
        confidence += 10
    elif tx_velocity_1hour >= 8:
        score += 15
        reasons.append(f"Elevated transaction frequency: {tx_velocity_1hour} txs")
        confidence += 5

    if tx_velocity_5min >= 5 and tx_velocity_1hour < 10:
        score += 20
        reasons.append("Burst pattern detected (sudden activity spike)")
        confidence += 5

    # High total amount moved
    if total_amount_history > 1000000:
        score += 30
        reasons.append(f"Very high total transaction volume: ${total_amount_history:.2f}")
        confidence += 10

    score = min(100, score)
    confidence = min(100, max(20, confidence))

    return json.dumps({
        "score": score,
        "confidence": confidence,
        "reasons": reasons
    })


@pw.udf
def account_behavior_agent(
    oldbalanceOrg: float,
    newbalanceOrig: float,
    amount: float,
    tx_type: str
) -> str:
    """
    Monitors overall account behavior patterns
    """
    score = 0.0
    confidence = 65.0
    reasons = []

    # Large portion of balance moved
    if oldbalanceOrg > 0:
        portion = amount / oldbalanceOrg
        if portion > 0.9:
            score += 35
            reasons.append(f"Moving {portion*100:.0f}% of account balance")
            confidence += 15
        elif portion > 0.7:
            score += 20
            reasons.append(f"Moving large portion of balance")
            confidence += 10

    # Account with very high balance
    if oldbalanceOrg > 1000000:
        score += 15
        reasons.append("High-value account transaction")
        confidence += 5

    # Complete account drain
    if oldbalanceOrg > 0 and newbalanceOrig == 0:
        score += 40
        reasons.append("Account completely emptied")
        confidence += 15

    score = min(100, score)
    confidence = min(100, max(20, confidence))

    return json.dumps({
        "score": score,
        "confidence": confidence,
        "reasons": reasons
    })


def apply_detection_agents(enriched_stream: pw.Table, login_stream: pw.Table = None, profile_stream: pw.Table = None) -> pw.Table:
    """
    Apply all 5 detection agents to enriched transaction stream

    Args:
        enriched_stream: Enriched transaction data with velocity features
        login_stream: Not used (kept for compatibility)
        profile_stream: Not used (kept for compatibility)

    Returns:
        Table with all agent scores and confidence levels
    """
    
    with_agents = enriched_stream.select(
        step=enriched_stream.step,
        type=enriched_stream.type,
        amount=enriched_stream.amount,
        nameOrig=enriched_stream.nameOrig,
        oldbalanceOrg=enriched_stream.oldbalanceOrg,
        newbalanceOrig=enriched_stream.newbalanceOrig,
        nameDest=enriched_stream.nameDest,
        oldbalanceDest=enriched_stream.oldbalanceDest,
        newbalanceDest=enriched_stream.newbalanceDest,
        isFraud=enriched_stream.isFraud,
        isFlaggedFraud=enriched_stream.isFlaggedFraud,
        tx_velocity_5min=enriched_stream.tx_velocity_5min,
        tx_velocity_1hour=enriched_stream.tx_velocity_1hour,
        total_amount_history=enriched_stream.total_amount_history,
        avg_amount_history=enriched_stream.avg_amount_history,
        location_result=amount_anomaly_agent(
            enriched_stream.amount,
            enriched_stream.avg_amount_history,
            enriched_stream.oldbalanceOrg,
            enriched_stream.newbalanceOrig,
            enriched_stream.type
        ),
        device_result=balance_inconsistency_agent(
            enriched_stream.oldbalanceOrg,
            enriched_stream.newbalanceOrig,
            enriched_stream.amount,
            enriched_stream.oldbalanceDest,
            enriched_stream.newbalanceDest
        ),
        credential_result=transaction_type_agent(
            enriched_stream.type,
            enriched_stream.amount,
            enriched_stream.tx_velocity_5min
        ),
        frequency_result=transaction_frequency_agent(
            enriched_stream.tx_velocity_5min,
            enriched_stream.tx_velocity_1hour,
            enriched_stream.total_amount_history
        ),
        biometric_result=account_behavior_agent(
            enriched_stream.oldbalanceOrg,
            enriched_stream.newbalanceOrig,
            enriched_stream.amount,
            enriched_stream.type
        )
    )

    return with_agents
