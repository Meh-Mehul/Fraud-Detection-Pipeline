"""
Enhanced Fraud Rules Loader
Supports both Report Generation and Detector Logic
Loads fraud detection rules and configurations from JSON
"""

import json
from pathlib import Path
from typing import Dict, Any, List, Tuple


class FraudRulesLoader:
    """Loads and manages fraud detection rules for both reports and detector"""
    
    def __init__(self, rules_file: str = None):
        if rules_file is None:
            # Default to shared/fraud_rules.json relative to this file
            current_dir = Path(__file__).parent
            rules_file = current_dir / "fraud_rules.json"
        
        self.rules_file = Path(rules_file)
        self.rules = self._load_rules()
    
    def _load_rules(self) -> Dict[str, Any]:
        """Load rules from JSON file"""
        try:
            with open(self.rules_file, 'r') as f:
                rules = json.load(f)
            print(f"✓ Loaded fraud rules from: {self.rules_file}")
            return rules
        except FileNotFoundError:
            print(f"❌ Rules file not found: {self.rules_file}")
            raise
        except json.JSONDecodeError as e:
            print(f"❌ Error parsing rules JSON: {e}")
            raise
    
    # ========================================================================
    # REPORT GENERATION METHODS (Original)
    # ========================================================================
    
    def get_indicator_details(self) -> Dict[str, Dict[str, str]]:
        """Get all indicator details"""
        return self.rules.get("indicator_details", {})
    
    def get_indicator(self, indicator_code: str) -> Dict[str, str]:
        """Get details for a specific indicator"""
        return self.rules.get("indicator_details", {}).get(indicator_code)
    
    def get_tier_info(self) -> Dict[str, Dict[str, str]]:
        """Get tier information"""
        return self.rules.get("tier_info", {})
    
    def get_tier(self, tier_num: int) -> Dict[str, str]:
        """Get information for a specific tier"""
        return self.rules.get("tier_info", {}).get(str(tier_num), {
            'name': 'Unknown Tier',
            'description': 'N/A',
            'action': 'Review required'
        })
    
    def get_investigation_protocol(self) -> Dict[str, List[str]]:
        """Get investigation protocol guidelines"""
        return self.rules.get("investigation_protocol", {})
    
    def get_immediate_actions(self) -> List[str]:
        """Get immediate action items"""
        return self.rules.get("investigation_protocol", {}).get("immediate_actions", [])
    
    def get_short_term_actions(self) -> List[str]:
        """Get short-term action items"""
        return self.rules.get("investigation_protocol", {}).get("short_term_actions", [])
    
    def get_verification_questions(self) -> List[str]:
        """Get customer verification questions"""
        return self.rules.get("investigation_protocol", {}).get("verification_questions", [])
    
    def format_verification_questions(self, merchant: str, amount: float, location: str) -> List[str]:
        """Format verification questions with transaction details"""
        questions = self.get_verification_questions()
        formatted = []
        for q in questions:
            formatted_q = q.replace("{merchant}", merchant)
            formatted_q = formatted_q.replace("{amount}", f"{amount:.2f}")
            formatted_q = formatted_q.replace("{location}", location)
            formatted.append(formatted_q)
        return formatted
    
    def get_risk_mitigation(self) -> Dict[str, List[str]]:
        """Get risk mitigation strategies"""
        return self.rules.get("risk_mitigation", {})
    
    def get_disposition_scenarios(self) -> Dict[str, List[str]]:
        """Get case disposition scenarios"""
        return self.rules.get("disposition_scenarios", {})
    
    def get_risk_thresholds(self) -> Dict[str, int]:
        """Get risk score thresholds"""
        return self.rules.get("risk_thresholds", {
            "extreme": 90,
            "critical": 80,
            "high": 70,
            "elevated": 60
        })
    
    def get_risk_level(self, risk_score: int) -> str:
        """Determine risk level from score"""
        thresholds = self.get_risk_thresholds()
        if risk_score >= thresholds.get("extreme", 90):
            return "EXTREME"
        elif risk_score >= thresholds.get("critical", 80):
            return "CRITICAL"
        elif risk_score >= thresholds.get("high", 70):
            return "HIGH"
        else:
            return "ELEVATED"
    
    def get_ml_thresholds(self) -> Dict[str, int]:
        """Get ML confidence thresholds"""
        return self.rules.get("ml_thresholds", {
            "high_confidence": 80,
            "medium_confidence": 60
        })
    
    def decode_ml_indicator(self, indicator_code: str, value: str = None) -> Dict[str, Any]:
        """Decode ML-specific indicator"""
        thresholds = self.get_ml_thresholds()
        
        # Extract score from code or use value
        score = indicator_code[2:] if len(indicator_code) > 2 else value
        
        severity = "HIGH" if score and float(score) > thresholds.get("high_confidence", 80) else "MEDIUM"
        
        return {
            'indicator': indicator_code if not value else f"ML({value})",
            'base': 'ML',
            'value': score,
            'name': 'ML Detection',
            'severity': severity,
            'description': f'Machine learning model confidence: {score}%' if score else 'ML anomaly detected',
            'risk': 'AI detected behavioral patterns inconsistent with legitimate transactions'
        }
    
    def get_severity_counts(self, decoded_indicators: List[Dict]) -> Dict[str, int]:
        """Count indicators by severity"""
        counts = {'CRITICAL': 0, 'HIGH': 0, 'MEDIUM': 0, 'UNKNOWN': 0}
        for ind in decoded_indicators:
            severity = ind.get('severity', 'UNKNOWN')
            if severity in counts:
                counts[severity] += 1
            else:
                counts['UNKNOWN'] += 1
        return counts
    
    # ========================================================================
    # DETECTOR METHODS (New)
    # ========================================================================
    
    def get_detector_rules(self) -> Dict[str, Any]:
        """Get all detector rules"""
        return self.rules.get("detector_rules", {})
    
    def get_tier1_rules(self) -> Dict[str, Any]:
        """Get Tier 1 (Critical) detection rules"""
        return self.get_detector_rules().get("tier1_rules", {})
    
    def get_tier2_rules(self) -> Dict[str, Any]:
        """Get Tier 2 (Score-based) detection rules"""
        return self.get_detector_rules().get("tier2_rules", {})
    
    def get_tier3_rules(self) -> Dict[str, Any]:
        """Get Tier 3 (ML-based) detection rules"""
        return self.get_detector_rules().get("tier3_rules", {})
    
    def check_tier1_extreme_signals(self, z_amt: float, amt: float, z_dist: float, 
                                distance: float, merch_fraud_rate: float, 
                                merch_total: int, fraud_history: int) -> List[str]:

        tier1 = self.get_tier1_rules()
        extreme_signals = []
        
        if not tier1:
            return extreme_signals
        
        # Amount checks
        for check in tier1.get("extreme_signals", {}).get("amount_checks", []):
            if check["code"] == "MASSIVE_AMT" and z_amt > 4.5:
                extreme_signals.append(check["format"].format(z_amt=z_amt))
            elif check["code"] == "HUGE_AMT" and z_amt > 3.8 and amt > 500:
                extreme_signals.append(check["format"].format(z_amt=z_amt, amt=amt))
        
        # Distance checks
        for check in tier1.get("extreme_signals", {}).get("distance_checks", []):
            if check["code"] == "EXTREME_DIST" and z_dist > 4:
                extreme_signals.append(check["format"].format(z_dist=z_dist))
            elif check["code"] == "VERY_FAR" and z_dist > 3.2 and distance > 100:
                extreme_signals.append(check["format"].format(distance=distance))
        
        # Merchant checks
        for check in tier1.get("extreme_signals", {}).get("merchant_checks", []):
            if check["code"] == "FRAUD_MERCHANT" and merch_fraud_rate > 0.4 and merch_total > 50:
                # Add the missing variable
                extreme_signals.append(check["format"].format(
                    fraud_rate=merch_fraud_rate,
                    **{'fraud_rate*100': merch_fraud_rate * 100}  # ← ADD THIS
                ))
        
        # History checks
        for check in tier1.get("extreme_signals", {}).get("history_checks", []):
            if check["code"] == "FRAUD_HISTORY" and fraud_history >= 3:
                extreme_signals.append(check["format"].format(fraud_history=fraud_history))
        
        return extreme_signals
    
    def evaluate_tier1(self, extreme_signals: List[str], ml_score: float) -> Tuple[bool, int, int, List[str]]:
        """
        Evaluate if transaction meets Tier 1 criteria
        Returns: (is_alert, tier, confidence, reasons)
        """
        tier1 = self.get_tier1_rules()
        
        for condition in tier1.get("trigger_conditions", []):
            if condition["name"] == "multiple_extreme" and len(extreme_signals) >= 2:
                return (True, condition["tier"], condition["confidence"], extreme_signals[:3])
            elif condition["name"] == "extreme_plus_ml" and len(extreme_signals) >= 1 and ml_score >= 80:
                reasons = extreme_signals + [f"ML{ml_score:.0f}"]
                return (True, condition["tier"], condition["confidence"], reasons)
        
        return (False, 0, 0, [])
    
    def calculate_tier2_score(self, z_amt: float, z_dist: float, merch_fraud_rate: float,
                          merch_total: int, is_online: bool, is_late: bool, amt: float,
                          fraud_history: int, ml_score: float) -> Tuple[int, List[str]]:

        tier2 = self.get_tier2_rules()
        total_score = 0
        reasons = []
        
        for rule in tier2.get("scoring_rules", []):
            triggered = False
            
            if rule["name"] == "VeryHighAmt" and z_amt > 3.5:
                triggered = True
            elif rule["name"] == "HighAmt" and z_amt > 3:
                triggered = True
            elif rule["name"] == "VeryFar" and z_dist > 3.5:
                triggered = True
            elif rule["name"] == "Far" and z_dist > 3:
                triggered = True
            elif rule["name"] == "RiskyMerch" and merch_fraud_rate > 0.3 and merch_total > 40:
                triggered = True
            elif rule["name"] == "LateOnline" and is_online and is_late and amt > 400:
                triggered = True
            elif rule["name"] == "PrevFraud" and fraud_history >= 2:
                triggered = True
            elif rule["name"] == "Amt+Dist" and z_amt > 2.5 and z_dist > 2.5:
                triggered = True
            elif rule["name"] == "ML_high" and ml_score >= 80:
                triggered = True
            elif rule["name"] == "ML_medium" and ml_score >= 70 and "ML_high" not in [r.split("(")[0] for r in reasons]:
                triggered = True
            
            if triggered:
                total_score += rule["points"]
                # Add the missing variable for fraud_rate*100
                formatted = rule["format"].format(
                    z_amt=z_amt, 
                    z_dist=z_dist, 
                    fraud_rate=merch_fraud_rate,
                    **{'fraud_rate*100': merch_fraud_rate * 100},  # ← ADD THIS LINE
                    fraud_history=fraud_history, 
                    ml_score=ml_score
                )
                reasons.append(formatted)
        
        return (total_score, reasons)
    
    def evaluate_tier2(self, z_amt: float, z_dist: float, merch_fraud_rate: float,
                       merch_total: int, is_online: bool, is_late: bool, amt: float,
                       fraud_history: int, ml_score: float) -> Tuple[bool, int, int, List[str]]:
        """
        Evaluate if transaction meets Tier 2 criteria
        Returns: (is_alert, tier, confidence, reasons)
        """
        tier2 = self.get_tier2_rules()
        score, reasons = self.calculate_tier2_score(
            z_amt, z_dist, merch_fraud_rate, merch_total, is_online, is_late, 
            amt, fraud_history, ml_score
        )
        
        threshold = tier2.get("threshold", 75)
        if score >= threshold:
            return (True, tier2.get("tier", 2), tier2.get("confidence", 80), reasons[:4])
        
        return (False, 0, 0, [])
    
    def evaluate_tier3(self, ml_score: float, z_amt: float, z_dist: float,
                       merch_fraud_rate: float, cat_fraud_rate: float,
                       fraud_history: int) -> Tuple[bool, int, int, List[str]]:
        """
        Evaluate if transaction meets Tier 3 (ML-based) criteria
        Returns: (is_alert, tier, confidence, reasons)
        """
        tier3 = self.get_tier3_rules()
        ml_threshold = tier3.get("ml_threshold", 82)
        
        if ml_score < ml_threshold:
            return (False, 0, 0, [])
        
        # Count support indicators
        support_count = sum([
            z_amt > 2,
            z_dist > 2,
            merch_fraud_rate > 0.15,
            cat_fraud_rate > 0.1,
            fraud_history >= 1
        ])
        
        min_support = tier3.get("min_support_count", 2)
        if support_count >= min_support:
            reasons = [tier3["reason_formats"]["ml_primary"].format(ml_score=ml_score)]
            if z_amt > 2.5:
                reasons.append(tier3["reason_formats"]["high_amount"])
            if z_dist > 2.5:
                reasons.append(tier3["reason_formats"]["far_location"])
            
            return (True, tier3.get("tier", 3), tier3.get("confidence", 75), reasons)
        
        return (False, 0, 0, [])
    
    def is_online_category(self, category: str) -> bool:
        """Check if category is online"""
        online_cats = self.get_detector_rules().get("online_categories", [])
        return category in online_cats
    
    def is_late_night(self, hour: int) -> bool:
        """Check if hour is in late night range"""
        late_night = self.get_detector_rules().get("late_night_hours", {})
        start = late_night.get("start", 1)
        end = late_night.get("end", 5)
        return start <= hour <= end
    
    def evaluate_transaction(self, z_amt: float, amt: float, z_dist: float, distance: float,
                           merch_fraud_rate: float, merch_total: int, cat_fraud_rate: float,
                           fraud_history: int, ml_score: float, category: str, 
                           hour: int) -> Tuple[bool, int, int, str]:
        """
        Complete transaction evaluation using all tiers
        Returns: (is_alert, tier, confidence, reason_string)
        """
        
        # Determine online and late night status
        is_online = self.is_online_category(category)
        is_late = self.is_late_night(hour)
        
        # TIER 1 - Critical signals
        extreme_signals = self.check_tier1_extreme_signals(
            z_amt, amt, z_dist, distance, merch_fraud_rate, merch_total, fraud_history
        )
        
        is_alert, tier, confidence, reasons = self.evaluate_tier1(extreme_signals, ml_score)
        if is_alert:
            return (True, tier, confidence, "|".join(reasons))
        
        # TIER 2 - Score-based
        is_alert, tier, confidence, reasons = self.evaluate_tier2(
            z_amt, z_dist, merch_fraud_rate, merch_total, is_online, is_late,
            amt, fraud_history, ml_score
        )
        if is_alert:
            return (True, tier, confidence, "|".join(reasons))
        
        # TIER 3 - ML-based
        is_alert, tier, confidence, reasons = self.evaluate_tier3(
            ml_score, z_amt, z_dist, merch_fraud_rate, cat_fraud_rate, fraud_history
        )
        if is_alert:
            return (True, tier, confidence, "|".join(reasons))
        
        # No alert triggered
        return (False, 0, 0, "")


# Singleton instance
_rules_loader = None


def get_rules_loader(rules_file: str = None) -> FraudRulesLoader:
    """Get or create the rules loader singleton"""
    global _rules_loader
    if _rules_loader is None:
        _rules_loader = FraudRulesLoader(rules_file)
    return _rules_loader


def reload_rules(rules_file: str = None):
    """Force reload of rules from file"""
    global _rules_loader
    _rules_loader = FraudRulesLoader(rules_file)
    return _rules_loader