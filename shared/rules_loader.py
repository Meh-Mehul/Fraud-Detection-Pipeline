"""
Fraud Rules Loader
Loads fraud detection rules and configurations from JSON
"""

import json
from pathlib import Path
from typing import Dict, Any, List


class FraudRulesLoader:
    """Loads and manages fraud detection rules"""
    
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