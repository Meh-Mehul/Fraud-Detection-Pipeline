"""
Fraud Detection Rules Loader
Loads and manages rules from JSON configuration files
"""

import json
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass


@dataclass
class RuleResult:
    """Result of a rule evaluation"""
    triggered: bool
    points: int = 0
    reason: str = ""
    severity: str = ""


class FraudRulesManager:
    """Manages fraud detection rules from JSON configuration"""
    
    def __init__(self, config_path: str = "fraud_detection_rules.json"):
        """Initialize rules manager with config file"""
        self.config_path = Path(config_path)
        self.rules = self._load_rules()
        
    def _load_rules(self) -> Dict[str, Any]:
        """Load rules from JSON file"""
        try:
            with open(self.config_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"⚠️  Rules file not found: {self.config_path}")
            print(f"   Creating default rules file...")
            self._create_default_rules()
            with open(self.config_path, 'r') as f:
                return json.load(f)
    
    def _create_default_rules(self):
        """Create default rules file if it doesn't exist"""
        # This would contain the JSON structure from the artifact
        # For brevity, assuming the file exists or is created externally
        pass
    
    def reload_rules(self):
        """Reload rules from file (useful for hot-reloading)"""
        self.rules = self._load_rules()
        print(f"✓ Rules reloaded from {self.config_path}")
    
    # ========================================================================
    # TIER 1 DETECTION
    # ========================================================================
    
    def check_tier1(self, z_amt: float, amt: float, z_dist: float, distance: float,
                    merch_fraud_rate: float, merch_total: int, 
                    customer_fraud_history: int, ml_score: float, 
                    ml_agreement: bool) -> tuple[bool, list[str], int]:
        """
        Check TIER 1 extreme signals
        Returns: (is_suspicious, reasons, tier)
        """
        tier1_rules = self.rules['detection_rules']['tier1_extreme_signals']
        extreme_signals = []
        
        # Amount anomalies
        amt_rules = tier1_rules['amount_anomaly']
        if z_amt > amt_rules['massive_z_score']:
            extreme_signals.append(f"MASSIVE_AMT(Z={z_amt:.1f})")
        elif z_amt > amt_rules['huge_z_score'] and amt > amt_rules['huge_min_amount']:
            extreme_signals.append(f"HUGE_AMT(Z={z_amt:.1f},${amt:.0f})")
        
        # Distance anomalies
        dist_rules = tier1_rules['distance_anomaly']
        if z_dist > dist_rules['extreme_z_score']:
            extreme_signals.append(f"EXTREME_DIST(Z={z_dist:.1f})")
        elif z_dist > dist_rules['very_far_z_score'] and distance > dist_rules['very_far_min_distance_km']:
            extreme_signals.append(f"VERY_FAR({distance:.0f}km)")
        
        # Merchant risk
        merch_rules = tier1_rules['merchant_risk']
        if merch_fraud_rate > merch_rules['fraud_rate_threshold'] and merch_total > merch_rules['min_transactions']:
            extreme_signals.append(f"FRAUD_MERCHANT({merch_fraud_rate*100:.0f}%)")
        
        # Customer history
        hist_rules = tier1_rules['customer_history']
        if customer_fraud_history >= hist_rules['fraud_history_threshold']:
            extreme_signals.append(f"FRAUD_HISTORY({customer_fraud_history})")
        
        # Check trigger conditions
        trigger = tier1_rules['trigger_conditions']
        ml_rules = tier1_rules['ml_threshold']
        
        # Condition 1: 2+ extreme signals
        if len(extreme_signals) >= trigger['min_extreme_signals']:
            return True, extreme_signals[:3], 1
        
        # Condition 2: 1 extreme + high ML
        ml_condition = trigger['or_extreme_plus_ml']
        if (len(extreme_signals) >= ml_condition['min_extreme_signals'] and 
            ml_score >= ml_condition['ml_score'] and 
            ml_condition['ml_agreement'] == ml_agreement):
            reasons = extreme_signals + [f"ML{ml_score:.0f}"]
            return True, reasons, 1
        
        return False, [], 0
    
    # ========================================================================
    # TIER 2 DETECTION
    # ========================================================================
    
    def check_tier2(self, z_amt: float, z_dist: float, merch_fraud_rate: float, 
                    merch_total: int, is_online: int, is_late: int, amt: float,
                    customer_fraud_history: int, ml_score: float, 
                    ml_agreement: bool) -> tuple[bool, list[str], int]:
        """
        Check TIER 2 score-based detection
        Returns: (is_suspicious, reasons, tier)
        """
        tier2_rules = self.rules['detection_rules']['tier2_strong_evidence']
        tier2_score = 0
        tier2_reasons = []
        
        # Iterate through scoring rules
        for rule in tier2_rules['scoring_rules']:
            rule_name = rule['name']
            
            if rule_name == 'very_high_amount':
                if z_amt > rule['z_threshold']:
                    tier2_score += rule['points']
                    tier2_reasons.append(f"VeryHighAmt(Z={z_amt:.1f})")
            
            elif rule_name == 'high_amount':
                if z_amt > rule['z_threshold'] and 'VeryHighAmt' not in str(tier2_reasons):
                    tier2_score += rule['points']
                    tier2_reasons.append(f"HighAmt(Z={z_amt:.1f})")
            
            elif rule_name == 'very_far_distance':
                if z_dist > rule['z_threshold']:
                    tier2_score += rule['points']
                    tier2_reasons.append(f"VeryFar(Z={z_dist:.1f})")
            
            elif rule_name == 'far_distance':
                if z_dist > rule['z_threshold'] and 'VeryFar' not in str(tier2_reasons):
                    tier2_score += rule['points']
                    tier2_reasons.append(f"Far(Z={z_dist:.1f})")
            
            elif rule_name == 'risky_merchant':
                if (merch_fraud_rate > rule['fraud_rate_threshold'] and 
                    merch_total > rule['min_transactions']):
                    tier2_score += rule['points']
                    tier2_reasons.append(f"RiskyMerch({merch_fraud_rate*100:.0f}%)")
            
            elif rule_name == 'late_online':
                if is_online and is_late and amt > rule['min_amount']:
                    tier2_score += rule['points']
                    tier2_reasons.append(f"LateOnline")
            
            elif rule_name == 'previous_fraud':
                if customer_fraud_history >= rule['min_history']:
                    tier2_score += rule['points']
                    tier2_reasons.append(f"PrevFraud({customer_fraud_history})")
            
            elif rule_name == 'amount_distance_combo':
                if z_amt > rule['min_z_amt'] and z_dist > rule['min_z_dist']:
                    tier2_score += rule['points']
                    tier2_reasons.append("Amt+Dist")
            
            elif rule_name == 'ml_high_confidence':
                if ml_score >= rule['ml_threshold'] and rule['require_agreement'] == ml_agreement:
                    tier2_score += rule['points']
                    tier2_reasons.append(f"ML{ml_score:.0f}")
            
            elif rule_name == 'ml_moderate_confidence':
                if ml_score >= rule['ml_threshold'] and 'ML' not in str(tier2_reasons):
                    tier2_score += rule['points']
        
        # Check if threshold met
        if tier2_score >= tier2_rules['trigger_threshold']:
            return True, tier2_reasons[:4], 2
        
        return False, [], 0
    
    # ========================================================================
    # TIER 3 DETECTION
    # ========================================================================
    
    def check_tier3(self, ml_score: float, ml_agreement: bool, z_amt: float, 
                    z_dist: float, merch_fraud_rate: float, cat_fraud_rate: float,
                    customer_fraud_history: int) -> tuple[bool, list[str], int]:
        """
        Check TIER 3 ML-based detection
        Returns: (is_suspicious, reasons, tier)
        """
        tier3_rules = self.rules['detection_rules']['tier3_ml_based']
        
        # Check ML threshold
        if ml_score < tier3_rules['ml_threshold'] or not ml_agreement:
            return False, [], 0
        
        # Count supporting indicators
        support_count = 0
        reasons = [f"ML{ml_score:.0f}"]
        
        for indicator in tier3_rules['supporting_indicators']:
            name = indicator['name']
            
            if name == 'amount_anomaly' and z_amt > 2:
                support_count += 1
                if z_amt > 2.5:
                    reasons.append("HighAmt")
            
            elif name == 'distance_anomaly' and z_dist > 2:
                support_count += 1
                if z_dist > 2.5:
                    reasons.append("FarLoc")
            
            elif name == 'merchant_risk' and merch_fraud_rate > 0.15:
                support_count += 1
            
            elif name == 'category_risk' and cat_fraud_rate > 0.1:
                support_count += 1
            
            elif name == 'fraud_history' and customer_fraud_history >= 1:
                support_count += 1
        
        # Check if minimum supporting indicators met
        if support_count >= tier3_rules['min_supporting_indicators']:
            return True, reasons, 3
        
        return False, [], 0
    
    # ========================================================================
    # COMPREHENSIVE DETECTION
    # ========================================================================
    
    def detect_fraud(self, **kwargs) -> Dict[str, Any]:
        """
        Comprehensive fraud detection using all tiers
        
        Required kwargs:
        - z_amt, amt, z_dist, distance
        - merch_fraud_rate, merch_total
        - cat_fraud_rate
        - customer_fraud_history
        - ml_score, ml_agreement
        - is_online, is_late
        """
        
        # Try TIER 1
        is_fraud, reasons, tier = self.check_tier1(
            kwargs['z_amt'], kwargs['amt'], kwargs['z_dist'], kwargs['distance'],
            kwargs['merch_fraud_rate'], kwargs['merch_total'],
            kwargs['customer_fraud_history'], kwargs['ml_score'], kwargs['ml_agreement']
        )
        
        if is_fraud:
            confidence = self.rules['detection_rules']['tier1_extreme_signals']['confidence']
            return {
                'is_fraud': True,
                'tier': tier,
                'reasons': reasons,
                'confidence': confidence
            }
        
        # Try TIER 2
        is_fraud, reasons, tier = self.check_tier2(
            kwargs['z_amt'], kwargs['z_dist'], kwargs['merch_fraud_rate'],
            kwargs['merch_total'], kwargs['is_online'], kwargs['is_late'], kwargs['amt'],
            kwargs['customer_fraud_history'], kwargs['ml_score'], kwargs['ml_agreement']
        )
        
        if is_fraud:
            confidence = self.rules['detection_rules']['tier2_strong_evidence']['confidence']
            return {
                'is_fraud': True,
                'tier': tier,
                'reasons': reasons,
                'confidence': confidence
            }
        
        # Try TIER 3
        is_fraud, reasons, tier = self.check_tier3(
            kwargs['ml_score'], kwargs['ml_agreement'], kwargs['z_amt'],
            kwargs['z_dist'], kwargs['merch_fraud_rate'], kwargs['cat_fraud_rate'],
            kwargs['customer_fraud_history']
        )
        
        if is_fraud:
            confidence = self.rules['detection_rules']['tier3_ml_based']['confidence']
            return {
                'is_fraud': True,
                'tier': tier,
                'reasons': reasons,
                'confidence': confidence
            }
        
        return {
            'is_fraud': False,
            'tier': 0,
            'reasons': [],
            'confidence': 0
        }
    
    # ========================================================================
    # INDICATOR LOOKUP (for report generation)
    # ========================================================================
    
    def get_indicator_details(self, indicator_code: str) -> Optional[Dict[str, Any]]:
        """Get detailed information about a fraud indicator"""
        
        # Parse indicator (e.g., "MASSIVE_AMT(Z=4.7)")
        base_code = indicator_code.split('(')[0]
        
        # Search through all indicator categories
        for category, indicators in self.rules['indicator_definitions'].items():
            if base_code in indicators:
                return indicators[base_code]
        
        # Check ML indicators
        if base_code.startswith('ML'):
            return self.rules['indicator_definitions']['ml_detection']['ML']
        
        return None
    
    def get_tier_info(self, tier: int) -> Dict[str, Any]:
        """Get tier information"""
        return self.rules['tier_information'].get(str(tier), {
            'name': f'Unknown Tier {tier}',
            'description': 'N/A',
            'action': 'Manual review required'
        })
    
    def get_all_indicators(self) -> Dict[str, Any]:
        """Get all indicator definitions (for report generation)"""
        return self.rules['indicator_definitions']
    
    def get_investigation_protocols(self) -> Dict[str, Any]:
        """Get investigation protocols for reports"""
        return self.rules['reporting_config']['investigation_protocols']
    
    def is_online_category(self, category: str) -> bool:
        """Check if category is online"""
        online_cats = self.rules['detection_rules']['behavioral_checks']['online_categories']
        return category in online_cats
    
    def is_late_night(self, hour: int) -> bool:
        """Check if hour is late night"""
        late_hours = self.rules['detection_rules']['behavioral_checks']['late_night_hours']
        return late_hours['start'] <= hour <= late_hours['end']
    
    def get_training_threshold(self) -> int:
        """Get minimum transactions before detection starts"""
        return self.rules['detection_rules']['training_phase']['min_transactions_per_customer']


# ========================================================================
# USAGE EXAMPLE
# ========================================================================

if __name__ == "__main__":
    # Initialize rules manager
    rules_mgr = FraudRulesManager("fraud_detection_rules.json")
    
    # Example transaction
    transaction = {
        'z_amt': 4.2,
        'amt': 850.0,
        'z_dist': 3.8,
        'distance': 150.0,
        'merch_fraud_rate': 0.35,
        'merch_total': 60,
        'cat_fraud_rate': 0.08,
        'customer_fraud_history': 1,
        'ml_score': 75.0,
        'ml_agreement': True,
        'is_online': 1,
        'is_late': 0
    }
    
    # Detect fraud
    result = rules_mgr.detect_fraud(**transaction)
    
    print("Fraud Detection Result:")
    print(f"  Is Fraud: {result['is_fraud']}")
    print(f"  Tier: {result['tier']}")
    print(f"  Confidence: {result['confidence']}%")
    print(f"  Reasons: {result['reasons']}")
    
    # Get tier info
    if result['is_fraud']:
        tier_info = rules_mgr.get_tier_info(result['tier'])
        print(f"\nTier Info:")
        print(f"  {tier_info['name']}")
        print(f"  Action: {tier_info['action']}")