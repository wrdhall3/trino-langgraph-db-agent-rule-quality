"""
Generic Data Quality Rule Engine for evaluating rules across multiple systems.
This engine is designed to be flexible and not hardcode any field names.
"""
import pandas as pd
from typing import Dict, List, Any, Optional, Tuple
from models.graph_schema import DQRule, RuleType, SystemType
from database.trino_connector import TrinoConnector
from database.neo4j_manager import Neo4jManager
from config.database_config import SystemConfig
import logging
from datetime import datetime
import uuid
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

class DQViolation(BaseModel):
    """Data Quality Violation model - used for in-memory processing, not stored in graph database."""
    violation_id: Optional[str] = Field(None, description="Unique identifier for the violation")
    rule_id: Optional[str] = Field(None, description="ID of the violated rule")
    cde_name: Optional[str] = Field(None, description="Name of the CDE that was violated")
    system: Optional[str] = Field(None, description="System where violation occurred")
    uitid: Optional[str] = Field(None, description="Trade identifier")
    violation_details: Optional[Dict[str, Any]] = Field(None, description="Details of the violation")
    detected_at: Optional[str] = Field(None, description="Timestamp when violation was detected")
    status: Optional[str] = Field(None, description="Status: OPEN, RESOLVED, IGNORED")

class RuleEngine:
    """Generic rule engine for evaluating data quality rules."""
    
    def __init__(self, trino_connector: TrinoConnector, neo4j_manager: Neo4jManager):
        """Initialize the rule engine."""
        self.trino_connector = trino_connector
        self.neo4j_manager = neo4j_manager
    
    def evaluate_rule(self, rule: DQRule, system_data: Dict[str, pd.DataFrame]) -> List[DQViolation]:
        """Evaluate a single rule across all applicable systems."""
        violations = []
        
        try:
            # Handle case where rule.systems is None or empty
            if not rule.systems:
                logger.info(f"Rule {rule.rule_id or rule.id} has no systems defined, applying to all available systems")
                systems_to_evaluate = list(system_data.keys())
            else:
                systems_to_evaluate = rule.systems
            
            for system_name in systems_to_evaluate:
                if system_name not in system_data or system_data[system_name].empty:
                    logger.warning(f"No data available for system {system_name}")
                    continue
                
                system_violations = self._evaluate_rule_for_system(rule, system_name, system_data[system_name])
                violations.extend(system_violations)
        except Exception as e:
            logger.error(f"Error evaluating rule {rule.rule_id or rule.id}: {e}")
        
        return violations
    
    def _evaluate_rule_for_system(self, rule: DQRule, system_name: str, data: pd.DataFrame) -> List[DQViolation]:
        """Evaluate a rule for a specific system."""
        violations = []
        
        # Get rule type from either rule_type or ruleType field
        rule_type = rule.rule_type or rule.ruleType
        if not rule_type:
            logger.warning(f"No rule type specified for rule {rule.rule_id or rule.id}")
            return violations
        
        # Get the column name for the CDE
        column_name = self._get_column_name_for_cde(rule.cde_name, system_name)
        if not column_name or column_name not in data.columns:
            logger.warning(f"Column {column_name} not found in {system_name} data for rule {rule.rule_id or rule.id}")
            return violations
        
        # Get the uitid column
        uitid_column = SystemConfig.COMMON_ID_FIELD
        if uitid_column not in data.columns:
            logger.warning(f"UITID column {uitid_column} not found in {system_name} data")
            return violations
        
        # Evaluate based on rule type
        if rule_type == "NOT_NULL" or rule_type == RuleType.NOT_NULL:
            violations = self._evaluate_not_null_rule(rule, system_name, data, column_name, uitid_column)
        elif rule_type == "NOT_EMPTY" or rule_type == RuleType.NOT_EMPTY:
            violations = self._evaluate_not_empty_rule(rule, system_name, data, column_name, uitid_column)
        elif rule_type == "POSITIVE_VALUE":
            violations = self._evaluate_positive_value_rule(rule, system_name, data, column_name, uitid_column)
        elif rule_type == "ENUM_VALUE":
            violations = self._evaluate_enum_value_rule(rule, system_name, data, column_name, uitid_column)
        elif rule_type == "RANGE" or rule_type == RuleType.RANGE:
            violations = self._evaluate_range_rule(rule, system_name, data, column_name, uitid_column)
        elif rule_type == "FORMAT" or rule_type == RuleType.FORMAT:
            violations = self._evaluate_format_rule(rule, system_name, data, column_name, uitid_column)
        elif rule_type == "UNIQUE" or rule_type == RuleType.UNIQUE:
            violations = self._evaluate_unique_rule(rule, system_name, data, column_name, uitid_column)
        else:
            logger.warning(f"Unsupported rule type: {rule_type}")
        
        return violations
    
    def _get_column_name_for_cde(self, cde_name: str, system_name: str) -> Optional[str]:
        """Get the actual column name for a CDE in a specific system."""
        # Map CDE names to actual database column names
        cde_mapping = {
            "Trade Date": "trade_date",
            "Quantity": "quantity", 
            "Symbol": "symbol",
            "Price": "price",
            "Side": "side",
            "uitid": "uitid"
        }
        
        # Handle case where cde_name is None
        if not cde_name:
            return None
            
        # Handle case-insensitive mapping
        mapped_column = cde_mapping.get(cde_name)
        if not mapped_column:
            # Try direct mapping if CDE name matches column name
            mapped_column = cde_name.lower()
        
        return mapped_column
    
    def _evaluate_positive_value_rule(self, rule: DQRule, system_name: str, data: pd.DataFrame, 
                                    column_name: str, uitid_column: str) -> List[DQViolation]:
        """Evaluate POSITIVE VALUE rule."""
        violations = []
        
        # Find values that are not positive (null, zero, or negative)
        positive_mask = (data[column_name].notnull()) & (data[column_name] > 0)
        violation_mask = ~positive_mask
        violation_records = data[violation_mask]
        
        for _, record in violation_records.iterrows():
            violation = DQViolation(
                violation_id=str(uuid.uuid4()),
                rule_id=rule.rule_id or rule.id,
                cde_name=rule.cde_name,
                system=SystemType(system_name),
                uitid=str(record[uitid_column]),
                violation_details={
                    "rule_type": "positive_value",
                    "column": column_name,
                    "value": record[column_name],
                    "expected": "POSITIVE VALUE > 0"
                },
                detected_at=datetime.now().isoformat(),
                status="OPEN"
            )
            violations.append(violation)
        
        return violations
    
    def _evaluate_enum_value_rule(self, rule: DQRule, system_name: str, data: pd.DataFrame, 
                                column_name: str, uitid_column: str) -> List[DQViolation]:
        """Evaluate ENUM VALUE rule."""
        violations = []
        
        # For Side column, valid values are BUY and SELL
        valid_values = ["BUY", "SELL"] if column_name == "side" else []
        
        if not valid_values:
            logger.warning(f"No valid enum values defined for column {column_name}")
            return violations
        
        # Find values not in the enum list
        enum_mask = data[column_name].isin(valid_values)
        violation_mask = ~enum_mask
        violation_records = data[violation_mask]
        
        for _, record in violation_records.iterrows():
            violation = DQViolation(
                violation_id=str(uuid.uuid4()),
                rule_id=rule.rule_id or rule.id,
                cde_name=rule.cde_name,
                system=SystemType(system_name),
                uitid=str(record[uitid_column]),
                violation_details={
                    "rule_type": "enum_value",
                    "column": column_name,
                    "value": record[column_name],
                    "valid_values": valid_values
                },
                detected_at=datetime.now().isoformat(),
                status="OPEN"
            )
            violations.append(violation)
        
        return violations
    
    def _evaluate_not_null_rule(self, rule: DQRule, system_name: str, data: pd.DataFrame, 
                               column_name: str, uitid_column: str) -> List[DQViolation]:
        """Evaluate NOT NULL rule."""
        violations = []
        
        # Find null values
        null_mask = data[column_name].isnull()
        null_records = data[null_mask]
        
        for _, record in null_records.iterrows():
            violation = DQViolation(
                violation_id=str(uuid.uuid4()),
                rule_id=rule.rule_id or rule.id,
                cde_name=rule.cde_name,
                system=SystemType(system_name),
                uitid=str(record[uitid_column]),
                violation_details={
                    "rule_type": "not_null",
                    "column": column_name,
                    "value": None,
                    "expected": "NOT NULL"
                },
                detected_at=datetime.now().isoformat(),
                status="OPEN"
            )
            violations.append(violation)
        
        return violations
    
    def _evaluate_not_empty_rule(self, rule: DQRule, system_name: str, data: pd.DataFrame,
                                column_name: str, uitid_column: str) -> List[DQViolation]:
        """Evaluate NOT EMPTY rule."""
        violations = []
        
        # Find empty values (null or empty string)
        empty_mask = (data[column_name].isnull()) | (data[column_name] == "")
        empty_records = data[empty_mask]
        
        for _, record in empty_records.iterrows():
            violation = DQViolation(
                violation_id=str(uuid.uuid4()),
                rule_id=rule.rule_id or rule.id,
                cde_name=rule.cde_name,
                system=SystemType(system_name),
                uitid=str(record[uitid_column]),
                violation_details={
                    "rule_type": "not_empty",
                    "column": column_name,
                    "value": record[column_name],
                    "expected": "NOT EMPTY"
                },
                detected_at=datetime.now().isoformat(),
                status="OPEN"
            )
            violations.append(violation)
        
        return violations
    
    def _evaluate_range_rule(self, rule: DQRule, system_name: str, data: pd.DataFrame,
                            column_name: str, uitid_column: str) -> List[DQViolation]:
        """Evaluate RANGE rule."""
        violations = []
        
        rule_def = rule.rule_definition
        if not rule_def:
            logger.warning(f"No rule definition for range rule {rule.rule_id or rule.id}")
            return violations
            
        min_val = rule_def.get("min")
        max_val = rule_def.get("max")
        exclude_min = rule_def.get("exclude_min", False)
        exclude_max = rule_def.get("exclude_max", False)
        
        # Create range mask
        range_mask = pd.Series([True] * len(data), index=data.index)
        
        if min_val is not None:
            if exclude_min:
                range_mask &= (data[column_name] > min_val)
            else:
                range_mask &= (data[column_name] >= min_val)
        
        if max_val is not None:
            if exclude_max:
                range_mask &= (data[column_name] < max_val)
            else:
                range_mask &= (data[column_name] <= max_val)
        
        # Find violations
        violation_mask = ~range_mask
        violation_records = data[violation_mask]
        
        for _, record in violation_records.iterrows():
            violation = DQViolation(
                violation_id=str(uuid.uuid4()),
                rule_id=rule.rule_id or rule.id,
                cde_name=rule.cde_name,
                system=SystemType(system_name),
                uitid=str(record[uitid_column]),
                violation_details={
                    "rule_type": "range",
                    "column": column_name,
                    "value": record[column_name],
                    "min": min_val,
                    "max": max_val,
                    "exclude_min": exclude_min,
                    "exclude_max": exclude_max
                },
                detected_at=datetime.now().isoformat(),
                status="OPEN"
            )
            violations.append(violation)
        
        return violations
    
    def _evaluate_format_rule(self, rule: DQRule, system_name: str, data: pd.DataFrame,
                             column_name: str, uitid_column: str) -> List[DQViolation]:
        """Evaluate FORMAT rule."""
        violations = []
        
        rule_def = rule.rule_definition
        pattern = rule_def.get("pattern")
        
        if not pattern:
            logger.warning("No pattern specified for format rule")
            return violations
        
        # Find records that don't match the pattern
        import re
        try:
            regex = re.compile(pattern)
            format_mask = data[column_name].astype(str).str.match(regex, na=False)
            violation_mask = ~format_mask
            violation_records = data[violation_mask]
            
            for _, record in violation_records.iterrows():
                violation = DQViolation(
                    violation_id=str(uuid.uuid4()),
                    rule_id=rule.rule_id,
                    cde_name=rule.cde_name,
                    system=SystemType(system_name),
                    uitid=str(record[uitid_column]),
                    violation_details={
                        "rule_type": "format",
                        "column": column_name,
                        "value": record[column_name],
                        "pattern": pattern
                    },
                    detected_at=datetime.now().isoformat(),
                    status="OPEN"
                )
                violations.append(violation)
        
        except re.error as e:
            logger.error(f"Invalid regex pattern {pattern}: {e}")
        
        return violations
    
    def _evaluate_unique_rule(self, rule: DQRule, system_name: str, data: pd.DataFrame,
                             column_name: str, uitid_column: str) -> List[DQViolation]:
        """Evaluate UNIQUE rule."""
        violations = []
        
        # Find duplicate values
        duplicates = data[data[column_name].duplicated(keep=False)]
        
        for _, record in duplicates.iterrows():
            violation = DQViolation(
                violation_id=str(uuid.uuid4()),
                rule_id=rule.rule_id,
                cde_name=rule.cde_name,
                system=SystemType(system_name),
                uitid=str(record[uitid_column]),
                violation_details={
                    "rule_type": "unique",
                    "column": column_name,
                    "value": record[column_name],
                    "duplicate_count": len(data[data[column_name] == record[column_name]])
                },
                detected_at=datetime.now().isoformat(),
                status="OPEN"
            )
            violations.append(violation)
        
        return violations
    
    def evaluate_all_rules(self, uitids: Optional[List[str]] = None) -> List[DQViolation]:
        """Evaluate all active rules across all systems."""
        # Get all active rules
        rules = self.neo4j_manager.get_all_dq_rules()
        
        # Get data from all systems
        system_data = self.trino_connector.get_all_trade_data(uitids)
        
        all_violations = []
        
        for rule in rules:
            try:
                rule_violations = self.evaluate_rule(rule, system_data)
                all_violations.extend(rule_violations)
                logger.info(f"Evaluated rule {rule.rule_id}: {len(rule_violations)} violations found")
            except Exception as e:
                logger.error(f"Error evaluating rule {rule.rule_id}: {e}")
        
        return all_violations
    
    def generate_violation_report(self, violations: List[DQViolation]) -> pd.DataFrame:
        """Generate a formatted violation report."""
        if not violations:
            return pd.DataFrame()
        
        # Get all systems for the report
        systems = list(SystemConfig.SYSTEMS.keys())
        
        # Group violations by CDE, rule, and uitid
        violation_dict = {}
        
        for violation in violations:
            key = (violation.cde_name, violation.rule_id, violation.uitid)
            if key not in violation_dict:
                violation_dict[key] = {system: "No" for system in systems}
            violation_dict[key][violation.system] = "Yes"
        
        # Create report DataFrame
        report_data = []
        
        for (cde_name, rule_id, uitid), system_violations in violation_dict.items():
            # Get rule description
            rule = next((r for r in self.neo4j_manager.get_all_dq_rules() if r.rule_id == rule_id), None)
            rule_desc = rule.description if rule else "Unknown Rule"
            
            row = {
                'CDE': cde_name,
                'DQ_Rule_Desc': rule_desc,
                'uitid': uitid
            }
            
            # Add system columns
            for system in systems:
                system_display_name = SystemConfig.SYSTEMS[system]
                row[system_display_name] = system_violations[system]
            
            report_data.append(row)
        
        return pd.DataFrame(report_data) 