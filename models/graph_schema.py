"""
Neo4j Graph Schema for Critical Data Elements (CDEs) and Data Quality Rules (DQ Rules).
This schema is designed to be generic and flexible for tracking data quality across multiple systems.
"""
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field
from enum import Enum

class SystemType(str, Enum):
    """Enumeration of system types."""
    TRADE = "trade"
    SETTLEMENT = "settlement"
    REPORTING = "reporting"

class DataType(str, Enum):
    """Common data types for CDEs."""
    STRING = "string"
    INTEGER = "integer"
    DECIMAL = "decimal"
    DATE = "date"
    DATETIME = "datetime"
    BOOLEAN = "boolean"

class RuleType(str, Enum):
    """Types of data quality rules."""
    NOT_NULL = "NOT_NULL"
    NOT_EMPTY = "NOT_EMPTY"
    POSITIVE_VALUE = "POSITIVE_VALUE"
    ENUM_VALUE = "ENUM_VALUE"
    RANGE = "RANGE"
    FORMAT = "FORMAT"
    UNIQUE = "UNIQUE"
    REFERENTIAL = "REFERENTIAL"
    CUSTOM = "CUSTOM"

class CDE(BaseModel):
    """Critical Data Element model."""
    name: str = Field(..., description="Name of the CDE")
    description: Optional[str] = Field(None, description="Description of the CDE")
    data_type: Optional[DataType] = Field(None, description="Data type of the CDE")
    systems: Optional[List[SystemType]] = Field(None, description="Systems where this CDE exists")
    table_name: Optional[str] = Field(None, description="Table name where this CDE is stored")
    column_name: Optional[str] = Field(None, description="Column name in the table")
    is_required: Optional[bool] = Field(None, description="Whether this CDE is required")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")

class DQRule(BaseModel):
    """Data Quality Rule model."""
    rule_id: Optional[str] = Field(None, description="Unique identifier for the rule (maps to 'id' property)")
    id: Optional[str] = Field(None, description="Database ID field")
    name: Optional[str] = Field(None, description="Name of the rule")
    description: Optional[str] = Field(None, description="Description of the rule")
    rule_type: Optional[str] = Field(None, description="Type of the rule (maps to 'ruleType' property)")
    ruleType: Optional[str] = Field(None, description="Database ruleType field")
    cde_name: Optional[str] = Field(None, description="Name of the CDE this rule applies to")
    systems: Optional[List[SystemType]] = Field(None, description="Systems where this rule applies")
    rule_definition: Optional[Dict[str, Any]] = Field(None, description="Rule definition parameters")
    severity: Optional[str] = Field(None, description="Severity level: ERROR, WARNING, INFO")
    is_active: Optional[bool] = Field(None, description="Whether the rule is active")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")

class GraphSchema:
    """Neo4j Graph Schema definition for CDEs and DQ Rules."""
    
    # Node labels
    CDE_LABEL = "CDE"
    DQ_RULE_LABEL = "DQRule"
    SYSTEM_LABEL = "System"
    TRADE_LABEL = "Trade"
    
    # Relationship types
    APPLIES_TO = "APPLIES_TO"
    EXISTS_IN = "EXISTS_IN"
    BELONGS_TO = "BELONGS_TO"
    HAS_RULE = "HAS_RULE"
 