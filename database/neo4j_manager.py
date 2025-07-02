"""
Neo4j database manager for handling Critical Data Elements (CDEs) and Data Quality Rules (DQ Rules).
"""
import logging
from typing import List, Dict, Any, Optional
from neo4j import GraphDatabase
from models.graph_schema import GraphSchema, CDE, DQRule, SystemType
from config.database_config import SystemConfig
import pandas as pd
import uuid

logger = logging.getLogger(__name__)

# Mapping between Neo4j system names and SystemType enum values
SYSTEM_NAME_MAPPING = {
    "Trade System": "trade",
    "Settlement System": "settlement", 
    "Reporting System": "reporting"
}

def map_neo4j_systems_to_enum(neo4j_systems: List[str]) -> List[str]:
    """Map Neo4j system names to SystemType enum values."""
    return [SYSTEM_NAME_MAPPING.get(system, system.lower()) for system in neo4j_systems]

class Neo4jManager:
    """Manager for Neo4j graph database operations."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize the Neo4j manager."""
        neo4j_config = config or SystemConfig.NEO4J
        self.driver = GraphDatabase.driver(
            neo4j_config.uri,
            auth=(neo4j_config.username, neo4j_config.password)
        )
        self.database = neo4j_config.database
    

    

    
    def get_all_cdes(self) -> List[CDE]:
        """Get all CDEs from the graph with their associated systems."""
        query = """
        MATCH (c:CDE)<-[:HAS_CDE]-(s:System)
        RETURN c.name as cde_name, c.description as description,
               collect(DISTINCT s.name) as systems
        ORDER BY c.name
        """
        
        with self.driver.session(database=self.database) as session:
            result = session.run(query)
            cdes = []
            for record in result:
                try:
                    # Only require name field - all others are optional
                    if record["cde_name"] is None:
                        logger.warning(f"Skipping CDE with no name: {dict(record)}")
                        continue
                    
                    cde = CDE(
                        name=record["cde_name"],
                        description=record.get("description"),
                        data_type=None,  # Not available in current database structure
                        systems=map_neo4j_systems_to_enum(record["systems"]),  # Map to enum values
                        table_name=None,  # Not available in current database structure
                        column_name=None,  # Not available in current database structure
                        is_required=None,  # Not available in current database structure
                        metadata=None  # Not available in current database structure
                    )
                    cdes.append(cde)
                except Exception as e:
                    logger.warning(f"Error processing CDE record: {e}")
                    continue
        
        return cdes
    
    def get_all_dq_rules(self) -> List[DQRule]:
        """Get all DQ Rules from the graph with their associated systems."""
        query = """
        MATCH (r:DQRule)<-[:HAS_RULE]-(c:CDE)<-[:HAS_CDE]-(s:System)
        RETURN r.id as rule_id, r.description as rule_desc, r.ruleType as rule_type,
               c.name as cde_name, collect(DISTINCT s.name) as systems
        ORDER BY r.id
        """
        
        with self.driver.session(database=self.database) as session:
            result = session.run(query)
            rules = []
            for record in result:
                try:
                    # Map database fields to model fields
                    rule = DQRule(
                        rule_id=record["rule_id"],  # Database uses 'id' field
                        id=record["rule_id"],
                        name=None,  # Not available in current database structure
                        description=record["rule_desc"],
                        rule_type=record["rule_type"],  # Database uses 'ruleType' field
                        ruleType=record["rule_type"],
                        cde_name=record["cde_name"],
                        systems=map_neo4j_systems_to_enum(record["systems"]),  # Map to enum values
                        rule_definition=None,  # Not available in current database structure
                        severity=None,  # Not available in current database structure
                        is_active=None,  # Not available in current database structure
                        metadata=None  # Not available in current database structure
                    )
                    rules.append(rule)
                except Exception as e:
                    logger.warning(f"Error processing DQ Rule record: {e}")
                    continue
        
        return rules
    
    def get_dq_rules_for_cde(self, cde_name: str) -> List[DQRule]:
        """Get all DQ Rules that apply to a specific CDE."""
        query = """
        MATCH (r:DQRule)-[:APPLIES_TO]->(c:CDE)
        WHERE c.name = $cde_name AND r.is_active = true
        RETURN r
        ORDER BY r.rule_id
        """
        
        with self.driver.session(database=self.database) as session:
            result = session.run(query, cde_name=cde_name)
            rules = []
            for record in result:
                rule_data = record["r"]
                rule = DQRule(
                    rule_id=rule_data["rule_id"],
                    name=rule_data["name"],
                    description=rule_data["description"],
                    rule_type=rule_data["rule_type"],
                    cde_name=rule_data["cde_name"],
                    systems=rule_data["systems"],
                    rule_definition=rule_data["rule_definition"],
                    severity=rule_data.get("severity", "ERROR"),
                    is_active=rule_data.get("is_active", True),
                    metadata=rule_data.get("metadata", {})
                )
                rules.append(rule)
        
        return rules
    
    def get_dq_rules_for_system(self, system_name: str) -> List[DQRule]:
        """Get all DQ Rules that apply to a specific system."""
        query = """
        MATCH (r:DQRule)-[:EXISTS_IN]->(s:System)
        WHERE s.name = $system_name AND r.is_active = true
        RETURN r
        ORDER BY r.rule_id
        """
        
        with self.driver.session(database=self.database) as session:
            result = session.run(query, system_name=system_name)
            rules = []
            for record in result:
                rule_data = record["r"]
                rule = DQRule(
                    rule_id=rule_data["rule_id"],
                    name=rule_data["name"],
                    description=rule_data["description"],
                    rule_type=rule_data["rule_type"],
                    cde_name=rule_data["cde_name"],
                    systems=rule_data["systems"],
                    rule_definition=rule_data["rule_definition"],
                    severity=rule_data.get("severity", "ERROR"),
                    is_active=rule_data.get("is_active", True),
                    metadata=rule_data.get("metadata", {})
                )
                rules.append(rule)
        
        return rules
    
    def get_violations_for_uitid(self, uitid: str) -> List:
        """Get all violations for a specific uitid.
        
        Note: DQ violations are not stored in the graph database.
        They are processed in memory by the rule engine and returned directly.
        This method returns an empty list for compatibility.
        """
        logger.info("DQ violations are not stored in the graph database")
        return []
    
    def get_violations_summary(self) -> pd.DataFrame:
        """Get a summary of all violations for reporting.
        
        Note: DQ violations are not stored in the graph database.
        They are processed in memory by the rule engine.
        This method returns an empty DataFrame for compatibility.
        """
        logger.info("DQ violations are not stored in the graph database")
        return pd.DataFrame(columns=['CDE', 'DQ_Rule_Desc', 'uitid', 'System', 'Status', 'Detected_At'])
    
    def close(self):
        """Close the Neo4j driver connection."""
        if self.driver:
            self.driver.close()
            logger.info("Neo4j connection closed")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close() 