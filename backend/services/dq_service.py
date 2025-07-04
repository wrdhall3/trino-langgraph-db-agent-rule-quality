"""
Service for handling data quality analysis and violations management
"""
import logging
from typing import Dict, List, Any, Optional
import asyncio
from datetime import datetime
import csv
import os
from database.trino_connector import TrinoConnector
from database.neo4j_manager import Neo4jManager
from dq_engine.rule_engine import RuleEngine, DQViolation
from models.graph_schema import CDE, DQRule, System

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

class DQService:
    """Service for data quality analysis and violations management"""
    
    def __init__(self, trino_connector: TrinoConnector, neo4j_manager: Neo4jManager):
        self.trino_connector = trino_connector
        self.neo4j_manager = neo4j_manager
        self.rule_engine = RuleEngine(trino_connector, neo4j_manager)
        self.last_analysis_violations = []
        self.last_analysis_timestamp = None
    
    async def run_analysis(self, uitids: Optional[List[str]] = None, generate_csv: bool = False) -> Dict[str, Any]:
        """Run data quality analysis
        
        Args:
            uitids: Optional list of UITIDs to analyze
            generate_csv: Whether to generate a CSV file with results (default: False)
        """
        try:
            logger.info(f"Starting DQ analysis for UITIDs: {uitids}")
            
            # Get CDEs and rules from Neo4j
            cdes = await self._get_cdes_from_neo4j()
            rules = await self._get_rules_from_neo4j()
            
            logger.info(f"Found {len(cdes)} CDEs and {len(rules)} rules")
            
            # Get system data from Trino
            system_data = self.trino_connector.get_all_trade_data(uitids)
            
            # Run rule engine evaluation
            all_violations = []
            for rule in rules:
                try:
                    violations = self.rule_engine.evaluate_rule(rule, system_data)
                    all_violations.extend(violations)
                except Exception as e:
                    logger.error(f"Error evaluating rule {rule.rule_id}: {e}")
                    continue
            
            # Store results
            self.last_analysis_violations = all_violations
            self.last_analysis_timestamp = datetime.now()
            
            # Generate summary
            summary = self._generate_analysis_summary(all_violations)
            
            # Build return data
            result = {
                "total_violations": len(all_violations),
                "violations": [violation.to_dict() for violation in all_violations],
                "summary": summary,
                "timestamp": self.last_analysis_timestamp.isoformat(),
                "uitids_analyzed": uitids or "all"
            }
            
            # Optionally save to CSV
            if generate_csv:
                csv_filename = await self._save_violations_to_csv(all_violations)
                result["csv_file"] = csv_filename
                logger.info(f"CSV file generated: {csv_filename}")
            else:
                logger.info("CSV generation skipped (generate_csv=False)")
            
            return result
            
        except Exception as e:
            logger.error(f"Error running DQ analysis: {e}")
            raise
    
    async def get_violations(self) -> List[Dict[str, Any]]:
        """Get current DQ violations"""
        try:
            if not self.last_analysis_violations:
                # Run a quick analysis if no violations cached (without generating CSV)
                analysis_result = await self.run_analysis(generate_csv=False)
                return analysis_result["violations"]
            
            return [violation.to_dict() for violation in self.last_analysis_violations]
            
        except Exception as e:
            logger.error(f"Error getting violations: {e}")
            raise
    
    async def get_all_rules(self) -> List[Dict[str, Any]]:
        """Get all DQ rules"""
        try:
            rules = await self._get_rules_from_neo4j()
            return [rule.to_dict() for rule in rules]
            
        except Exception as e:
            logger.error(f"Error getting DQ rules: {e}")
            raise
    
    async def get_all_cdes(self) -> List[Dict[str, Any]]:
        """Get all CDEs"""
        try:
            cdes = await self._get_cdes_from_neo4j()
            return [cde.to_dict() for cde in cdes]
            
        except Exception as e:
            logger.error(f"Error getting CDEs: {e}")
            raise
    
    async def get_systems_info(self) -> List[Dict[str, Any]]:
        """Get all systems information"""
        try:
            systems = await self._get_systems_from_neo4j()
            return [system.to_dict() for system in systems]
            
        except Exception as e:
            logger.error(f"Error getting systems info: {e}")
            raise
    
    async def export_violations_to_csv(self) -> str:
        """Export current violations to CSV file"""
        try:
            if not self.last_analysis_violations:
                logger.info("No violations to export - running analysis first")
                await self.run_analysis(generate_csv=False)
            
            csv_filename = await self._save_violations_to_csv(self.last_analysis_violations)
            logger.info(f"Violations exported to: {csv_filename}")
            return csv_filename
            
        except Exception as e:
            logger.error(f"Error exporting violations to CSV: {e}")
            raise
    
    async def _get_cdes_from_neo4j(self) -> List[CDE]:
        """Get CDEs from Neo4j"""
        try:
            with self.neo4j_manager.driver.session() as session:
                result = session.run("""
                    MATCH (cde:CDE)<-[:HAS_CDE]-(system:System)
                    RETURN cde.name AS name, 
                           cde.description AS description,
                           collect(DISTINCT system.name) AS systems
                """)
                
                cdes = []
                for record in result:
                    cde = CDE(
                        name=record["name"],
                        description=record.get("description"),
                        data_type=None,  # Not available in current schema
                        column_name=None,  # Not available in current schema
                        table_name=None,  # Not available in current schema
                        systems=map_neo4j_systems_to_enum(record.get("systems", []))  # Map system names to enum values
                    )
                    cdes.append(cde)
                
                return cdes
                
        except Exception as e:
            logger.error(f"Error getting CDEs from Neo4j: {e}")
            raise
    
    async def _get_rules_from_neo4j(self) -> List[DQRule]:
        """Get DQ rules from Neo4j"""
        try:
            with self.neo4j_manager.driver.session() as session:
                result = session.run("""
                    MATCH (rule:DQRule)<-[:HAS_RULE]-(cde:CDE)<-[:HAS_CDE]-(system:System)
                    RETURN rule.id AS rule_id,
                           rule.description AS description,
                           rule.ruleType AS rule_type,
                           cde.name AS cde_name,
                           collect(DISTINCT system.name) AS systems
                """)
                
                rules = []
                for record in result:
                    # Generate a meaningful name from available data
                    rule_id = record.get("rule_id")
                    cde_name = record.get("cde_name")
                    rule_type = record.get("rule_type")
                    description = record.get("description")
                    
                    # Create rule name: use description if available, otherwise create from rule_type and cde_name
                    if description:
                        rule_name = description
                    elif rule_type and cde_name:
                        rule_name = f"{rule_type.replace('_', ' ').title()} - {cde_name}"
                    else:
                        rule_name = f"Rule {rule_id}" if rule_id else "Unknown Rule"
                    
                    rule = DQRule(
                        name=rule_name,  # Generated from available data
                        rule_id=rule_id,
                        description=description,
                        rule_type=rule_type,
                        severity=None,  # Not available in current schema
                        threshold=None,  # Not available in current schema
                        parameters=None,  # Not available in current schema
                        cde_name=cde_name,
                        systems=map_neo4j_systems_to_enum(record.get("systems", []))  # Map system names to enum values
                    )
                    rules.append(rule)
                
                return rules
                
        except Exception as e:
            logger.error(f"Error getting DQ rules from Neo4j: {e}")
            raise
    
    async def _get_systems_from_neo4j(self) -> List[System]:
        """Get systems from Neo4j"""
        try:
            with self.neo4j_manager.driver.session() as session:
                result = session.run("""
                    MATCH (system:System)
                    OPTIONAL MATCH (system)-[:HAS_CDE]->(cde:CDE)
                    RETURN system.name AS name,
                           system.description AS description,
                           system.database AS database,
                           system.table AS table,
                           collect(cde.name) AS cdes
                """)
                
                systems = []
                for record in result:
                    system = System(
                        name=record["name"],
                        description=record.get("description"),
                        database=record.get("database"),
                        table=record.get("table"),
                        cdes=record.get("cdes", [])
                    )
                    systems.append(system)
                
                return systems
                
        except Exception as e:
            logger.error(f"Error getting systems from Neo4j: {e}")
            raise
    
    def _generate_analysis_summary(self, violations: List[DQViolation]) -> Dict[str, Any]:
        """Generate analysis summary"""
        if not violations:
            return {
                "total_violations": 0,
                "by_severity": {},
                "by_rule_type": {},
                "by_system": {},
                "by_rule": {}
            }
        
        by_severity = {}
        by_rule_type = {}
        by_system = {}
        by_rule = {}
        
        for violation in violations:
            # Get violation details
            details = violation.violation_details or {}
            
            # Count by severity
            severity = details.get("severity", "UNKNOWN")
            by_severity[severity] = by_severity.get(severity, 0) + 1
            
            # Count by rule type
            rule_type = details.get("rule_type", "UNKNOWN")
            by_rule_type[rule_type] = by_rule_type.get(rule_type, 0) + 1
            
            # Count by system
            system = str(violation.system) if violation.system else "UNKNOWN"
            by_system[system] = by_system.get(system, 0) + 1
            
            # Count by rule
            rule_name = details.get("rule_name", "UNKNOWN")
            by_rule[rule_name] = by_rule.get(rule_name, 0) + 1
        
        return {
            "total_violations": len(violations),
            "by_severity": by_severity,
            "by_rule_type": by_rule_type,
            "by_system": by_system,
            "by_rule": by_rule
        }
    
    async def _save_violations_to_csv(self, violations: List[DQViolation]) -> str:
        """Save violations to CSV file"""
        try:
            # Create results directory if it doesn't exist
            results_dir = "results"
            if not os.path.exists(results_dir):
                os.makedirs(results_dir)
            
            # Generate filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            csv_filename = f"{results_dir}/dq_violations_report_{timestamp}.csv"
            
            # Write to CSV
            with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
                if not violations:
                    # Write header even if no violations
                    writer = csv.writer(csvfile)
                    writer.writerow(['No violations found'])
                    return csv_filename
                
                fieldnames = ['rule_name', 'rule_type', 'severity', 'system', 'table', 'column', 
                             'uitid', 'value', 'message', 'timestamp']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                writer.writeheader()
                for violation in violations:
                    # Convert violation to dict to get all fields properly
                    violation_dict = violation.to_dict()
                    writer.writerow({
                        'rule_name': violation_dict.get('rule_name', 'Unknown'),
                        'rule_type': violation_dict.get('rule_type', 'Unknown'),
                        'severity': violation_dict.get('severity', 'MEDIUM'),
                        'system': violation_dict.get('system', 'Unknown'),
                        'table': violation_dict.get('table', 'trade'),
                        'column': violation_dict.get('column', 'unknown'),
                        'uitid': violation_dict.get('uitid', ''),
                        'value': violation_dict.get('value', ''),
                        'message': violation_dict.get('message', ''),
                        'timestamp': violation_dict.get('timestamp', '')
                    })
            
            logger.info(f"Saved {len(violations)} violations to {csv_filename}")
            return csv_filename
            
        except Exception as e:
            logger.error(f"Error saving violations to CSV: {e}")
            raise
    
    def close(self):
        """Close connections"""
        if self.rule_engine:
            self.rule_engine.close()
        if self.trino_connector:
            self.trino_connector.close()
        if self.neo4j_manager:
            self.neo4j_manager.close() 