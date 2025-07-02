"""
LangGraph agents for data quality analysis and rule evaluation.
"""
from typing import Dict, List, Any, Optional, TypedDict, Annotated
from langgraph.graph import StateGraph, END
from langgraph.prebuilt import ToolNode
from langchain_core.messages import HumanMessage, AIMessage
from langchain_openai import ChatOpenAI
import pandas as pd
import logging
from datetime import datetime
from dataclasses import dataclass
from enum import Enum

from database.trino_connector import TrinoConnector
from database.neo4j_manager import Neo4jManager
from dq_engine.rule_engine import RuleEngine, DQViolation
from models.graph_schema import DQRule, SystemType

logger = logging.getLogger(__name__)

# State definition for the LangGraph workflow
class DQState(TypedDict):
    """State for the data quality analysis workflow."""
    messages: List[Any]
    cdes: List[Any]
    dq_rules: List[DQRule]
    system_data: Dict[str, pd.DataFrame]
    violations: List[DQViolation]
    report: Optional[pd.DataFrame]
    uitids: Optional[List[str]]
    analysis_summary: Optional[str]

@dataclass
class DQAnalysisResult:
    """Result of data quality analysis."""
    uitids: List[str]
    violations: List[DQViolation]

class DQAnalysisAgent:
    """Main agent for orchestrating data quality analysis."""
    
    def __init__(self, trino_connector: TrinoConnector, neo4j_manager: Neo4jManager):
        """Initialize the DQ analysis agent."""
        self.trino_connector = trino_connector
        self.neo4j_manager = neo4j_manager
        self.rule_engine = RuleEngine(trino_connector, neo4j_manager)
        self.llm = ChatOpenAI(model="gpt-3.5-turbo", temperature=0)
        
        # Create the workflow graph
        self.workflow = self._create_workflow()
    
    def _create_workflow(self) -> StateGraph:
        """Create the LangGraph workflow for DQ analysis."""
        
        # Create the state graph
        workflow = StateGraph(DQState)
        
        # Add nodes
        workflow.add_node("extract_cdes", self._extract_cdes_node)
        workflow.add_node("extract_rules", self._extract_rules_node)
        workflow.add_node("extract_data", self._extract_data_node)
        workflow.add_node("evaluate_rules", self._evaluate_rules_node)
        workflow.add_node("generate_report", self._generate_report_node)
        workflow.add_node("analyze_results", self._analyze_results_node)
        
        # Define the flow
        workflow.set_entry_point("extract_cdes")
        workflow.add_edge("extract_cdes", "extract_rules")
        workflow.add_edge("extract_rules", "extract_data")
        workflow.add_edge("extract_data", "evaluate_rules")
        workflow.add_edge("evaluate_rules", "generate_report")
        workflow.add_edge("generate_report", "analyze_results")
        workflow.add_edge("analyze_results", END)
        
        return workflow.compile()
    
    def _extract_cdes_node(self, state: DQState) -> DQState:
        """Extract CDEs from the graph database."""
        try:
            cdes = self.neo4j_manager.get_all_cdes()
            state["cdes"] = cdes
            
            # Add message about CDEs found
            cde_names = [cde.name for cde in cdes]
            state["messages"].append(
                AIMessage(content=f"Found {len(cdes)} CDEs: {', '.join(cde_names)}")
            )
            
            logger.info(f"Extracted {len(cdes)} CDEs from graph database")
            
        except Exception as e:
            logger.error(f"Error extracting CDEs: {e}")
            state["messages"].append(
                AIMessage(content=f"Error extracting CDEs: {str(e)}")
            )
        
        return state
    
    def _extract_rules_node(self, state: DQState) -> DQState:
        """Extract DQ rules from the graph database."""
        try:
            rules = self.neo4j_manager.get_all_dq_rules()
            state["dq_rules"] = rules
            
            # Add message about rules found
            rule_ids = [rule.rule_id for rule in rules]
            state["messages"].append(
                AIMessage(content=f"Found {len(rules)} active DQ rules: {', '.join(rule_ids)}")
            )
            
            logger.info(f"Extracted {len(rules)} DQ rules from graph database")
            
        except Exception as e:
            logger.error(f"Error extracting DQ rules: {e}")
            state["messages"].append(
                AIMessage(content=f"Error extracting DQ rules: {str(e)}")
            )
        
        return state
    
    def _extract_data_node(self, state: DQState) -> DQState:
        """Extract data from all systems."""
        try:
            uitids = state.get("uitids")
            system_data = self.trino_connector.get_all_trade_data(uitids)
            state["system_data"] = system_data
            
            # Add message about data extracted
            data_summary = []
            for system_name, data in system_data.items():
                if not data.empty:
                    data_summary.append(f"{system_name}: {len(data)} records")
                else:
                    data_summary.append(f"{system_name}: No data")
            
            state["messages"].append(
                AIMessage(content=f"Extracted data from systems: {', '.join(data_summary)}")
            )
            
            logger.info(f"Extracted data from {len(system_data)} systems")
            
        except Exception as e:
            logger.error(f"Error extracting data: {e}")
            state["messages"].append(
                AIMessage(content=f"Error extracting data: {str(e)}")
            )
        
        return state
    
    def _evaluate_rules_node(self, state: DQState) -> DQState:
        """Evaluate all rules against the extracted data."""
        try:
            uitids = state.get("uitids")
            violations = self.rule_engine.evaluate_all_rules(uitids)
            state["violations"] = violations
            
            # Note: Violations are processed in memory only, not stored in Neo4j
            
            # Add message about violations found
            state["messages"].append(
                AIMessage(content=f"Found {len(violations)} DQ violations across all systems")
            )
            
            logger.info(f"Evaluated rules and found {len(violations)} violations")
            
        except Exception as e:
            logger.error(f"Error evaluating rules: {e}")
            state["messages"].append(
                AIMessage(content=f"Error evaluating rules: {str(e)}")
            )
        
        return state
    
    def _generate_report_node(self, state: DQState) -> DQState:
        """Generate a formatted violation report."""
        try:
            violations = state.get("violations", [])
            report = self.rule_engine.generate_violation_report(violations)
            state["report"] = report
            
            # Add message about report generation
            if not report.empty:
                state["messages"].append(
                    AIMessage(content=f"Generated violation report with {len(report)} entries")
                )
            else:
                state["messages"].append(
                    AIMessage(content="No violations found - report is empty")
                )
            
            logger.info(f"Generated violation report with {len(report)} entries")
            
        except Exception as e:
            logger.error(f"Error generating report: {e}")
            state["messages"].append(
                AIMessage(content=f"Error generating report: {str(e)}")
            )
        
        return state
    
    def _analyze_results_node(self, state: DQState) -> DQState:
        """Analyze the results and provide insights."""
        try:
            violations = state.get("violations", [])
            report = state.get("report")
            
            if not violations:
                analysis = "No data quality violations were found across all systems. All CDEs are compliant with their respective DQ rules."
            else:
                # Group violations by system
                system_violations = {}
                for violation in violations:
                    system = violation.system
                    if system not in system_violations:
                        system_violations[system] = 0
                    system_violations[system] += 1
                
                # Group violations by CDE
                cde_violations = {}
                for violation in violations:
                    cde = violation.cde_name
                    if cde not in cde_violations:
                        cde_violations[cde] = 0
                    cde_violations[cde] += 1
                
                # Create analysis summary
                analysis_parts = [
                    f"Found {len(violations)} total data quality violations:",
                    "",
                    "Violations by system:"
                ]
                
                for system, count in system_violations.items():
                    analysis_parts.append(f"- {system}: {count} violations")
                
                analysis_parts.extend([
                    "",
                    "Violations by CDE:"
                ])
                
                for cde, count in cde_violations.items():
                    analysis_parts.append(f"- {cde}: {count} violations")
                
                analysis = "\n".join(analysis_parts)
            
            state["analysis_summary"] = analysis
            state["messages"].append(AIMessage(content=analysis))
            
            logger.info("Completed analysis of DQ violations")
            
        except Exception as e:
            logger.error(f"Error analyzing results: {e}")
            state["messages"].append(
                AIMessage(content=f"Error analyzing results: {str(e)}")
            )
        
        return state
    
    def run_analysis(self, uitids: Optional[List[str]] = None) -> Dict[str, Any]:
        """Run the complete data quality analysis workflow."""
        # Initialize state
        initial_state = DQState(
            messages=[HumanMessage(content="Starting data quality analysis")],
            cdes=[],
            dq_rules=[],
            system_data={},
            violations=[],
            report=None,
            uitids=uitids,
            analysis_summary=None
        )
        
        # Run the workflow
        try:
            final_state = self.workflow.invoke(initial_state)
            logger.info("Data quality analysis workflow completed successfully")
            return final_state
        except Exception as e:
            logger.error(f"Error running DQ analysis workflow: {e}")
            raise

class DQMonitoringAgent:
    """Agent for continuous monitoring of data quality."""
    
    def __init__(self, trino_connector: TrinoConnector, neo4j_manager: Neo4jManager):
        """Initialize the DQ monitoring agent."""
        self.trino_connector = trino_connector
        self.neo4j_manager = neo4j_manager
        self.rule_engine = RuleEngine(trino_connector, neo4j_manager)
    
    def monitor_specific_uitids(self, uitids: List[str]) -> pd.DataFrame:
        """Monitor data quality for specific uitids."""
        # Note: Violations are processed in memory only, not stored in Neo4j
        
        # Run analysis
        analysis_agent = DQAnalysisAgent(self.trino_connector, self.neo4j_manager)
        result = analysis_agent.run_analysis(uitids)
        
        return result.get("report", pd.DataFrame())
    
    def get_violation_summary(self) -> pd.DataFrame:
        """Get a summary of all current violations."""
        return self.neo4j_manager.get_violations_summary()
    
    def get_violations_for_uitid(self, uitid: str) -> List[DQViolation]:
        """Get all violations for a specific uitid."""
        return self.neo4j_manager.get_violations_for_uitid(uitid) 