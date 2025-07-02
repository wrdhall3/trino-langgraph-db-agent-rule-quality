#!/usr/bin/env python3
"""
Sample script to run the full Data Quality workflow.

This script demonstrates:
1. Setting up connections to Trino and Neo4j
2. Initializing the graph schema with sample data
3. Creating sample trade data in the databases
4. Running the DQ analysis workflow
5. Generating and displaying violation reports

Prerequisites:
- Trino server running and configured
- Neo4j database running
- MySQL databases for the three systems
- Environment variables set for database connections
"""

import os
import sys
import logging
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import uuid

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.trino_connector import TrinoConnector
from database.neo4j_manager import Neo4jManager
from agents.dq_agent import DQAnalysisAgent, DQMonitoringAgent
from config.database_config import SystemConfig

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_sample_env_file():
    """Create a sample .env file with configuration templates."""
    env_content = """
# Database Configuration
# Trade System Database
TRADE_DB_HOST=localhost
TRADE_DB_PORT=3306
TRADE_DB_NAME=trade_system
TRADE_DB_USER=root
TRADE_DB_PASSWORD=password
TRADE_DB_SCHEMA=trade_system

# Settlement System Database
SETTLEMENT_DB_HOST=localhost
SETTLEMENT_DB_PORT=3306
SETTLEMENT_DB_NAME=settlement_system
SETTLEMENT_DB_USER=root
SETTLEMENT_DB_PASSWORD=password
SETTLEMENT_DB_SCHEMA=settlement_system

# Reporting System Database
REPORTING_DB_HOST=localhost
REPORTING_DB_PORT=3306
REPORTING_DB_NAME=reporting_system
REPORTING_DB_USER=root
REPORTING_DB_PASSWORD=password
REPORTING_DB_SCHEMA=reporting_system

# Neo4j Configuration
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=password

# Trino Configuration
TRINO_HOST=localhost
TRINO_PORT=8080
TRINO_USER=admin

# OpenAI Configuration (for LangGraph agents)
OPENAI_API_KEY=your_openai_api_key_here
"""
    
    if not os.path.exists('.env'):
        with open('.env', 'w') as f:
            f.write(env_content.strip())
        logger.info("Created sample .env file. Please update with your actual configuration.")
    else:
        logger.info(".env file already exists.")

def create_sample_trade_data() -> List[Dict[str, Any]]:
    """Create sample trade data for testing."""
    sample_trades = []
    
    # Sample data with some intentional DQ violations
    base_trades = [
        {
            'uitid': 'T001',
            'trade_date': '2024-01-15',
            'quantity': 100,
            'symbol': 'AAPL',
            'price': 150.25,
            'side': 'BUY'
        },
        {
            'uitid': 'T002',
            'trade_date': None,  # Violation: NULL trade date
            'quantity': 250,
            'symbol': 'GOOGL',
            'price': 2500.00,
            'side': 'SELL'
        },
        {
            'uitid': 'T003',
            'trade_date': '2024-01-16',
            'quantity': -50,  # Violation: Negative quantity
            'symbol': 'MSFT',
            'price': 300.75,
            'side': 'BUY'
        },
        {
            'uitid': 'T004',
            'trade_date': '2024-01-17',
            'quantity': 75,
            'symbol': '',  # Violation: Empty symbol
            'price': 45.50,
            'side': 'SELL'
        },
        {
            'uitid': 'T005',
            'trade_date': '2024-01-18',
            'quantity': 200,
            'symbol': 'TSLA',
            'price': 180.00,
            'side': 'BUY'
        }
    ]
    
    # Create variations for different systems (some systems might miss data)
    for trade in base_trades:
        # Trade system - all trades
        sample_trades.append({
            'system': 'trade',
            **trade
        })
        
        # Settlement system - might miss some trades or have different values
        if trade['uitid'] != 'T002':  # T002 missing in settlement
            settlement_trade = trade.copy()
            # Sometimes settlement has different trade dates (late settlement)
            if trade['uitid'] == 'T003':
                settlement_trade['trade_date'] = None  # Settlement date missing
            sample_trades.append({
                'system': 'settlement',
                **settlement_trade
            })
        
        # Reporting system - might have additional delays
        if trade['uitid'] not in ['T002', 'T004']:  # T002 and T004 missing in reporting
            reporting_trade = trade.copy()
            sample_trades.append({
                'system': 'reporting',
                **reporting_trade
            })
    
    return sample_trades

def setup_sample_databases(trino_connector: TrinoConnector):
    """Set up sample databases with trade tables (simulation)."""
    logger.info("Setting up sample databases...")
    
    # Note: In a real scenario, you would create the actual MySQL tables
    # This is just for demonstration purposes
    sample_data = create_sample_trade_data()
    
    logger.info(f"Created {len(sample_data)} sample trade records across all systems")
    
    # Group by system for reporting
    system_counts = {}
    for trade in sample_data:
        system = trade['system']
        if system not in system_counts:
            system_counts[system] = 0
        system_counts[system] += 1
    
    for system, count in system_counts.items():
        logger.info(f"  {system.title()} System: {count} trades")
    
    return sample_data

def run_dq_analysis_workflow(sample_uitids: List[str] = None):
    """Run the complete data quality analysis workflow."""
    logger.info("="*60)
    logger.info("STARTING DATA QUALITY ANALYSIS WORKFLOW")
    logger.info("="*60)
    
    try:
        # Initialize connections
        logger.info("1. Initializing database connections...")
        
        # Initialize Neo4j manager
        neo4j_manager = Neo4jManager()
        logger.info("   ✓ Neo4j connection established")
        
        # Initialize Trino connector
        trino_connector = TrinoConnector()
        logger.info("   ✓ Trino connection established")
        
        # Check existing Neo4j data
        logger.info("2. Checking Neo4j data...")
        cdes = neo4j_manager.get_all_cdes()
        rules = neo4j_manager.get_all_dq_rules()
        
        if not cdes and not rules:
            logger.warning("   ⚠️  No CDEs or DQ Rules found in Neo4j database")
            logger.info("   ℹ️  You may need to manually create CDEs and rules in Neo4j")
            logger.info("   ℹ️  Continuing with simulation for demonstration purposes...")
        else:
            logger.info(f"   ✓ Found {len(cdes)} CDEs and {len(rules)} DQ Rules in Neo4j")
        
        # Set up sample databases
        logger.info("3. Setting up sample trade data...")
        sample_data = setup_sample_databases(trino_connector)
        logger.info("   ✓ Sample trade data created")
        
        # Initialize the DQ Analysis Agent
        logger.info("4. Initializing DQ Analysis Agent...")
        dq_agent = DQAnalysisAgent(trino_connector, neo4j_manager)
        logger.info("   ✓ DQ Analysis Agent initialized")
        
        # Run the analysis workflow
        logger.info("5. Running DQ analysis workflow...")
        result = dq_agent.run_analysis(uitids=sample_uitids)
        logger.info("   ✓ DQ analysis completed")
        
        # Display results
        logger.info("6. Analysis Results:")
        logger.info("-" * 40)
        
        # Show workflow messages
        messages = result.get('messages', [])
        for i, message in enumerate(messages, 1):
            logger.info(f"   Step {i}: {message.content}")
        
        # Show violations report
        report = result.get('report')
        if report is not None and not report.empty:
            logger.info("\n7. Violation Report:")
            logger.info("-" * 40)
            print("\nData Quality Violation Report:")
            print("=" * 80)
            print(report.to_string(index=False))
            
            # Save report to CSV
            results_dir = "results"
            os.makedirs(results_dir, exist_ok=True)  # Create results directory if it doesn't exist
            
            report_filename = f"dq_violations_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            report_filepath = os.path.join(results_dir, report_filename)
            
            report.to_csv(report_filepath, index=False)
            logger.info(f"\n   ✓ Report saved to: {report_filepath}")
        else:
            logger.info("\n7. No violations found - all data quality checks passed!")
        
        # Show analysis summary
        analysis_summary = result.get('analysis_summary')
        if analysis_summary:
            logger.info("\n8. Analysis Summary:")
            logger.info("-" * 40)
            for line in analysis_summary.split('\n'):
                logger.info(f"   {line}")
        
        return result
        
    except Exception as e:
        logger.error(f"Error running DQ analysis workflow: {e}")
        raise
    finally:
        # Clean up connections
        try:
            neo4j_manager.close()
            trino_connector.close()
            logger.info("\n   ✓ Database connections closed")
        except:
            pass

def run_monitoring_example(uitids_to_monitor: Optional[List[str]] = None):
    """Run an example of continuous monitoring.
    
    Note: This monitors specific UITIDs that actually exist in the database.
    The previous main workflow found violations in real data and saved to CSV.
    This monitoring example demonstrates targeted monitoring of specific trades.
    
    Args:
        uitids_to_monitor: Optional list of UITIDs to monitor. If not provided,
                          will attempt to discover UITIDs from the database.
    """
    logger.info("\n" + "="*60)
    logger.info("RUNNING MONITORING EXAMPLE")
    logger.info("="*60)
    
    try:
        # Initialize connections
        neo4j_manager = Neo4jManager()
        trino_connector = TrinoConnector()
        
        # Initialize monitoring agent
        monitoring_agent = DQMonitoringAgent(trino_connector, neo4j_manager)
        
        # Use provided UITIDs or discover from database
        if uitids_to_monitor:
            specific_uitids = uitids_to_monitor
            logger.info(f"Using UITIDs from previous analysis: {specific_uitids}")
        else:
            logger.info("No UITIDs provided, discovering from database...")
            try:
                # Get some real UITIDs from the trade system
                sample_uitids_query = "SELECT DISTINCT uitid FROM trade_system.trade LIMIT 5"
                uitid_results = trino_connector.execute_query(sample_uitids_query)
                specific_uitids = [row[0] for row in uitid_results if row[0]] # Filter out null values
                
                if not specific_uitids:
                    logger.warning("No UITIDs found in database, using fallback UITIDs")
                    specific_uitids = ['UIT-0001-ABC', 'UIT-0002-XYZ', 'UIT-0003-DEF']
                else:
                    logger.info(f"Found {len(specific_uitids)} UITIDs in database")
            except Exception as e:
                logger.warning(f"Error querying UITIDs: {e}")
                logger.info("Using fallback UITIDs")
                specific_uitids = ['UIT-0001-ABC', 'UIT-0002-XYZ', 'UIT-0003-DEF']
        
        logger.info(f"Monitoring specific UITIDs: {specific_uitids}")
        
        report = monitoring_agent.monitor_specific_uitids(specific_uitids)
        
        if not report.empty:
            print("\nMonitoring Report for Specific UITIDs:")
            print("=" * 60)
            print(report.to_string(index=False))
            logger.info(f"Found {len(report)} violations in monitored UITIDs")
        else:
            logger.info("No violations found for monitored UITIDs")
            logger.info("This could mean:")
            logger.info("  1. These UITIDs don't exist in the database")
            logger.info("  2. These UITIDs exist but have no violations")
            logger.info("  3. The monitoring query needs to be adjusted")
        
        # Get overall violation summary
        summary = monitoring_agent.get_violation_summary()
        if not summary.empty:
            print("\nOverall Violation Summary:")
            print("=" * 40)
            print(summary.to_string(index=False))
        
    except Exception as e:
        logger.error(f"Error running monitoring example: {e}")
        raise
    finally:
        try:
            neo4j_manager.close()
            trino_connector.close()
        except:
            pass

def main():
    """Main function to run the complete demonstration."""
    print("Data Quality Analysis Workflow - Sample Script")
    print("=" * 60)
    
    # Create sample environment file
    create_sample_env_file()
    
    # Check if required environment variables are set
    required_env_vars = ['NEO4J_URI', 'NEO4J_USER', 'NEO4J_PASSWORD']
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    
    if missing_vars:
        logger.warning(f"Missing environment variables: {missing_vars}")
        logger.warning("Please update your .env file with the correct values")
        logger.info("Continuing with default values for demonstration...")
    
    try:
        # Run the main DQ analysis workflow
        result = run_dq_analysis_workflow()
        
        # Run monitoring example with UITIDs that had violations
        violating_uitids = []
        if result and result.get('violations'):
            violating_uitids = list(set([v.uitid for v in result.get('violations', [])]))
        
        run_monitoring_example(uitids_to_monitor=violating_uitids)
        
        logger.info("\n" + "="*60)
        logger.info("WORKFLOW COMPLETED SUCCESSFULLY!")
        logger.info("="*60)
        
        return result
        
    except Exception as e:
        logger.error(f"Workflow failed: {e}")
        logger.error("Please check your database connections and configuration")
        return None

if __name__ == "__main__":
    main() 