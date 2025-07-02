#!/usr/bin/env python3
"""
Quick Start Script for Data Quality Analysis

This is a simplified version for quick testing and demonstration.
Run this script to see the DQ system in action with minimal setup.
"""

import os
import sys
import logging
from typing import List

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)



def quick_demo():
    """Run a quick demonstration of the DQ system."""
    print("🚀 Data Quality Analysis - Quick Start Demo")
    print("=" * 50)
    
    try:
        # Import modules
        from database.neo4j_manager import Neo4jManager
        from database.trino_connector import TrinoConnector
        from agents.dq_agent import DQAnalysisAgent
        
        print("✅ All modules imported successfully")
        
        # Test Neo4j connection (will use defaults if env vars not set)
        print("\n📊 Testing Neo4j connection...")
        with Neo4jManager() as neo4j_manager:
            print("✅ Neo4j connection successful")
            
            # Get existing CDEs and rules (don't create sample data)
            cdes = neo4j_manager.get_all_cdes()
            rules = neo4j_manager.get_all_dq_rules()
            
            if not cdes and not rules:
                print("ℹ️  No CDEs or DQ Rules found in Neo4j database")
                print("✅ Connection test successful - no data modifications made")
                return
            
            print(f"✅ Found {len(cdes)} CDEs and {len(rules)} DQ Rules")
            
            # Display CDEs
            print("\n📋 Critical Data Elements (CDEs):")
            for cde in cdes:
                systems_str = ", ".join(cde.systems) if cde.systems else "No systems"
                data_type_str = cde.data_type if cde.data_type else "Unknown type"
                print(f"  • {cde.name} ({data_type_str}) - Systems: {systems_str}")
            
            # Display Rules
            print("\n📏 Data Quality Rules:")
            for rule in rules:
                systems_str = ", ".join(rule.systems) if rule.systems else "No systems"
                rule_id = rule.rule_id or rule.id or "Unknown ID"
                description = rule.description or "No description"
                print(f"  • {rule_id}: {description} - Systems: {systems_str}")
        
        print("\n🎉 Quick demo completed successfully!")
        print("\nNext steps:")
        print("1. Set up your database connections in .env file")  
        print("2. Run 'python sample_dq_workflow.py' for full workflow")
        print("3. Customize CDEs and rules in Neo4j for your specific needs")
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("Please install required dependencies: pip install -r requirements.txt")
    except Exception as e:
        print(f"❌ Error: {e}")
        print("Please check your Neo4j connection and configuration")



def test_components():
    """Test individual components."""
    print("\n🔧 Testing Individual Components")
    print("=" * 40)
    
    # Test configuration
    try:
        from config.database_config import SystemConfig
        print("✅ Configuration module loaded")
        
        systems = SystemConfig.get_all_systems()
        print(f"✅ Found {len(systems)} configured systems: {', '.join(systems.keys())}")
        
    except Exception as e:
        print(f"❌ Configuration test failed: {e}")
    
    # Test models
    try:
        from models.graph_schema import CDE, DQRule, DataType, RuleType
        
        # Create a sample CDE
        sample_cde = CDE(
            name="Test CDE",
            description="Test Critical Data Element",
            data_type=DataType.STRING,
            systems=["trade", "settlement"],
            table_name="trade",
            column_name="test_column"
        )
        print("✅ CDE model working")
        
        # Create a sample rule
        sample_rule = DQRule(
            rule_id="TEST001",
            name="Test Rule",
            description="Test data quality rule",
            rule_type=RuleType.NOT_NULL,
            cde_name="Test CDE",
            systems=["trade"],
            rule_definition={"field": "test_column"}
        )
        print("✅ DQRule model working")
        
    except Exception as e:
        print(f"❌ Models test failed: {e}")

def show_expected_output():
    """Show what the expected output should look like."""
    print("\n📊 Expected Output Format")
    print("=" * 30)
    
    sample_report = """
Data Quality Violation Report:
================================================================================
CDE         DQ_Rule_Desc                    uitid  Trade System  Settlement System  Reporting System
Trade Date  Trade Date cannot be null       T002   No            No                 No
Trade Date  Trade Date cannot be null       T003   Yes           No                 Yes
Quantity    Quantity must be positive       T003   Yes           Yes                Yes
Symbol      Symbol cannot be empty string   T004   Yes           Yes                No
"""
    
    print(sample_report)
    
    print("📈 Analysis Summary:")
    print("Found 4 total data quality violations:")
    print("- trade: 3 violations")
    print("- settlement: 2 violations") 
    print("- reporting: 2 violations")

if __name__ == "__main__":
    print("ℹ️  Running connection test - no database modifications will be made")
    print()
    
    # Run connection test demo
    quick_demo()
    
    # Test components
    test_components()
    
    # Show expected output
    show_expected_output() 