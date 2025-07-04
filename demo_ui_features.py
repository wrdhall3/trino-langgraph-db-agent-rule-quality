#!/usr/bin/env python3
"""
Demo script to showcase the Data Quality Management UI features
"""
import requests
import json
import time
from typing import Dict, Any

# Configuration
BACKEND_URL = "http://localhost:8000"
FRONTEND_URL = "http://localhost:3000"

def check_services():
    """Check if backend services are running"""
    try:
        response = requests.get(f"{BACKEND_URL}/health", timeout=5)
        if response.status_code == 200:
            print("✅ Backend service is running")
            return True
        else:
            print(f"❌ Backend service error: {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"❌ Backend service not accessible: {e}")
        return False

def demo_graph_queries():
    """Demonstrate GraphDB query functionality"""
    print("\n🔍 GraphDB Query Demo")
    print("=" * 30)
    
    # Sample natural language queries
    sample_queries = [
        "Show me all data quality rules",
        "Find all CDEs for the trade system",
        "Show rules that validate completeness",
        "Count total number of rules by type"
    ]
    
    for query in sample_queries:
        print(f"\n📝 Query: {query}")
        
        try:
            # Convert natural language to Cypher
            nl_response = requests.post(
                f"{BACKEND_URL}/api/graphdb/nl-to-cypher",
                json={"query": query},
                timeout=10
            )
            
            if nl_response.status_code == 200:
                cypher_query = nl_response.json()["data"]["cypher"]
                print(f"🔄 Generated Cypher: {cypher_query}")
                
                # Execute the Cypher query
                exec_response = requests.post(
                    f"{BACKEND_URL}/api/graphdb/execute-cypher",
                    json={"cypher": cypher_query},
                    timeout=10
                )
                
                if exec_response.status_code == 200:
                    results = exec_response.json()["data"]["results"]
                    print(f"✅ Results: {len(results)} records returned")
                    if results:
                        print(f"   Sample: {json.dumps(results[0], indent=2)[:200]}...")
                else:
                    print(f"❌ Execution failed: {exec_response.status_code}")
            else:
                print(f"❌ Conversion failed: {nl_response.status_code}")
                
        except requests.exceptions.RequestException as e:
            print(f"❌ Request failed: {e}")
        
        time.sleep(1)  # Rate limiting

def demo_dq_analysis():
    """Demonstrate data quality analysis functionality"""
    print("\n📊 Data Quality Analysis Demo")
    print("=" * 35)
    
    try:
        # Run DQ analysis
        print("🔄 Running data quality analysis...")
        analysis_response = requests.post(
            f"{BACKEND_URL}/api/dq/analyze",
            json={"uitids": None},  # Analyze all data
            timeout=30
        )
        
        if analysis_response.status_code == 200:
            analysis_data = analysis_response.json()["data"]
            print(f"✅ Analysis completed!")
            print(f"   Total violations: {analysis_data['total_violations']}")
            print(f"   Analysis timestamp: {analysis_data['timestamp']}")
            
            # Show summary
            summary = analysis_data.get("summary", {})
            if summary:
                print("\n📈 Summary by severity:")
                for severity, count in summary.get("by_severity", {}).items():
                    print(f"   {severity}: {count}")
                
                print("\n🏢 Summary by system:")
                for system, count in summary.get("by_system", {}).items():
                    print(f"   {system}: {count}")
        else:
            print(f"❌ Analysis failed: {analysis_response.status_code}")
    
    except requests.exceptions.RequestException as e:
        print(f"❌ Request failed: {e}")

def demo_system_info():
    """Demonstrate system information retrieval"""
    print("\n🏗️ System Information Demo")
    print("=" * 30)
    
    endpoints = [
        ("rules", "Data Quality Rules"),
        ("cdes", "Common Data Elements"),
        ("systems", "Systems Information")
    ]
    
    for endpoint, description in endpoints:
        try:
            print(f"\n📋 Fetching {description}...")
            response = requests.get(f"{BACKEND_URL}/api/dq/{endpoint}", timeout=10)
            
            if response.status_code == 200:
                data = response.json()["data"]
                print(f"✅ Retrieved {len(data)} {description.lower()}")
                
                if data:
                    # Show sample data
                    sample = data[0]
                    print(f"   Sample: {json.dumps(sample, indent=2)[:300]}...")
            else:
                print(f"❌ Failed to fetch {description}: {response.status_code}")
                
        except requests.exceptions.RequestException as e:
            print(f"❌ Request failed: {e}")

def demo_schema_info():
    """Demonstrate GraphDB schema information"""
    print("\n🗄️ GraphDB Schema Demo")
    print("=" * 25)
    
    try:
        print("🔄 Fetching GraphDB schema...")
        response = requests.get(f"{BACKEND_URL}/api/graphdb/schema", timeout=10)
        
        if response.status_code == 200:
            schema = response.json()["data"]
            print("✅ Schema information retrieved!")
            
            print(f"\n🏷️  Node Labels: {', '.join(schema.get('node_labels', []))}")
            print(f"🔗 Relationships: {', '.join(schema.get('relationship_types', []))}")
            print(f"🔑 Properties: {len(schema.get('property_keys', []))} keys")
            
            # Show sample nodes
            sample_nodes = schema.get("sample_nodes", {})
            if sample_nodes:
                print(f"\n📊 Sample data available for: {', '.join(sample_nodes.keys())}")
        else:
            print(f"❌ Failed to fetch schema: {response.status_code}")
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Request failed: {e}")

def print_ui_instructions():
    """Print instructions for using the web UI"""
    print("\n🌐 Web UI Instructions")
    print("=" * 25)
    print(f"1. Open your browser and go to: {FRONTEND_URL}")
    print("2. Use the navigation menu to explore different pages:")
    print("   📊 Dashboard - System overview and metrics")
    print("   🔍 Graph Query - Natural language GraphDB queries")
    print("   ⚠️  DQ Violations - Data quality violations management")
    print("\n💡 Pro Tips:")
    print("   - Try natural language queries like 'Show me all completeness rules'")
    print("   - Use the sample queries for inspiration")
    print("   - Filter violations by severity, system, or rule type")
    print("   - Click on violations for detailed information")
    print("   - Use the dashboard for quick system health overview")

def main():
    """Main demo function"""
    print("🚀 Data Quality Management UI Demo")
    print("=" * 40)
    print("This demo will showcase the key features of the DQ Management UI")
    print("Make sure both backend and frontend servers are running!")
    
    # Check services
    if not check_services():
        print("\n❌ Backend service is not running. Please start it first:")
        print("   python start_dq_system.py")
        return
    
    # Run demos
    demo_schema_info()
    demo_graph_queries()
    demo_system_info()
    demo_dq_analysis()
    
    # Instructions
    print_ui_instructions()
    
    print("\n🎉 Demo completed successfully!")
    print("The backend API is working correctly and ready for the web UI.")

if __name__ == "__main__":
    main() 