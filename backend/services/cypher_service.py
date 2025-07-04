"""
Service for handling natural language to Cypher conversion and query execution
"""
import logging
from typing import Dict, List, Any, Optional
from langchain_openai import ChatOpenAI
from langchain.prompts import PromptTemplate
from langchain.schema import HumanMessage, SystemMessage
import os
from database.neo4j_manager import Neo4jManager

logger = logging.getLogger(__name__)

class CypherService:
    """Service for natural language to Cypher conversion and execution"""
    
    def __init__(self, neo4j_manager: Neo4jManager):
        self.neo4j_manager = neo4j_manager
        self.llm = ChatOpenAI(
            model="gpt-4",
            temperature=0.1,
            openai_api_key=os.getenv("OPENAI_API_KEY")
        )
        self.schema_info = None
        self._load_schema_info()
    
    def _load_schema_info(self):
        """Load and cache schema information from Neo4j"""
        try:
            with self.neo4j_manager.driver.session() as session:
                # Get node labels
                node_labels = session.run("CALL db.labels()").values()
                
                # Get relationship types
                relationship_types = session.run("CALL db.relationshipTypes()").values()
                
                # Get property keys
                property_keys = session.run("CALL db.propertyKeys()").values()
                
                # Get sample nodes for each label
                sample_nodes = {}
                for label in node_labels:
                    try:
                        result = session.run(f"MATCH (n:{label[0]}) RETURN n LIMIT 3")
                        sample_nodes[label[0]] = [dict(record["n"]) for record in result]
                    except Exception as e:
                        logger.warning(f"Could not get sample nodes for label {label[0]}: {e}")
                        sample_nodes[label[0]] = []
                
                self.schema_info = {
                    "node_labels": [label[0] for label in node_labels],
                    "relationship_types": [rel[0] for rel in relationship_types],
                    "property_keys": [prop[0] for prop in property_keys],
                    "sample_nodes": sample_nodes
                }
                
                logger.info(f"Schema information loaded successfully - Labels: {len(node_labels)}, "
                           f"Relationships: {len(relationship_types)}, Properties: {len(property_keys)}")
        except Exception as e:
            logger.error(f"Error loading schema info: {e}")
            # Set default empty schema if there's an error
            self.schema_info = {
                "node_labels": [],
                "relationship_types": [],
                "property_keys": [],
                "sample_nodes": {},
                "error": str(e)
            }
    
    async def natural_language_to_cypher(self, query: str, context: Optional[str] = None) -> str:
        """Convert natural language query to Cypher"""
        try:
            system_prompt = self._build_system_prompt(context)
            
            messages = [
                SystemMessage(content=system_prompt),
                HumanMessage(content=f"Convert this natural language query to Cypher: {query}")
            ]
            
            response = self.llm.invoke(messages)
            cypher_query = response.content.strip()
            
            # Clean up the response (remove markdown code blocks if present)
            if cypher_query.startswith("```cypher"):
                cypher_query = cypher_query[9:-3].strip()
            elif cypher_query.startswith("```"):
                cypher_query = cypher_query[3:-3].strip()
            
            logger.info(f"Converted NL query to Cypher: {cypher_query}")
            return cypher_query
            
        except Exception as e:
            logger.error(f"Error converting NL to Cypher: {e}")
            raise
    
    def _build_system_prompt(self, context: Optional[str] = None) -> str:
        """Build system prompt with schema information"""
        base_prompt = """You are an expert in converting natural language queries to Cypher queries for Neo4j.

Neo4j Database Schema:
- Node Labels: {node_labels}
- Relationship Types: {relationship_types}
- Property Keys: {property_keys}

Sample Data Structure:
{sample_data}

Guidelines:
1. Always return valid Cypher syntax
2. Use proper node labels and relationship types from the schema
3. Include LIMIT clauses for queries that might return many results
4. Use appropriate WHERE clauses for filtering
5. Return results in a meaningful format
6. Handle case sensitivity properly
7. Use parameterized queries when appropriate

Common Patterns:
- Find nodes: MATCH (n:Label) WHERE n.property = 'value' RETURN n
- Find relationships: MATCH (a:Label1)-[r:RELATIONSHIP]->(b:Label2) RETURN a, r, b
- Count queries: MATCH (n:Label) RETURN COUNT(n)
- Filter by properties: MATCH (n:Label) WHERE n.property CONTAINS 'text' RETURN n

{context_section}

Important: Only return the Cypher query, no explanation or additional text.
"""
        
        # Format sample data
        sample_data_str = ""
        if self.schema_info and "sample_nodes" in self.schema_info:
            for label, samples in self.schema_info["sample_nodes"].items():
                sample_data_str += f"\n{label} nodes example:\n"
                for sample in samples[:2]:  # Show only first 2 samples
                    sample_data_str += f"  {sample}\n"
        
        context_section = ""
        if context:
            context_section = f"\nAdditional Context:\n{context}"
        
        return base_prompt.format(
            node_labels=self.schema_info.get("node_labels", []) if self.schema_info else [],
            relationship_types=self.schema_info.get("relationship_types", []) if self.schema_info else [],
            property_keys=self.schema_info.get("property_keys", []) if self.schema_info else [],
            sample_data=sample_data_str,
            context_section=context_section
        )
    
    async def execute_cypher(self, cypher_query: str) -> List[Dict[str, Any]]:
        """Execute a Cypher query and return results"""
        try:
            with self.neo4j_manager.driver.session() as session:
                result = session.run(cypher_query)
                records = []
                for record in result:
                    # Convert Neo4j record to dictionary
                    record_dict = {}
                    for key, value in record.items():
                        if hasattr(value, '__dict__'):
                            # Handle Neo4j nodes and relationships
                            if hasattr(value, 'labels'):  # Node
                                record_dict[key] = {
                                    'labels': list(value.labels),
                                    'properties': dict(value)
                                }
                            elif hasattr(value, 'type'):  # Relationship
                                record_dict[key] = {
                                    'type': value.type,
                                    'properties': dict(value)
                                }
                            else:
                                record_dict[key] = dict(value)
                        else:
                            record_dict[key] = value
                    records.append(record_dict)
                
                logger.info(f"Executed Cypher query, returned {len(records)} records")
                return records
                
        except Exception as e:
            logger.error(f"Error executing Cypher query: {e}")
            raise
    
    async def get_schema_info(self) -> Dict[str, Any]:
        """Get schema information"""
        if not self.schema_info:
            self._load_schema_info()
        return self.schema_info
    
    async def get_sample_queries(self) -> List[Dict[str, str]]:
        """Get sample natural language queries with their Cypher equivalents"""
        return [
            {
                "natural_language": "Show me all data quality rules",
                "cypher": "MATCH (r:DQRule) RETURN r",
                "description": "Returns all data quality rules in the system"
            },
            {
                "natural_language": "Find all CDEs for the trade system",
                "cypher": "MATCH (c:CDE)-[:BELONGS_TO]->(s:System {name: 'trade'}) RETURN c, s",
                "description": "Returns all Common Data Elements for the trade system"
            },
            {
                "natural_language": "Show rules that validate completeness",
                "cypher": "MATCH (r:DQRule) WHERE r.rule_type = 'COMPLETENESS' RETURN r",
                "description": "Returns all completeness validation rules"
            },
            {
                "natural_language": "Find CDEs with their associated rules",
                "cypher": "MATCH (c:CDE)-[:VALIDATES]->(r:DQRule) RETURN c, r",
                "description": "Returns CDEs and their associated validation rules"
            },
            {
                "natural_language": "Show me all systems and their CDEs",
                "cypher": "MATCH (s:System)<-[:BELONGS_TO]-(c:CDE) RETURN s, collect(c) as cdes",
                "description": "Returns all systems with their associated CDEs"
            },
            {
                "natural_language": "Count total number of rules by type",
                "cypher": "MATCH (r:DQRule) RETURN r.rule_type, COUNT(r) as count",
                "description": "Returns count of rules grouped by rule type"
            },
            {
                "natural_language": "Find rules that check for null values",
                "cypher": "MATCH (r:DQRule) WHERE r.rule_type = 'COMPLETENESS' OR r.description CONTAINS 'null' RETURN r",
                "description": "Returns rules that validate null/missing values"
            },
            {
                "natural_language": "Show the data quality governance structure",
                "cypher": "MATCH (s:System)<-[:BELONGS_TO]-(c:CDE)-[:VALIDATES]->(r:DQRule) RETURN s, c, r",
                "description": "Returns the complete governance structure: systems, CDEs, and rules"
            }
        ]
    
    def close(self):
        """Close connections"""
        if self.neo4j_manager:
            self.neo4j_manager.close() 