"""
Database configuration for the three banking systems and Neo4j graph database.
"""
from dataclasses import dataclass
from typing import Dict, Any
import os
from dotenv import load_dotenv

load_dotenv()

@dataclass
class DatabaseConfig:
    """Configuration for a single database connection."""
    host: str
    port: int
    database: str
    username: str
    password: str
    schema: str = "public"

@dataclass
class Neo4jConfig:
    """Configuration for Neo4j graph database."""
    uri: str
    username: str
    password: str
    database: str = "neo4j"

class SystemConfig:
    """Configuration for all systems in the banking environment."""
    
    # Database configurations
    TRADE_SYSTEM = DatabaseConfig(
        host=os.getenv("TRADE_DB_HOST", "localhost"),
        port=int(os.getenv("TRADE_DB_PORT", "3306")),
        database=os.getenv("TRADE_DB_NAME", "trade_system"),
        username=os.getenv("TRADE_DB_USER", "root"),
        password=os.getenv("TRADE_DB_PASSWORD", "Ridgewood2024"),
        schema=os.getenv("TRADE_DB_SCHEMA", "trade_system")
    )
    
    SETTLEMENT_SYSTEM = DatabaseConfig(
        host=os.getenv("SETTLEMENT_DB_HOST", "localhost"),
        port=int(os.getenv("SETTLEMENT_DB_PORT", "3306")),
        database=os.getenv("SETTLEMENT_DB_NAME", "settlement_system"),
        username=os.getenv("SETTLEMENT_DB_USER", "root"),
        password=os.getenv("SETTLEMENT_DB_PASSWORD", "Ridgewood2024"),
        schema=os.getenv("SETTLEMENT_DB_SCHEMA", "settlement_system")
    )
    
    REPORTING_SYSTEM = DatabaseConfig(
        host=os.getenv("REPORTING_DB_HOST", "localhost"),
        port=int(os.getenv("REPORTING_DB_PORT", "3306")),
        database=os.getenv("REPORTING_DB_NAME", "reporting_system"),
        username=os.getenv("REPORTING_DB_USER", "root"),
        password=os.getenv("REPORTING_DB_PASSWORD", "Ridgewood2024"),
        schema=os.getenv("REPORTING_DB_SCHEMA", "reporting_system")
    )
    
    # Neo4j configuration
    NEO4J = Neo4jConfig(
        uri=os.getenv("NEO4J_URI", "bolt://localhost:7687"),
        username=os.getenv("NEO4J_USER", "neo4j"),
        password=os.getenv("NEO4J_PASSWORD", "testtest"),
        database=os.getenv("NEO4J_DATABASE", "neo4j")
    )
    
    # Trino configuration
    TRINO_CONFIG = {
        "host": os.getenv("TRINO_HOST", "localhost"),
        "port": int(os.getenv("TRINO_PORT", "8080")),
        "user": os.getenv("TRINO_USER", "admin"),
        "catalog": "mysql",
        "schema": "default"
    }
    
    # System names for easy reference
    SYSTEMS = {
        "trade": "Trade System",
        "settlement": "Settlement System", 
        "reporting": "Reporting System"
    }
    
    # Common identifier field across all systems
    COMMON_ID_FIELD = "uitid"
    
    @classmethod
    def get_system_config(cls, system_name: str) -> DatabaseConfig:
        """Get configuration for a specific system."""
        config_map = {
            "trade": cls.TRADE_SYSTEM,
            "settlement": cls.SETTLEMENT_SYSTEM,
            "reporting": cls.REPORTING_SYSTEM
        }
        return config_map.get(system_name.lower())
    
    @classmethod
    def get_all_systems(cls) -> Dict[str, DatabaseConfig]:
        """Get all system configurations."""
        return {
            "trade": cls.TRADE_SYSTEM,
            "settlement": cls.SETTLEMENT_SYSTEM,
            "reporting": cls.REPORTING_SYSTEM
        } 