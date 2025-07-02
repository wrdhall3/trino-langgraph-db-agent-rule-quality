"""
Trino connector for accessing the three banking system databases.
"""
import trino
import pandas as pd
from typing import Dict, List, Optional, Any
from config.database_config import SystemConfig
import logging

logger = logging.getLogger(__name__)

class TrinoConnector:
    """Connector for accessing databases through Trino."""
    
    def __init__(self, config: Dict[str, Any] = None):
        """Initialize the Trino connector."""
        self.config = config or SystemConfig.TRINO_CONFIG
        self.connection = None
        self._connect()
    
    def _connect(self):
        """Establish connection to Trino."""
        try:
            self.connection = trino.dbapi.connect(
                host=self.config["host"],
                port=self.config["port"],
                user=self.config["user"],
                catalog=self.config["catalog"],
                schema=self.config["schema"]
            )
            logger.info(f"Connected to Trino at {self.config['host']}:{self.config['port']}")
        except Exception as e:
            logger.error(f"Failed to connect to Trino: {e}")
            raise
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute a query and return results as a DataFrame."""
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            
            # Fetch column names
            columns = [desc[0] for desc in cursor.description]
            
            # Fetch all rows
            rows = cursor.fetchall()
            
            # Create DataFrame
            df = pd.DataFrame(rows, columns=columns)
            return df
            
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            logger.error(f"Query: {query}")
            raise
    
    def get_trade_data(self, system_name: str, uitids: Optional[List[str]] = None) -> pd.DataFrame:
        """Get trade data from a specific system."""
        system_config = SystemConfig.get_system_config(system_name)
        if not system_config:
            raise ValueError(f"Unknown system: {system_name}")
        
        # Build the query
        schema_name = f"{system_config.database}.{system_config.schema}"
        table_name = "trade"  # Assuming all systems have a trade table
        
        query = f"""
        SELECT *
        FROM {schema_name}.{table_name}
        """
        
        if uitids:
            uitid_list = "', '".join(uitids)
            query += f" WHERE uitid IN ('{uitid_list}')"
        
        return self.execute_query(query)
    
    def get_all_trade_data(self, uitids: Optional[List[str]] = None) -> Dict[str, pd.DataFrame]:
        """Get trade data from all systems."""
        systems = SystemConfig.get_all_systems()
        results = {}
        
        for system_name in systems.keys():
            try:
                results[system_name] = self.get_trade_data(system_name, uitids)
                logger.info(f"Retrieved {len(results[system_name])} records from {system_name}")
            except Exception as e:
                logger.error(f"Failed to retrieve data from {system_name}: {e}")
                results[system_name] = pd.DataFrame()  # Empty DataFrame for failed systems
        
        return results
    
    def get_cde_values(self, system_name: str, cde_name: str, uitids: Optional[List[str]] = None) -> pd.DataFrame:
        """Get specific CDE values from a system."""
        system_config = SystemConfig.get_system_config(system_name)
        if not system_config:
            raise ValueError(f"Unknown system: {system_name}")
        
        # This would need to be dynamically determined based on the CDE definition
        # For now, we'll assume the column name is the same as the CDE name (lowercase)
        column_name = cde_name.lower().replace(" ", "_")
        
        schema_name = f"{system_config.database}.{system_config.schema}"
        table_name = "trade"
        
        query = f"""
        SELECT uitid, {column_name}
        FROM {schema_name}.{table_name}
        """
        
        if uitids:
            uitid_list = "', '".join(uitids)
            query += f" WHERE uitid IN ('{uitid_list}')"
        
        return self.execute_query(query)
    
    def get_system_schema(self, system_name: str) -> pd.DataFrame:
        """Get schema information for a system."""
        system_config = SystemConfig.get_system_config(system_name)
        if not system_config:
            raise ValueError(f"Unknown system: {system_name}")
        
        schema_name = f"{system_config.database}.{system_config.schema}"
        
        query = f"""
        DESCRIBE {schema_name}.trade
        """
        
        return self.execute_query(query)
    
    def get_all_system_schemas(self) -> Dict[str, pd.DataFrame]:
        """Get schema information for all systems."""
        systems = SystemConfig.get_all_systems()
        results = {}
        
        for system_name in systems.keys():
            try:
                results[system_name] = self.get_system_schema(system_name)
            except Exception as e:
                logger.error(f"Failed to get schema from {system_name}: {e}")
                results[system_name] = pd.DataFrame()
        
        return results
    
    def close(self):
        """Close the Trino connection."""
        if self.connection:
            self.connection.close()
            logger.info("Trino connection closed")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close() 