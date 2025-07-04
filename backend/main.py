"""
FastAPI backend for the Data Quality UI
Provides endpoints for natural language GraphDB querying and DQ violations display
"""
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import logging
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.neo4j_manager import Neo4jManager
from database.trino_connector import TrinoConnector
from agents.dq_agent import DQAnalysisAgent
from backend.services.cypher_service import CypherService
from backend.services.dq_service import DQService

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Data Quality Management API",
    description="API for natural language GraphDB querying and DQ violations management",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # React dev server
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic models
class NaturalLanguageQuery(BaseModel):
    query: str
    context: Optional[str] = None

class CypherQuery(BaseModel):
    cypher: str

class DQAnalysisRequest(BaseModel):
    uitids: Optional[List[str]] = None

class APIResponse(BaseModel):
    success: bool
    data: Any
    message: Optional[str] = None

# Global services (initialized on startup)
cypher_service: CypherService = None
dq_service: DQService = None

@app.on_event("startup")
async def startup_event():
    """Initialize services on startup"""
    global cypher_service, dq_service
    try:
        neo4j_manager = Neo4jManager()
        trino_connector = TrinoConnector()
        cypher_service = CypherService(neo4j_manager)
        dq_service = DQService(trino_connector, neo4j_manager)
        logger.info("Services initialized successfully")
    except Exception as e:
        logger.error(f"Error initializing services: {e}")
        raise

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    global cypher_service, dq_service
    if cypher_service:
        cypher_service.close()
    if dq_service:
        dq_service.close()

# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "message": "API is running"}

# GraphDB endpoints
@app.post("/api/graphdb/nl-to-cypher")
async def natural_language_to_cypher(query: NaturalLanguageQuery) -> APIResponse:
    """Convert natural language to Cypher query"""
    try:
        cypher_query = await cypher_service.natural_language_to_cypher(query.query, query.context)
        return APIResponse(
            success=True,
            data={"cypher": cypher_query, "original_query": query.query},
            message="Successfully converted natural language to Cypher"
        )
    except Exception as e:
        logger.error(f"Error converting NL to Cypher: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/graphdb/execute-cypher")
async def execute_cypher(query: CypherQuery) -> APIResponse:
    """Execute a Cypher query against the GraphDB"""
    try:
        results = await cypher_service.execute_cypher(query.cypher)
        return APIResponse(
            success=True,
            data={"results": results, "query": query.cypher},
            message="Cypher query executed successfully"
        )
    except Exception as e:
        logger.error(f"Error executing Cypher: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/graphdb/schema")
async def get_graph_schema() -> APIResponse:
    """Get the GraphDB schema information"""
    try:
        schema = await cypher_service.get_schema_info()
        return APIResponse(
            success=True,
            data=schema,
            message="Schema retrieved successfully"
        )
    except Exception as e:
        logger.error(f"Error getting schema: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/graphdb/sample-queries")
async def get_sample_queries() -> APIResponse:
    """Get sample natural language queries with their Cypher equivalents"""
    try:
        samples = await cypher_service.get_sample_queries()
        return APIResponse(
            success=True,
            data=samples,
            message="Sample queries retrieved successfully"
        )
    except Exception as e:
        logger.error(f"Error getting sample queries: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Data Quality endpoints
@app.post("/api/dq/analyze")
async def analyze_data_quality(request: DQAnalysisRequest) -> APIResponse:
    """Run data quality analysis"""
    try:
        results = await dq_service.run_analysis(request.uitids)
        return APIResponse(
            success=True,
            data=results,
            message="Data quality analysis completed successfully"
        )
    except Exception as e:
        logger.error(f"Error running DQ analysis: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/dq/violations")
async def get_violations() -> APIResponse:
    """Get current DQ violations"""
    try:
        violations = await dq_service.get_violations()
        return APIResponse(
            success=True,
            data=violations,
            message="Violations retrieved successfully"
        )
    except Exception as e:
        logger.error(f"Error getting violations: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/dq/rules")
async def get_dq_rules() -> APIResponse:
    """Get all DQ rules"""
    try:
        rules = await dq_service.get_all_rules()
        return APIResponse(
            success=True,
            data=rules,
            message="DQ rules retrieved successfully"
        )
    except Exception as e:
        logger.error(f"Error getting DQ rules: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/dq/cdes")
async def get_cdes() -> APIResponse:
    """Get all CDEs"""
    try:
        cdes = await dq_service.get_all_cdes()
        return APIResponse(
            success=True,
            data=cdes,
            message="CDEs retrieved successfully"
        )
    except Exception as e:
        logger.error(f"Error getting CDEs: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/dq/systems")
async def get_systems() -> APIResponse:
    """Get all systems information"""
    try:
        systems = await dq_service.get_systems_info()
        return APIResponse(
            success=True,
            data=systems,
            message="Systems information retrieved successfully"
        )
    except Exception as e:
        logger.error(f"Error getting systems: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/dq/export-csv")
async def export_violations_csv() -> APIResponse:
    """Export current violations to CSV file"""
    try:
        csv_filename = await dq_service.export_violations_to_csv()
        return APIResponse(
            success=True,
            data={"csv_file": csv_filename},
            message="Violations exported to CSV successfully"
        )
    except Exception as e:
        logger.error(f"Error exporting violations to CSV: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True) 