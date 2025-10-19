from fastapi import APIRouter, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict, Any
import time
from datetime import datetime

from .schemas import (
    QueryRequest, QueryResponse, TableListResponse, 
    ExplainRequest, ExplainResponse, SystemStatsResponse,
    ErrorResponse, HealthResponse
)
from .core.db_engine import DatabaseEngine

router = APIRouter()

db_engine: DatabaseEngine = None

def get_db_engine() -> DatabaseEngine:
    if db_engine is None:
        raise HTTPException(status_code=500, detail="Database engine no inicializada")
    return db_engine

@router.post("/query", response_model=QueryResponse)
async def execute_query(
    request: QueryRequest,
    engine: DatabaseEngine = Depends(get_db_engine)
):
    try:
        result = engine.execute_query(request.query)
        
        return QueryResponse(
            success=result["success"],
            data=result.get("data"),
            count=result.get("count", 0),
            time=result.get("time", 0.0),
            error=result.get("error"),
            explain=result.get("explain")
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"Error ejecutando query: {str(e)}"
        )

@router.get("/tables", response_model=TableListResponse)
async def list_tables(engine: DatabaseEngine = Depends(get_db_engine)):
    try:
        result = engine.list_tables()
        
        if not result["success"]:
            raise HTTPException(status_code=500, detail=result.get("error", "Error listando tablas"))
        
        return TableListResponse(
            success=True,
            tables=result["tables"],
            count=result["count"]
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error listando tablas: {str(e)}"
        )

@router.get("/tables/{table_name}")
async def get_table_info(
    table_name: str,
    engine: DatabaseEngine = Depends(get_db_engine)
):
    try:
        tables_result = engine.list_tables()
        
        if not tables_result["success"]:
            raise HTTPException(status_code=500, detail="Error obteniendo informacion de tablas")
        
        table_info = None
        for table in tables_result["tables"]:
            if table["name"] == table_name:
                table_info = table
                break
        
        if not table_info:
            raise HTTPException(status_code=404, detail=f"Tabla '{table_name}' no encontrada")
        
        return {
            "success": True,
            "table": table_info
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error obteniendo informacion de tabla: {str(e)}"
        )

@router.post("/explain", response_model=ExplainResponse)
async def explain_query(
    request: ExplainRequest,
    engine: DatabaseEngine = Depends(get_db_engine)
):
    try:
        result = engine.explain_query(request.query)
        
        return ExplainResponse(
            success=result["success"],
            plan=result.get("plan"),
            error=result.get("error")
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"Error generando plan de ejecucion: {str(e)}"
        )

@router.get("/stats", response_model=SystemStatsResponse)
async def get_system_stats(engine: DatabaseEngine = Depends(get_db_engine)):
    try:
        result = engine.get_system_stats()
        
        return SystemStatsResponse(
            success=True,
            total_tables=result.get("total_tables", 0),
            total_records=result.get("total_records", 0),
            memory_usage=result.get("memory_usage"),
            uptime=result.get("uptime")
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error obteniendo estadisticas: {str(e)}"
        )

@router.get("/health", response_model=HealthResponse)
async def health_check():
    return HealthResponse(
        status="healthy",
        timestamp=datetime.now(),
        version="1.0.0"
    )

@router.get("/")
async def root():
    return {
        "message": "SQL Database Backend API",
        "version": "1.0.0",
        "endpoints": {
            "POST /query": "Ejecutar consultas SQL",
            "GET /tables": "Listar todas las tablas",
            "GET /tables/{name}": "Informacion de tabla especifica",
            "POST /explain": "Plan de ejecucion de consulta",
            "GET /stats": "Estadisticas del sistema",
            "GET /health": "Health check",
            "GET /docs": "Documentacion Swagger"
        }
    }