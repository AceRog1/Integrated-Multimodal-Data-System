from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from datetime import datetime


class QueryRequest(BaseModel):
    query: str


class QueryResponse(BaseModel):
    success: bool
    data: Optional[List[Dict[str, Any]]] = None
    count: int = 0
    time: float = 0.0
    error: Optional[str] = None
    explain: Optional[str] = None


class TableInfo(BaseModel):
    name: str
    columns: List[Dict[str, Any]]
    primary_key: str
    primary_index_type: str
    record_size: int
    total_records: int
    active_records: int
    indexed_columns: List[str]


class TableListResponse(BaseModel):
    success: bool
    tables: List[TableInfo]
    count: int


class ExplainRequest(BaseModel):
    query: str


class ExplainResponse(BaseModel):
    success: bool
    plan: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


class SystemStatsResponse(BaseModel):
    success: bool
    total_tables: int
    total_records: int
    memory_usage: Optional[str] = None
    uptime: Optional[str] = None


class ErrorResponse(BaseModel):
    success: bool = False
    error: str
    error_code: Optional[str] = None


class HealthResponse(BaseModel):
    status: str = "healthy"
    timestamp: datetime
    version: str = "1.0.0"