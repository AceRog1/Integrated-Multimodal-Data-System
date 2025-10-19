from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import os

from . import routes
from .core.db_engine import DatabaseEngine

db_engine: DatabaseEngine = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_engine
    print("Inicializando")
    data_dir = os.getenv("DATA_DIR", "data")
    os.makedirs(data_dir, exist_ok=True)
    
    #inicializar database engine
    db_engine = DatabaseEngine(data_dir=data_dir)
    routes.db_engine = db_engine
    print("Motor de base de datos inicializado correctamente")
    print(f"Directorio de datos: {data_dir}")
    print(f"Tablas cargadas: {len(db_engine.list_tables()['tables'])}")
    
    yield 
    
    print("Apagando motor de base de datos")
    print("Motor de base de datos apagado correctamente")

app = FastAPI(
    title="SQL Database Backend API",
    description="Backend para sistema de base de datos SQL con varias estructuras de indices",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(routes.router, prefix="/api/v1", tags=["database"])