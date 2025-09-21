from fastapi import FastAPI
from contextlib import asynccontextmanager

# Espacio con las rutas
from . import routes

# Eventos cuando inicie el motor y cunado se apague
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Inicializando motor de base de datos...")
    yield 
    print("Apagando motor de base de datos...")


app = FastAPI(lifespan=lifespan)

app.include_router(routes.router)
