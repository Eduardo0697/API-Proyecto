# Documentacion
# http://127.0.0.1:8000/docs
# http://127.0.0.1:8000/redoc

# Ejecucion
# uvicorn app.zmain:app --reload

from fastapi import FastAPI
from app.routers import users, manzanas, timeSeries, anomalyAnalysis, generalStats
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

origins = [
    "http://localhost.tiangolo.com",
    "https://localhost.tiangolo.com",
    "http://localhost",
    "http://localhost:8080",
    "http://192.168.100.162:8080"  # Aplicacion web,
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return "Bienvenido"

app.include_router(users.router)
app.include_router(manzanas.router)
app.include_router(timeSeries.router)
app.include_router(anomalyAnalysis.router)
app.include_router(generalStats.router)