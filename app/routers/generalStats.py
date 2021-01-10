from fastapi import APIRouter
from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()
# Obtenemos las variables de entorno
DATABASE = os.getenv("DATABASE")
TABLA_USUARIOS = os.getenv("TABLA_USUARIOS")
TABLA_CONSUMOS=os.getenv("TABLA_CONSUMOS")
TABLA_MANZANAS=os.getenv("TABLA_MANZANAS")
TABLA_FRENTES=os.getenv("TABLA_FRENTES")
TABLA_CENTROIDES=os.getenv("TABLA_CENTROIDES")
db_connection_url = "postgres://postgres:admin@127.0.0.1:5432/{}".format(DATABASE)

router = APIRouter()

@router.get("/stats/anomaly/count", tags=["generalStats"])
async def get_anomalies_users_count():
    engine = create_engine(db_connection_url)
    with engine.connect() as connection:
        result = connection.execute("""
            SELECT COUNT(*) AS no_user
            FROM {}
            WHERE cluster='-1'
        """.format(TABLA_USUARIOS))
        no_anomalies = pd.DataFrame(result.fetchall())
        result = connection.execute("""
                SELECT COUNT(*) 
                FROM {}
               """.format(TABLA_USUARIOS))
        total_users = pd.DataFrame(result.fetchall())
        response = {
            "total": total_users.iloc[0][0].item(),
            "anomalies" :  no_anomalies.iloc[0][0].item()
        }
        print(response)
    return response
