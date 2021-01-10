from fastapi import APIRouter
from sqlalchemy import create_engine
import pandas as pd
import json

from pydantic import BaseModel
from typing import List


class TimeSeries(BaseModel):
    rpu: List[str]


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

@router.post("/timeSeries/", tags=["timeSeries"])
async def get_specific_timeseries(timeseries: TimeSeries):
    engine = create_engine(db_connection_url)
    list_ts = []
    print(timeseries.rpu)
    with engine.connect() as connection:
        for rpu in timeseries.rpu:
            result = connection.execute("SELECT * FROM {} WHERE rpu='{}'" .format(TABLA_USUARIOS, rpu))
            df_timeseries = pd.DataFrame(result.fetchall())
            list_ts.append(json.loads(df_timeseries.to_json(orient="index")))
    return list_ts

@router.get("/centroids/", tags=["timeSeries"])
async def read_centroids():
    engine = create_engine(db_connection_url)
    with engine.connect() as connection:
        result = connection.execute("SELECT * FROM {}".format(TABLA_CENTROIDES))
        df_centroids = pd.DataFrame(result.fetchall())
        print(df_centroids)
        # for index, row in df_centroids.iterrows():
        #     print('Cluster:',row[0], ' Data', row[1:])
        dict_values = { row[0] : row[1: ]for index, row in df_centroids.iterrows()}
        # return json.loads(df_centroids.to_json(orient="index"))
    return dict_values


@router.get("/centroids/{id}", tags=["timeSeries"])
async def get_centroid(id):
    engine = create_engine(db_connection_url)
    with engine.connect() as connection:
        result = connection.execute("SELECT * FROM {} WHERE centroid='{}'".format(TABLA_CENTROIDES, id))
        df_centroid = pd.DataFrame(result.fetchall())
        print(df_centroid.values[0])
        # for index, row in df_centroids.iterrows():
        #     print('Cluster:',row[0], ' Data', row[1:])
        dict_values = { row[0] : row[1: ]for index, row in df_centroid.iterrows()}
        # return json.loads(df_centroids.to_json(orient="index"))
    return df_centroid.values[0][1:].tolist()