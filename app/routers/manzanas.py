from fastapi import APIRouter
from sqlalchemy import create_engine
import geopandas as gpd
import json
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

@router.get("/manzanas/{cve}", tags=["manzanas"])
async def get_info_manzana(cve: str):
    engine = create_engine(db_connection_url+"")
    with engine.connect() as connection:
        df_manzanas = gpd.GeoDataFrame.from_postgis(
            sql="""
                SELECT * 
                FROM {} 
                WHERE "CVEGEO"='{}'""".format(TABLA_MANZANAS, cve),
            con=connection,
            geom_col="geometry"
        )
        print(df_manzanas)

    return json.loads(df_manzanas.to_json())

@router.get("/frente/{oid}", tags=["manzanas"])
async def get_info_frente_manzana(oid: str):
    engine = create_engine(db_connection_url+"")
    with engine.connect() as connection:
        df_frentes = gpd.GeoDataFrame.from_postgis(
            sql="""
                SELECT * 
                FROM {} 
                WHERE "OID"='{}'""".format(TABLA_FRENTES, oid),
            con=connection,
            geom_col="geometry"
        )
        print(df_frentes)
    return json.loads(df_frentes.to_json())


@router.get("/cloropeth_manzanas/", tags=["manzanas"])
async def get_cloropeth_manzanas():
    engine = create_engine(db_connection_url)
    with engine.connect() as connection:
        df_manzanas_count = gpd.GeoDataFrame.from_postgis(
            sql="""
                SELECT * FROM
                (SELECT mz_cvegeo, COUNT(rpu) AS numero_usuarios
                FROM {}
                WHERE cluster='-1'
                GROUP BY mz_cvegeo) AS manzanas_count,
                (SELECT "CVEGEO", "geometry"
                FROM {}) as manzanas_geometry
                WHERE manzanas_geometry."CVEGEO" = manzanas_count.mz_cvegeo
                """.format(TABLA_USUARIOS, TABLA_MANZANAS),
            con=connection,
            geom_col="geometry"
        )
        print(df_manzanas_count)
    return json.loads(df_manzanas_count.to_json())

@router.get("/cloropeth/vialidades", tags=["manzanas"])
async def get_cloropeth_vialidades():
    engine = create_engine(db_connection_url)
    with engine.connect() as connection:
        df_manzanas_count = gpd.GeoDataFrame.from_postgis(
            sql="""
                 SELECT * FROM
                (SELECT  mz_ft_oid, COUNT(rpu) AS numero_usuarios
                FROM {}
                WHERE cluster='-1'
                GROUP BY  mz_ft_oid) AS frentes_count,
                (SELECT "OID", "geometry"
                FROM {}) as frentes_geometry
                WHERE CAST(frentes_geometry."OID" AS varchar) = CAST(frentes_count.mz_ft_oid AS varchar)
                """.format(TABLA_USUARIOS, TABLA_FRENTES),
            con=connection,
            geom_col="geometry"
        )

        print(df_manzanas_count['numero_usuarios'].max())
        df_manzanas_count['maxValue'] = 30
        print(df_manzanas_count.head())
    return json.loads(df_manzanas_count.to_json())