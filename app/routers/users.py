from fastapi import APIRouter
from sqlalchemy import create_engine
import pandas as pd
import geopandas as gpd
import json
from scipy import stats
from pydantic import BaseModel
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

class AnalysisSpecs(BaseModel):
    tarifa: str
    giro: str
    cluster: str
    type: str

router = APIRouter()

@router.get("/users/frente/{frente}", tags=["users"])
async def get_frente_users(frente: str):
    engine = create_engine(db_connection_url)
    with engine.connect() as connection:
        print(frente)
        df_frente_users = gpd.GeoDataFrame.from_postgis(
            sql="""
                SELECT * 
                FROM {} 
                WHERE cluster='-1' AND mz_ft_oid='{}'""".format(TABLA_USUARIOS, frente),
            con=connection,
            geom_col="geometry"
        )
    return json.loads(df_frente_users.to_json())

@router.get("/users/manzana/{manzana}", tags=["users"])
async def get_manzana_users(manzana: str):
    engine = create_engine(db_connection_url)
    with engine.connect() as connection:
        print(manzana)
        df_manzana_users = gpd.GeoDataFrame.from_postgis(
            sql="""
                SELECT * 
                FROM {} 
                WHERE cluster='-1' AND mz_cvegeo='{}'""".format(TABLA_USUARIOS, manzana),
            con=connection,
            geom_col="geometry"
        )
    return json.loads(df_manzana_users.to_json())

@router.get("/users/{rpu}", tags=["users"])
async def get_info_user(rpu: str):
    engine = create_engine(db_connection_url)
    with engine.connect() as connection:
        print(rpu)
        df_user = gpd.GeoDataFrame.from_postgis(
            sql="""
                SELECT * 
                FROM {} 
                WHERE rpu='{}'""".format(TABLA_USUARIOS,rpu),
            con=connection,
            geom_col="geometry"
        )
    return json.loads(df_user.to_json())

@router.get("/users/timeseries/{rpu}", tags=["users"])
async def get_consumos_user(rpu: str):
    engine = create_engine(db_connection_url)
    with engine.connect() as connection:
        print('Hola')

        result = connection.execute("SELECT * FROM {} WHERE rpu='{}'".format(TABLA_CONSUMOS, rpu))
        df_timeseries = pd.DataFrame(result.fetchall(), columns=result._metadata.keys)
        print(df_timeseries)
        df_timeseries.sort_values(by="mesconsumo", inplace=True)
        print('Returning', df_timeseries)
        print('Suma de kwh', df_timeseries["kwh"].sum())
        # print(stats.zscore(df_timeseries["kwh"]))

        if( df_timeseries["kwh"].sum() == 0):
            response = {
                "df": json.loads(df_timeseries.to_json(orient="records")),
                "zconsumos" : df_timeseries["kwh"].tolist()
            }
        else:
            response = {
                "df": json.loads(df_timeseries.to_json(orient="records")),
                "zconsumos": stats.zscore(df_timeseries["kwh"]).tolist()
            }

        print('Response', response)

    return response


@router.post("/users/analisis/", tags=["users"])
async def get_analysis(analysis: AnalysisSpecs):
    engine = create_engine(db_connection_url)
    with engine.connect() as connection:

        result = connection.execute("SELECT * FROM {}".format(TABLA_CONSUMOS))
        df_consumos = pd.DataFrame(result.fetchall())
        df_consumos.rename(columns={0: 'rpu', 1: 'mesConsumo', 2: 'kwh', 3: 'Year', 4:'Month', 5:'Weekday'}, inplace=True)

        df_usuarios = gpd.GeoDataFrame.from_postgis(
            sql="""
                        SELECT * 
                        FROM {} 
                        WHERE tarifa='{}' OR giro='{}'""".format(TABLA_USUARIOS, analysis.tarifa, analysis.giro),
            con=connection,
            geom_col="geometry"
        )

        print(analysis)
        print(df_usuarios.head())
        print(df_consumos.head())

        users_merged = df_usuarios.merge(
            df_consumos[['rpu', 'mesConsumo', 'kwh']].sort_values(by=['rpu', 'mesConsumo']).pivot(
                index="rpu",
                columns="mesConsumo",
                values="kwh"
            ),
            left_on="rpu",
            right_on="rpu"
        )

        date_cols = list(df_consumos[['rpu', 'mesConsumo', 'kwh']].sort_values(by=['rpu', 'mesConsumo']).pivot(
            index="rpu",
            columns="mesConsumo",
            values="kwh"
        ).columns)

        # print(users_merged.head())

        filtro_giro = (users_merged['giro'] == analysis.giro)
        filtro_tarifa = (users_merged['tarifa'] == analysis.tarifa)
        filtro_both = (users_merged['tarifa'] == analysis.tarifa) & (users_merged['giro'] == analysis.giro)

        if(analysis.type == 'median'):
            print('Median calculated')
            response = {
                'filtro_giro': users_merged[filtro_giro][date_cols].median().dropna(),
                'filtro_tarifa': users_merged[filtro_tarifa][date_cols].median().dropna(),
                'filtro_ambos': users_merged[filtro_both][date_cols].median().dropna()
            }
        else:
            print('Mean calculated')
            response = {
                'filtro_giro': users_merged[filtro_giro][date_cols].mean().dropna(),
                'filtro_tarifa': users_merged[filtro_tarifa][date_cols].mean().dropna(),
                'filtro_ambos': users_merged[filtro_both][date_cols].mean().dropna()
            }
        print('Respues',response)


    return response