from fastapi import APIRouter
from sqlalchemy import create_engine
import pandas as pd
import json
import gower
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

@router.get("/anomaly/analysis/cluster/{cluster}", tags=["AnomalyAnalysis"])
async def get_cluster_stats(cluster: str):
    engine = create_engine(db_connection_url)
    with engine.connect() as connection:
        result = connection.execute("""
        SELECT * 
        FROM {}
        WHERE cluster = '{}'
        """.format(TABLA_USUARIOS, cluster))
        df_users = pd.DataFrame(result.fetchall(), columns=result._metadata.keys)
        print(df_users['consumopro'])

        response = {
            "consumoPro": df_users['consumopro'].tolist()
        }
    return response

@router.get("/anomaly/analysis/similar/{rpu}", tags=["AnomalyAnalysis"])
async def get_nearest_cluster(rpu: str):
    engine = create_engine(db_connection_url)
    with engine.connect() as connection:
        result = connection.execute("""
            SELECT * 
            FROM {}
        """.format(TABLA_USUARIOS))
        list_cols = ['consumopro','timeseries', 'tipotarifa', 'tarifa' ,'divisiongiro', 'statusmedi', 'multiplica', 'hilos']
        list_cols_2 = ['consumopro', 'timeseries', 'tipotarifa', 'tarifa', 'divisiongiro', 'statusmedi', 'multiplica','hilos', 'cluster']
        df_users = pd.DataFrame(result.fetchall(), columns=result._metadata.keys)
        df_users.set_index("rpu", inplace=True)
        df_users['hilos'] = df_users['hilos'].astype('object')
        df_users['timeseries'] = df_users['timeseries'].astype('object')
        df_users['consumopro'] = df_users['consumopro'].astype('float64')
        # print(df_users[list_cols].info())
        # print( df_users['divisiongiro'].value_counts() / len(df_users))
        print('Usuario en cuestion \n', df_users[list_cols].loc[[rpu]])
        near_users = gower.gower_topn(df_users[list_cols].loc[[rpu]], df_users[list_cols], n=1000)
        # print(near_users)
        # print(near_users['index'].tolist())
        users_inspect =  df_users[list_cols_2].iloc[near_users['index'].tolist()]
        # print((users_inspect['cluster'] == '-1'))
        print('Usuarios mas parecidos que si pertenecen a un cluster \n', users_inspect[~(users_inspect['cluster'] == -1)])
        print('Usuario mas cercano: ', users_inspect[~(users_inspect['cluster'] == -1)].iloc[[0]].index.values)
        response = {
            "rpu" : users_inspect[~(users_inspect['cluster'] == -1)].iloc[[0]].index.values[0],
            "cluster" : users_inspect[~(users_inspect['cluster'] == -1)].iloc[[0]]['cluster'].item()
        }
    return response

@router.get("/anomaly/analysis/cluster/{cluster}", tags=["AnomalyAnalysis"])
async def get_cluster_stats(cluster: str):
    engine = create_engine(db_connection_url)
    with engine.connect() as connection:
        result = connection.execute("""
        SELECT * 
        FROM {}
        WHERE cluster = '{}'
        """.format(TABLA_USUARIOS, cluster))
        df_users = pd.DataFrame(result.fetchall(), columns=result._metadata.keys)
        print(df_users['consumopro'])

        response = {
            "consumoPro": df_users['consumopro'].tolist()
        }
    return response


@router.get("/cluster/analysis/{cluster}", tags=["AnomalyAnalysis"])
async def get_cluster_information(cluster: str):
    engine = create_engine(db_connection_url)
    with engine.connect() as connection:
        result = connection.execute("""
           SELECT COUNT(rpu) AS no_users 
           FROM {} 
           WHERE cluster='{}'
        """.format(TABLA_USUARIOS,cluster))
        df_cluster = pd.DataFrame(result.fetchall(), columns=result._metadata.keys)
        print(df_cluster)
    return json.loads(df_cluster.to_json(orient="index"))

@router.get("/anomaly/analysis/{table}/{topNumber}", tags=["AnomalyAnalysis"])
async def get_top_tabla(topNumber : int, table: str):
    engine = create_engine(db_connection_url)
    with engine.connect() as connection:
        result = connection.execute("""
            SELECT COUNT(rpu) AS count_usuarios ,{} 
            FROM {}
            WHERE cluster='-1'
            GROUP BY {}
            ORDER BY count_usuarios DESC
            LIMIT {}
        """.format(table, TABLA_USUARIOS,  table, topNumber))
        df_centroids = pd.DataFrame(result.fetchall())
        print(df_centroids)
    return json.loads(df_centroids.to_json(orient="index"))

@router.get("/anomaly/analysis/frentes/{table}/{topNumber}", tags=["AnomalyAnalysis"])
async def get_all_top_frentes_manzanas(table: str, topNumber: str):
    engine = create_engine(db_connection_url)
    with engine.connect() as connection:
        query = """
            SELECT COUNT(users.rpu) AS count_usuarios, ft."{}"
            FROM {} as users
            INNER JOIN {} as ft
            ON users.mz_ft_oid = CAST(ft."OID" AS varchar)
            WHERE users.cluster='-1'
            GROUP BY ft."{}"
            ORDER BY count_usuarios DESC
        """.format(table, TABLA_USUARIOS, TABLA_FRENTES, table)
        if topNumber!='all':
            query += "LIMIT {}".format(topNumber)
        result = connection.execute(query)
        result = connection.execute(query)
        df_centroids = pd.DataFrame(result.fetchall())
        print(df_centroids)
    return json.loads(df_centroids.to_json(orient="index"))



@router.get("/anomaly/analysis/manzanas/{table}/{topNumber}", tags=["AnomalyAnalysis"])
async def get_all_top_manzanas(table: str, topNumber: str):
    engine = create_engine(db_connection_url)
    with engine.connect() as connection:
        query = """
            SELECT COUNT(users.rpu) AS count_usuarios, mz."{}"
            FROM {} as users
            INNER JOIN {} as mz
            ON users.mz_cvegeo = CAST(mz."CVEGEO" AS varchar)
            WHERE users.cluster='-1'
            GROUP BY mz."{}"
            ORDER BY count_usuarios DESC
        """.format(table, TABLA_USUARIOS, TABLA_MANZANAS, table)
        if topNumber!='all':
            query += "LIMIT {}".format(topNumber)
        result = connection.execute(query)
        df_centroids = pd.DataFrame(result.fetchall())
        print(df_centroids)
    return json.loads(df_centroids.to_json(orient="index"))

@router.get("/anomaly/analysis/continuous/manzanas/{table}", tags=["AnomalyAnalysis"])
async def get_continuous_data_manzanas(table: str):
    engine = create_engine(db_connection_url)
    with engine.connect() as connection:
        print('')
        query = """
           SELECT mz."{}"
            FROM {} as users
            INNER JOIN {} as mz
            ON users.mz_cvegeo = CAST(mz."CVEGEO" AS varchar)
            WHERE users.cluster='-1'
            ORDER BY mz."{}" DESC
        """.format(table, TABLA_USUARIOS, TABLA_MANZANAS, table)
        result = connection.execute(query)
        df = pd.DataFrame(result.fetchall())
    return json.loads(df.to_json(orient="values"))


@router.get("/anomaly/analysis/cluster/table/", tags=["AnomalyAnalysis"])
async def get_top_table_cluster(topNumber : str, table: str, cluster: str):
    engine = create_engine(db_connection_url)
    with engine.connect() as connection:
        query = """
            SELECT COUNT(rpu) AS count_usuarios ,{}
            FROM {}
            WHERE cluster='{}'
            GROUP BY {}
            ORDER BY count_usuarios DESC
        """.format(table,  TABLA_USUARIOS, cluster,  table)
        if topNumber != 'all':
            numberItems = " LIMIT {}".format(topNumber)
            query += numberItems
        result = connection.execute(query)
        df_users = pd.DataFrame(result.fetchall())
        print(df_users)
    return json.loads(df_users.to_json(orient="index"))



@router.get("/anomaly/analysis/2/frentes/{table}/{topNumber}", tags=["AnomalyAnalysis"])
async def get_proportions_frentes_manzanas2(table: str, topNumber: str):
    engine = create_engine(db_connection_url)
    with engine.connect() as connection:

        query0 = """
            SELECT DISTINCT(mz_ft_oid) 
            FROM {}
            where cluster = '-1'
            ORDER BY mz_ft_oid
        """.format(TABLA_USUARIOS)
        queryA = """
                    SELECT "OID", "{}"
                    FROM {}
                """.format(table, TABLA_FRENTES)

        query2 = """
            SELECT "{}", COUNT("OID") AS no_ocurrences
            FROM {}
            GROUP BY "{}"
            ORDER BY "{}"
        """.format(table,TABLA_FRENTES, table,table)

        result0 = connection.execute(query0)
        resultA = connection.execute(queryA)
        result2 = connection.execute(query2)

        df_distincstOID = pd.DataFrame(result0.fetchall(), columns=result0._metadata.keys)

        df_FRENTES = pd.DataFrame(resultA.fetchall(), columns=resultA._metadata.keys)
        df_distincstOID["mz_ft_oid"] = df_distincstOID["mz_ft_oid"].astype("int64")

        print(df_distincstOID.dtypes)
        print(df_FRENTES.dtypes)
        merge_queries = df_distincstOID.merge(df_FRENTES, left_on="mz_ft_oid", right_on="OID")
        print(merge_queries)
        df_value_counts = merge_queries.value_counts(table).rename_axis(table).reset_index(name='counts')
        print(df_value_counts)
        df_counts_all = pd.DataFrame(result2.fetchall(), columns=result2._metadata.keys)
        print(df_counts_all)
        df_merge = df_value_counts.merge(df_counts_all, on=table)
        print(df_merge)
        data = { 0 : df_merge["counts"] / df_merge["no_ocurrences"], 1: df_merge[table]}
        df_proportions = pd.DataFrame(data).sort_values(by=0, ascending=False).reset_index(drop=True)
        print(df_proportions)
        if(topNumber == 'all'):
            response = json.loads(df_proportions.to_json(orient="index"))
        else:
            response = json.loads(df_proportions.head(int(topNumber)).to_json(orient="index"))
    return response

@router.get("/anomaly/analysis/2/manzanas/{table}/{topNumber}", tags=["AnomalyAnalysis"])
async def get_proportions_manzanas2(table: str, topNumber: str):
    engine = create_engine(db_connection_url)
    with engine.connect() as connection:

        query0 = """
            SELECT DISTINCT(mz_cvegeo) 
            FROM {}
            where cluster = '-1'
            ORDER BY mz_cvegeo
        """.format(TABLA_USUARIOS)
        queryA = """
                    SELECT "CVEGEO", "{}"
                    FROM {}
                """.format(table, TABLA_MANZANAS)
        # query = """
        #     SELECT COUNT(users.rpu) AS count_usuarios, ft."{}"
        #     FROM {} as users
        #     INNER JOIN {} as ft
        #     ON users.mz_ft_oid = CAST(ft."OID" AS varchar)
        #     WHERE users.cluster='-1'
        #     GROUP BY ft."{}"
        #     ORDER BY ft."{}"
        # """.format(table, TABLA_USUARIOS, TABLA_FRENTES, table, table)
        # if topNumber!='all':
        #     query += "LIMIT {}".format(topNumber)
        query2 = """
            SELECT "{}", COUNT("CVEGEO") AS no_ocurrences
            FROM {}
            GROUP BY "{}"
            ORDER BY "{}"
        """.format(table,TABLA_MANZANAS, table,table)
        result0 = connection.execute(query0)
        resultA = connection.execute(queryA)
        result2 = connection.execute(query2)
        df_distincstOID = pd.DataFrame(result0.fetchall(), columns=result0._metadata.keys)
        df_FRENTES = pd.DataFrame(resultA.fetchall(), columns=resultA._metadata.keys)
        # df_distincstOID["mz_cvegeo"] = df_distincstOID["mz_cvegeo"].astype("int64")
        # print(df_distincstOID.dtypes)
        # print(df_FRENTES.dtypes)
        merge_queries = df_distincstOID.merge(df_FRENTES, left_on="mz_cvegeo", right_on="CVEGEO")
        print(merge_queries)
        df_value_counts = merge_queries.value_counts(table).rename_axis(table).reset_index(name='counts')
        print(df_value_counts)
        df_counts_all = pd.DataFrame(result2.fetchall(), columns=result2._metadata.keys)
        print(df_counts_all)
        df_merge = df_value_counts.merge(df_counts_all, on=table)
        print(df_merge)
        data = { 0 : df_merge["counts"] / df_merge["no_ocurrences"], 1: df_merge[table]}
        df_proportions = pd.DataFrame(data).sort_values(by=0, ascending=False).reset_index(drop=True)
        print(df_proportions)
        if(topNumber == 'all'):
            response = json.loads(df_proportions.to_json(orient="index"))
        else:
            response = json.loads(df_proportions.head(int(topNumber)).to_json(orient="index"))
    return response