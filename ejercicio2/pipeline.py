"""
Pipeline ETL — Ejercicio 2
==========================

Extrae datos desde:
  • PostgreSQL (lab5)
      - pais_poblacion
      - pais_envejecimiento
  • MongoDB Atlas (laboratorio5)
      - costos_turisticos
      - paises_mundo_big_mac

Integra ambos datasets en memoria con pandas
y carga el resultado en un Data Warehouse PostgreSQL.

Destino:
  • PostgreSQL DW (dw_database)
      - tabla: dw_paises
"""

import os
import sys
import logging
from typing import Optional

import pandas as pd
import psycopg2
import psycopg2.extras
from psycopg2.extensions import connection as PgConnection
from pymongo import MongoClient
from pymongo.database import Database as MongoDatabase
from dotenv import load_dotenv


# ============================================================
# Logging
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ============================================================
# DDL Data Warehouse
# ============================================================
DW_DDL = """
CREATE TABLE IF NOT EXISTS dw_paises (
    id SERIAL PRIMARY KEY,
    pais VARCHAR(100) NOT NULL,
    continente VARCHAR(50),
    region VARCHAR(100),
    capital VARCHAR(100),
    poblacion BIGINT,
    tasa_de_envejecimiento NUMERIC(6,2),
    hospedaje_bajo NUMERIC(10,2),
    hospedaje_promedio NUMERIC(10,2),
    hospedaje_alto NUMERIC(10,2),
    comida_bajo NUMERIC(10,2),
    comida_promedio NUMERIC(10,2),
    comida_alto NUMERIC(10,2),
    transporte_bajo NUMERIC(10,2),
    transporte_promedio NUMERIC(10,2),
    transporte_alto NUMERIC(10,2),
    entretenimiento_bajo NUMERIC(10,2),
    entretenimiento_promedio NUMERIC(10,2),
    entretenimiento_alto NUMERIC(10,2),
    precio_big_mac_usd NUMERIC(10,2),
    indice_costo_total NUMERIC(10,2)
);
"""

INSERT_QUERY = """
INSERT INTO dw_paises (
    pais, continente, region, capital, poblacion,
    tasa_de_envejecimiento,
    hospedaje_bajo, hospedaje_promedio, hospedaje_alto,
    comida_bajo, comida_promedio, comida_alto,
    transporte_bajo, transporte_promedio, transporte_alto,
    entretenimiento_bajo, entretenimiento_promedio, entretenimiento_alto,
    precio_big_mac_usd, indice_costo_total
) VALUES (
    %(pais)s, %(continente)s, %(region)s, %(capital)s, %(poblacion)s,
    %(tasa_de_envejecimiento)s,
    %(hospedaje_bajo)s, %(hospedaje_promedio)s, %(hospedaje_alto)s,
    %(comida_bajo)s, %(comida_promedio)s, %(comida_alto)s,
    %(transporte_bajo)s, %(transporte_promedio)s, %(transporte_alto)s,
    %(entretenimiento_bajo)s, %(entretenimiento_promedio)s, %(entretenimiento_alto)s,
    %(precio_big_mac_usd)s, %(indice_costo_total)s
);
"""


# ============================================================
# 1. Load ENV
# ============================================================
def load_env() -> dict:
    load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

    config = {
        "pg_user": os.getenv("POSTGRES_USER"),
        "pg_password": os.getenv("POSTGRES_PASSWORD"),
        "pg_host": os.getenv("POSTGRES_HOST"),
        "pg_port": int(os.getenv("POSTGRES_PORT", 5432)),
        "pg_src_db": os.getenv("POSTGRES_SOURCE_DB", "lab5"),
        "dw_db": os.getenv("DW_DATABASE_NAME"),
        "mongo_uri": os.getenv("MONGO_URI"),
        "mongo_db": os.getenv("MONGO_DB"),
    }

    missing = [k for k, v in config.items() if v is None]
    if missing:
        log.error("Variables faltantes en .env: %s", missing)
        sys.exit(1)

    return config


# ============================================================
# 2-3. Crear DW si no existe
# ============================================================
def check_or_create_dw(config: dict):
    conn = psycopg2.connect(
        host=config["pg_host"],
        port=config["pg_port"],
        user=config["pg_user"],
        password=config["pg_password"],
        dbname="postgres",
    )
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (config["dw_db"],))
    if not cur.fetchone():
        log.info("Creando base DW...")
        cur.execute(f'CREATE DATABASE "{config["dw_db"]}";')

    cur.close()
    conn.close()


# ============================================================
# 4. Conectar DW
# ============================================================
def connect_dw(config: dict) -> PgConnection:
    return psycopg2.connect(
        host=config["pg_host"],
        port=config["pg_port"],
        user=config["pg_user"],
        password=config["pg_password"],
        dbname=config["dw_db"],
    )


# ============================================================
# 5. Crear tabla DW
# ============================================================
def create_tables(conn_dw: PgConnection):
    with conn_dw.cursor() as cur:
        cur.execute(DW_DDL)
    conn_dw.commit()


# ============================================================
# 6. Extraer SQL
# ============================================================
def extract_sql(config: dict):
    conn = psycopg2.connect(
        host=config["pg_host"],
        port=config["pg_port"],
        user=config["pg_user"],
        password=config["pg_password"],
        dbname=config["pg_src_db"],
    )

    df_p = pd.read_sql("SELECT * FROM pais_poblacion;", conn)
    df_e = pd.read_sql("SELECT * FROM pais_envejecimiento;", conn)

    conn.close()
    return df_p, df_e


# ============================================================
# 7. Extraer Mongo
# ============================================================
def extract_mongo(config: dict):
    client = MongoClient(config["mongo_uri"])
    db = client[config["mongo_db"]]

    df_costos = pd.DataFrame(list(db["costos_turisticos"].find({}, {"_id": 0})))
    df_bigmac = pd.DataFrame(list(db["paises_mundo_big_mac"].find({}, {"_id": 0})))

    return df_costos, df_bigmac, client


# ============================================================
# 8. Limpieza
# ============================================================
def clean_data(df_p, df_e, df_c, df_b):

    df_p["pais"] = df_p["pais"].str.lower().str.strip()
    df_e.rename(columns={"nombre_pais": "pais"}, inplace=True)
    df_e["pais"] = df_e["pais"].str.lower().str.strip()

    df_c.rename(columns={
        "país": "pais",
        "región": "region",
        "población": "poblacion_mongo"
    }, inplace=True)
    df_c["pais"] = df_c["pais"].str.lower().str.strip()

    df_b.rename(columns={"país": "pais"}, inplace=True)
    df_b["pais"] = df_b["pais"].str.lower().str.strip()

    return df_p, df_e, df_c, df_b


# ============================================================
# 9. Integración en memoria
# ============================================================
def integrate(df_p, df_e, df_c, df_b):

    df = df_c.copy()

    # Big Mac
    df = df.merge(df_b[["pais", "precio_big_mac_usd"]], on="pais", how="left")

    # Envejecimiento
    df = df.merge(
        df_e[["pais", "tasa_de_envejecimiento"]],
        on="pais",
        how="left"
    )

    # Población SQL
    df = df.merge(
        df_p[["pais", "poblacion"]].rename(columns={"poblacion": "poblacion_sql"}),
        on="pais",
        how="left"
    )

    df["poblacion"] = df["poblacion_sql"].combine_first(df["poblacion_mongo"])
    df.drop(columns=["poblacion_sql", "poblacion_mongo"], inplace=True)

    # Índice de costo
    cols_prom = [
        "hospedaje_promedio",
        "comida_promedio",
        "transporte_promedio",
        "entretenimiento_promedio",
    ]

    df["indice_costo_total"] = df[cols_prom].mean(axis=1, skipna=True)

    return df.round(2)


# ============================================================
# 10-11. Cargar DW
# ============================================================
def load_dw(conn_dw: PgConnection, df_final: pd.DataFrame):

    with conn_dw.cursor() as cur:
        cur.execute("TRUNCATE TABLE dw_paises RESTART IDENTITY;")
        psycopg2.extras.execute_batch(
            cur,
            INSERT_QUERY,
            df_final.where(pd.notna(df_final), None).to_dict("records"),
        )

    conn_dw.commit()


# ============================================================
# MAIN
# ============================================================
def run():
    config = load_env()
    check_or_create_dw(config)

    conn_dw = connect_dw(config)
    create_tables(conn_dw)

    df_p, df_e = extract_sql(config)
    df_c, df_b, mongo_client = extract_mongo(config)

    df_p, df_e, df_c, df_b = clean_data(df_p, df_e, df_c, df_b)
    df_final = integrate(df_p, df_e, df_c, df_b)
    df_final.to_csv("./ejercicio2/dw_paises.csv", index=False)

    load_dw(conn_dw, df_final)

    conn_dw.close()
    mongo_client.close()

    log.info("Pipeline finalizado correctamente.")


if __name__ == "__main__":
    run()