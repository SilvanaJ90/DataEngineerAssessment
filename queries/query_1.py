#!/usr/bin/env python3
"""
    The portfolio of each client and what percentage each macro-asset and asset
    represents in the total portfolio as of the latest available date
"""

import pandas as pd
from sqlalchemy import create_engine, text


DATABASE_URL = 'postgresql+psycopg2://fintech:pass123@localhost:5432/fintech'
engine = create_engine(DATABASE_URL)

query_1 = """
WITH latest_data AS (
    SELECT
        id_sistema_cliente,
        macroactivo,
        cod_activo,
        aba,
        RANK() OVER (
            PARTITION BY id_sistema_cliente ORDER BY ingestion_year DESC,
            ingestion_month DESC, ingestion_day DESC) AS rank
    FROM historico_aba_macroactivos
)
SELECT
    ld.id_sistema_cliente,
    ld.macroactivo,
    ld.cod_activo,
    ld.aba,
    ld.aba * 100.0 / SUM(ld.aba) OVER (
        PARTITION BY ld.id_sistema_cliente) AS porcentaje_macroactivo,
    ld.aba * 100.0 / SUM(ld.aba) OVER (
        PARTITION BY ld.id_sistema_cliente, ld.cod_activo) AS porcentaje_activo
FROM latest_data ld
WHERE ld.rank = 1;
"""
df = pd.read_sql(query_1, engine)

#  Adjust the display options for pandas
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', None)

# Display the DataFrame
print(df)
