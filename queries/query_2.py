#!/usr/bin/env python3
"""
    The portfolio by bank and the percentage of
    each macro-asset as of the latest available date.
"""

import pandas as pd
from sqlalchemy import create_engine, text


DATABASE_URL = 'postgresql+psycopg2://fintech:pass123@localhost:5432/fintech'
engine = create_engine(DATABASE_URL)

query_2 = """
 WITH latest_data AS (
    SELECT
        cod_banca,
        macroactivo,
        aba,
        RANK() OVER (
            PARTITION BY cod_banca ORDER BY ingestion_year DESC,
            ingestion_month DESC, ingestion_day DESC) AS rank
    FROM historico_aba_macroactivos
)
SELECT
    ld.cod_banca,
    cb.banca,
    ld.macroactivo,
    SUM(ld.aba) AS total_aba,
    SUM(ld.aba) * 100.0 / SUM(SUM(ld.aba))
    OVER (PARTITION BY ld.cod_banca) AS porcentaje_macroactivo
FROM latest_data ld
JOIN catalogo_banca cb ON ld.cod_banca = cb.cod_banca
WHERE ld.rank = 1
GROUP BY ld.cod_banca, cb.banca, ld.macroactivo;
"""
df = pd.read_sql(query_2, engine)

#  Adjust the display options for pandas
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', None)

# Display the DataFrame
print(df)
