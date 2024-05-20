#!/usr/bin/env python3
"""
    The portfolio by risk profile and the percentage of
    each macro-asset as of the latest available date.
"""

import pandas as pd
from sqlalchemy import create_engine, text


DATABASE_URL = 'postgresql+psycopg2://fintech:pass123@localhost:5432/fintech'
engine = create_engine(DATABASE_URL)


query_3 = """
WITH latest_data AS (
    SELECT
        cod_perfil_riesgo,
        macroactivo,
        aba,
        RANK() OVER (
            PARTITION BY cod_perfil_riesgo ORDER BY ingestion_year DESC,
            ingestion_month DESC, ingestion_day DESC) AS rank
    FROM historico_aba_macroactivos
)
SELECT
    ld.cod_perfil_riesgo,
    cpr.perfil_riesgo,
    ld.macroactivo,
    SUM(ld.aba) AS total_aba,
    SUM(ld.aba) * 100.0 / SUM(SUM(ld.aba))
    OVER (PARTITION BY ld.cod_perfil_riesgo) AS porcentaje_macroactivo
FROM latest_data ld
JOIN cat_perfil_riesgo cpr ON ld.cod_perfil_riesgo = cpr.cod_perfil_riesgo
WHERE ld.rank = 1
GROUP BY ld.cod_perfil_riesgo, cpr.perfil_riesgo, ld.macroactivo;
"""
df = pd.read_sql(query_3, engine)

#  Adjust the display options for pandas
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', None)

# Display the DataFrame
print(df)
