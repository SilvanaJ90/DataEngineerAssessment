#!/usr/bin/env python3
"""
   The month-on-month evolution of the average ABA
   (Assets Under Management) of the total portfolio
"""

import pandas as pd
from sqlalchemy import create_engine, text


DATABASE_URL = 'postgresql+psycopg2://fintech:pass123@localhost:5432/fintech'
engine = create_engine(DATABASE_URL)


query_4 = """
SELECT
    ingestion_year,
    ingestion_month,
    AVG(aba) AS promedio_aba
FROM historico_aba_macroactivos
WHERE (ingestion_year * 100 + ingestion_month) BETWEEN 202311 AND 202405
GROUP BY ingestion_year, ingestion_month
ORDER BY ingestion_year, ingestion_month;
"""
df = pd.read_sql(query_4, engine)

#  Adjust the display options for pandas
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', None)

# Display the DataFrame
print(df)
