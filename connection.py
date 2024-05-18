#!/usr/bin/env python3
""" Script to insert records into a PostgreSQL database """

import psycopg2 as pg2
import pandas as pd
import os

# Connection configuration
connection = pg2.connect(
    dbname="fintech",
    user="fintech",
    password="pass123",
    host="localhost",
    port="5432"
)

# Create a cursor
cursor = connection.cursor()

# Path to the folder containing CSV files
data_folder = 'data'


# Function to check and convert data types
def clean_data(df):
    def convert_value(x):
        if pd.isna(x):
            return None
        if isinstance(x, float) and x.is_integer():
            return str(int(x))
        return str(x)

    return df.applymap(convert_value)


# Function to insert data into a table from a CSV file
def insert_data_from_csv(cursor, table_name, csv_file_path):
    # Leer el archivo CSV
    df = pd.read_csv(csv_file_path)
    df = clean_data(df)

    # Generate SQL for data insertion
    columns = ', '.join(df.columns)
    values = ', '.join(['%s' for _ in df.columns])
    insert_sql = f'INSERT INTO {table_name} ({columns}) VALUES ({values})'

    # Insert each row of data
    for idx, row in df.iterrows():
        try:
            cursor.execute(insert_sql, tuple(row))
        except Exception as e:
            print(f"Error en la fila {idx + 2}: {row.to_dict()}")
            raise e


# Specific tables to insert data into in the specified order
tables = ['cat_perfil_riesgo', 'catalogo_banca',
          'catalogo_activos', 'historico_aba_macroactivos']

# Insert data into each specified table
for table in tables:
    csv_file_path = os.path.join(data_folder, f'{table}.csv')
    insert_data_from_csv(cursor, table, csv_file_path)


# Commit the changes
connection.commit()

# Close the cursor and the connection
cursor.close()
connection.close()
