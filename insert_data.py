#!/usr/bin/env python3
""" Script to insert records into a PostgreSQL database """

import pandas as pd
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
from sqlalchemy import create_engine, text
import os

DATABASE_URL = 'postgresql+psycopg2://fintech:pass123@localhost:5432/fintech'
engine = create_engine(DATABASE_URL)

# Path to the folder containing CSV files
data_folder = 'data'
columns_to_drop = ['year', 'month']

# Function to check and convert data types
def clean_data(df):
    def convert_value(x):
        if pd.isna(x):
            return None
        if isinstance(x, float) and x.is_integer():
            return str(int(x))
        return str(x)
    return df.map(convert_value)

# Function to insert data into a table from a CSV file
def insert_data_from_csv(session, table_name, csv_file_path):
    # Read the CSV file
    df = pd.read_csv(csv_file_path)
    df = clean_data(df)

    # Drop rows with NaN values
    df.dropna(inplace=True)

    # Remove columns that were dropped previously
    df.drop(columns=columns_to_drop, inplace=True, errors='ignore')

    # Generate SQL for data insertion
    columns = ', '.join(df.columns)
    values = ', '.join([f":{col}" for col in df.columns])
    insert_sql = text(f'INSERT INTO {table_name} ({columns}) VALUES ({values})')

    # Insert each row of data
    for idx, row in df.iterrows():
        try:
            session.execute(insert_sql, row.to_dict())
        except SQLAlchemyError as e:
            print(f"Error in row {idx + 2}: {row.to_dict()}")
            session.rollback()
            raise e

# Create a session
def create_session(engine):
    Session = sessionmaker(bind=engine)
    return Session()

# Insert data into each specified table
def insert_data(engine, data_folder):
    session = create_session(engine)
    tables = ['cat_perfil_riesgo', 'catalogo_banca', 'catalogo_activos', 'historico_aba_macroactivos']

    try:
        for table in tables:
            csv_file_path = os.path.join(data_folder, f'{table}.csv')
            insert_data_from_csv(session, table, csv_file_path)

        # Commit the changes
        session.commit()
    except Exception as e:
        print(f"An error occurred: {e}")
        session.rollback()
    finally:
        # Close the session
        session.close()

    print("Data insertion complete.")

# Call the function
insert_data(engine, data_folder)
