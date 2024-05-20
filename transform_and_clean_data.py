#!/usr/bin/env python3
""" Script to transform and clean data """

import pandas as pd
import os

# Path to the folder containing CSV files
data_folder = 'data'

# Name of the file from which you want to remove columns
file_to_process = 'historico_aba_macroactivos.csv'

# Specifies the columns to delete
columns_to_drop = ['year', 'month']

def transform_and_clean_data(data_folder, file_to_process, columns_to_drop):
    # Iterate over all files in the path
    for file_name in os.listdir(data_folder):
        # Constructs the full path to the file
        file_path = os.path.join(data_folder, file_name)

        # Verify if the file is the one you want to process
        if file_name == file_to_process:
            # Reads the CSV file and stores the data in a DataFrame
            df = pd.read_csv(file_path)

            # Deletes the specified columns
            df.drop(columns=columns_to_drop, inplace=True)

            # Prints a confirmation message
            print(f"Columnas eliminadas del archivo '{file_name}': {columns_to_drop}")

            # Check for NaN values in each column
            nan_columns = df.columns[df.isna().any()].tolist()

            # Print columns with NaN values
            print("Columns with NaN values:", nan_columns)

            # Specify the columns with NaN values
            columns_with_nan = ['ingestion_month', 'id_sistema_cliente', 'macroactivo',
                                'cod_activo', 'aba', 'cod_perfil_riesgo', 'cod_banca']

            # Drop rows with NaN values in the specified columns
            df.dropna(subset=columns_with_nan, inplace=True)

            return df

# Call the function
df = transform_and_clean_data(data_folder, file_to_process, columns_to_drop)
