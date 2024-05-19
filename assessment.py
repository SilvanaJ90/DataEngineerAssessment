#!/usr/bin/env python
# coding: utf-8
Technical Test for Investment Analytics Management
# In[283]:


# Importing libraries
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
import os
import dash
from dash import dcc
from dash import html
from dash.dependencies import Input, Output
import plotly.express as px

From Sources
# In[284]:


# Path to the folder containing CSV files
data_folder = 'data'

ETLTransform and clean
# In[285]:


# Name of the file from which you want to remove columns
file_to_process = 'historico_aba_macroactivos.csv'

# Specifies the columns to delete
columns_to_drop = ['year', 'month']

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


# In[286]:


# Check for NaN values in each column
nan_columns = df.columns[df.isna().any()].tolist()

# Print columns with NaN values
print("Columns with NaN values:", nan_columns)


# In[287]:


# Specify the columns with NaN values
columns_with_nan = ['ingestion_month', 'id_sistema_cliente', 'macroactivo', 
                    'cod_activo', 'aba', 'cod_perfil_riesgo', 'cod_banca']

# Drop rows with NaN values in the specified columns
df.dropna(subset=columns_with_nan, inplace=True)

Data WarehouseDatabase Postgres
# In[288]:


# Connection configuration using SQLAlchemy
DATABASE_URL = 'postgresql+psycopg2://fintech:pass123@localhost:5432/fintech'
engine = create_engine(DATABASE_URL)


# In[289]:


# Function to check and convert data types
def clean_data(df):
    def convert_value(x):
        if pd.isna(x):
            return None
        if isinstance(x, float) and x.is_integer():
            return str(int(x))
        return str(x)
    return df.map(convert_value)


# In[290]:


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


# In[291]:


# Create a session
Session = sessionmaker(bind=engine)
session = Session()


# In[292]:


# Specific tables to insert data into in the specified order
tables = ['cat_perfil_riesgo', 'catalogo_banca',
          'catalogo_activos', 'historico_aba_macroactivos']


# In[293]:


# Insert data into each specified table
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

Reports and Dash
# In[338]:


# The portfolio of each client and what percentage each macro-asset and asset represents in the total portfolio as of the latest available date.
query_1 = """
WITH latest_data AS (
    SELECT 
        id_sistema_cliente,
        macroactivo,
        cod_activo,
        aba,
        RANK() OVER (PARTITION BY id_sistema_cliente ORDER BY ingestion_year DESC, ingestion_month DESC, ingestion_day DESC) AS rank
    FROM historico_aba_macroactivos
)
SELECT 
    ld.id_sistema_cliente,
    ld.macroactivo,
    ld.cod_activo,
    ld.aba,
    ld.aba * 100.0 / SUM(ld.aba) OVER (PARTITION BY ld.id_sistema_cliente) AS porcentaje_macroactivo,
    ld.aba * 100.0 / SUM(ld.aba) OVER (PARTITION BY ld.id_sistema_cliente, ld.cod_activo) AS porcentaje_activo
FROM latest_data ld
WHERE ld.rank = 1;
"""
df = pd.read_sql(query_1, engine)

#  Adjust the display options for pandas
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.width', 1000)        # Adjust display width to fit all columns
pd.set_option('display.max_colwidth', None) # Show full column content

# Display the DataFrame
print(df)


# In[296]:


# The portfolio by bank and the percentage of each macro-asset as of the latest available date.
query_2 = """
 WITH latest_data AS (
    SELECT 
        cod_banca,
        macroactivo,
        aba,
        RANK() OVER (PARTITION BY cod_banca ORDER BY ingestion_year DESC, ingestion_month DESC, ingestion_day DESC) AS rank
    FROM historico_aba_macroactivos
)
SELECT 
    ld.cod_banca,
    cb.banca,
    ld.macroactivo,
    SUM(ld.aba) AS total_aba,
    SUM(ld.aba) * 100.0 / SUM(SUM(ld.aba)) OVER (PARTITION BY ld.cod_banca) AS porcentaje_macroactivo
FROM latest_data ld
JOIN catalogo_banca cb ON ld.cod_banca = cb.cod_banca
WHERE ld.rank = 1
GROUP BY ld.cod_banca, cb.banca, ld.macroactivo;
"""
df = pd.read_sql(query_2, engine)

#  Adjust the display options for pandas
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.width', 1000)        # Adjust display width to fit all columns
pd.set_option('display.max_colwidth', None) # Show full column content

# Display the DataFrame
print(df)

The portfolio by risk profile and the percentage of each macro-asset as of the latest available date.
# In[318]:


# The portfolio by risk profile and the percentage of each macro-asset as of the latest available date.
query_3 = """
WITH latest_data AS (
    SELECT 
        cod_perfil_riesgo,
        macroactivo,
        aba,
        RANK() OVER (PARTITION BY cod_perfil_riesgo ORDER BY ingestion_year DESC, ingestion_month DESC, ingestion_day DESC) AS rank
    FROM historico_aba_macroactivos
)
SELECT 
    ld.cod_perfil_riesgo,
    cpr.perfil_riesgo,
    ld.macroactivo,
    SUM(ld.aba) AS total_aba,
    SUM(ld.aba) * 100.0 / SUM(SUM(ld.aba)) OVER (PARTITION BY ld.cod_perfil_riesgo) AS porcentaje_macroactivo
FROM latest_data ld
JOIN cat_perfil_riesgo cpr ON ld.cod_perfil_riesgo = cpr.cod_perfil_riesgo
WHERE ld.rank = 1
GROUP BY ld.cod_perfil_riesgo, cpr.perfil_riesgo, ld.macroactivo;
"""
df = pd.read_sql(query_3, engine)

#  Adjust the display options for pandas
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.width', 1000)        # Adjust display width to fit all columns
pd.set_option('display.max_colwidth', None) # Show full column content

# Display the DataFrame
print(df)

The month-on-month evolution of the average ABA (Assets Under Management) of the total portfolio
# In[341]:


# The month-on-month evolution of the average ABA (Assets Under Management) of the total portfolio
query_4 = """
SELECT 
    ingestion_year,
    ingestion_month,
    AVG(aba) AS promedio_aba
FROM historico_aba_macroactivos
WHERE (ingestion_year * 100 + ingestion_month) BETWEEN 202311 AND 202412
GROUP BY ingestion_year, ingestion_month
ORDER BY ingestion_year, ingestion_month;
"""
df = pd.read_sql(query_4, engine)

#  Adjust the display options for pandas
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.width', 1000)        # Adjust display width to fit all columns
pd.set_option('display.max_colwidth', None) # Show full column content

# Display the DataFrame
print(df)

Data visualisation
# In[348]:


# Get data from database for query_3
df_query_3 = pd.read_sql(query_3, engine)

# Get data from database for query_4
df_query_4 = pd.read_sql(query_4, engine)
# Create a date column for better visualisation
df_query_4['date'] = pd.to_datetime(df_query_4['ingestion_year'].astype(str) + df_query_4['ingestion_month'].astype(str), format='%Y%m')

# Create histogram for query_3
fig_query_3 = px.histogram(df_query_3, x="macroactivo", y="porcentaje_macroactivo", color="perfil_riesgo", title="Distribution of the percentage of each macro-asset by risk profile")

# Create box plot for query_4
fig_query_4 = px.box(df_query_4, x='date', y='promedio_aba', title='Box Plot de la evolución mensual del promedio de ABA', labels={'date': 'Fecha', 'promedio_aba': 'Promedio ABA'})

    
# Create the Dash application
app = dash.Dash(__name__)

# Design of the application
app.layout = html.Div([
    html.H1("Financial Data Analysis"),

    # Graph for query_3
    html.Div([
        html.H2("Distribution of the percentage of each macro-asset by risk profile"),
        dcc.Graph(figure=fig_query_3)
    ]),

    # Gráfico para query_4
    html.Div([
        html.H2("Monthly evolution of the ABA average"),
        dcc.Graph(figure=fig_query_4)
    ])
])

if __name__ == '__main__':
    app.run_server(debug=True)

Analysis

Distribution of the percentage of each macro-asset by risk profile

Asset Diversification by Risk Profile: There is asset diversification observed for each risk profile. For instance, for the "UNDEFINED" profile (1466), there is a distribution between FICs and Equities, indicating a mixed or diversified investment strategy.

Conservative vs. Aggressive Investment Strategies: There is a difference in asset composition between conservative and aggressive risk profiles. For example, for the "CONSERVATIVE" profile (1467), all assets are allocated to FICs, suggesting a more conservative investment strategy. In contrast, for the "AGGRESSIVE" profile (1469), all assets are allocated to Equities, indicating a more aggressive strategy.

Moderate Profile with Balanced Diversification: For the "MODERATE" profile (1468), there is a more balanced distribution between FICs, Fixed Income, and Equities. This suggests a moderate investment strategy seeking a balance between safety and potential returns.

Impact on Investment Decision-Making: Investors and portfolio managers can use this information to better understand how assets are distributed across different risk profiles and adjust their investment strategies accordingly. For example, they could opt for a more conservative or aggressive asset allocation based on their risk tolerance and investment objectives.



Evolución mensual del promedio de ABA

Continuous Growth: There is a gradual growth observed in the average ABA over the recorded months. This suggests a steady increase in the amount of assets managed by the financial institution during this period of time.

Year Change: There is a significant change between December 2023 and January 2024, with a substantial increase in the average ABA. This change could be attributable to factors such as the start of a new fiscal year or changes in clients' investment strategies.

Stability in 2024: After the initial increase in January 2024, the average ABA shows some stability, with minor fluctuations in the following months. This stability could indicate efficient asset management and successful client retention.

Upward Trend: Despite minor fluctuations, the overall trend of the average ABA for the months of 2024 is upward. This suggests sustained growth in the base of assets managed by the financial institution during this period.

Technical Conclusions: