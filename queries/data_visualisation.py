#!/usr/bin/env python3
"""
    Data Visualisation
"""

import pandas as pd
from sqlalchemy import create_engine, text
import dash
from dash import dcc
from dash import html
from dash.dependencies import Input, Output
import plotly.express as px

# Import queries
from query_1 import query_1
from query_2 import query_2
from query_3 import query_3
from query_4 import query_4

DATABASE_URL = 'postgresql+psycopg2://fintech:pass123@localhost:5432/fintech'
engine = create_engine(DATABASE_URL)

# Get data from database for query_2
df_query_1 = pd.read_sql(query_1, engine)

# Get data from database for query_2
df_query_2 = pd.read_sql(query_2, engine)

# Get data from database for query_3
df_query_3 = pd.read_sql(query_3, engine)

# Get data from database for query_4
df_query_4 = pd.read_sql(query_4, engine)

# Create a date column for better visualisation
df_query_4['date'] = pd.to_datetime(df_query_4['ingestion_year'].astype(str) + df_query_4['ingestion_month'].astype(str), format='%Y%m')


# Create scatter plot for query_1
#fig_query_1 = px.line(df_query_1, x='id_sistema_cliente', y='porcentaje_activo', color='macroactivo', title='Evolución del Porcentaje de Activos por Cliente')

fig_query_1  = px.scatter(df_query_1, x='id_sistema_cliente', y='porcentaje_activo', color='macroactivo', title='Porcentaje de Activos por Cliente y Tipo de Macroactivo', labels={'id_sistema_cliente': 'ID del Cliente', 'porcentaje_activo': 'Porcentaje de Activos'})

# Create stacked bar chart for query_2
fig_query_2 = px.bar(df_query_2, x="banca", y="porcentaje_macroactivo", color="macroactivo", title="Portfolio Composition by Bank and Macro-Asset",
                              labels={'banca': 'Bank', 'porcentaje_macroactivo': 'Percentage of Macro-Asset', 'macroactivo': 'Macro-Asset'})


# Create histogram for query_3
fig_query_3 = px.histogram(df_query_3, x="macroactivo", y="porcentaje_macroactivo", color="perfil_riesgo", title="Distribution of the percentage of each macro-asset by risk profile")

# Create box plot for query_4
fig_query_4 = px.box(df_query_4, x='date', y='promedio_aba', title='Box Plot de la evolución mensual del promedio de ABA', labels={'date': 'Fecha', 'promedio_aba': 'Promedio ABA'})

    
# Create the Dash application
app = dash.Dash(__name__)

# Design of the application
app.layout = html.Div([
    html.H1("Financial Data Analysis"),

    # plot for query_1
    html.Div([
        html.H2("Distribución de Porcentajes de Activos"),
        dcc.Graph(figure=fig_query_1)
    ]),

    # Bar plot for query_2
    html.Div([
        html.H2("Portfolio by bank and percentage of each macro-asset"),
        dcc.Graph(figure=fig_query_2)
    ]),
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
    app.run_server(debug=True, port=8051) 
