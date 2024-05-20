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
from query_3 import query_3
from query_4 import query_4

DATABASE_URL = 'postgresql+psycopg2://fintech:pass123@localhost:5432/fintech'
engine = create_engine(DATABASE_URL)

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
    app.run_server(debug=True, port=8051) 
