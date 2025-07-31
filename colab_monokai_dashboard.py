
import pandas as pd
import plotly.express as px
from dash import Dash, dcc, html, Input, Output
import dash_bootstrap_components as dbc
import gdown
import os

cookie_path = os.path.expanduser("~/.cache/gdown/cookies.txt")
if os.path.exists(cookie_path):
    os.remove(cookie_path)
    print("✅ Cookie file removed")

silver_url = "https://drive.google.com/uc?id=1uKett14AoOlL80UbOVumf91sp9RoCWs5"
bronze_url = "https://drive.google.com/uc?id=1ZKogyr5MCtr9Xa1W_LiyxkYawRUn5Wux"

#gdown.download(silver_url, "silver_layer.csv", quiet=False)
#gdown.download(bronze_url, "bronze_layer.csv", quiet=False)

silver_df = pd.read_csv("silver_layer.csv", parse_dates=["fecha_consulta", "fecha_de_inicio"])
bronze_df = pd.read_csv("bronze_layer.csv", parse_dates=["start_date", "fecha_actualizacion", "fecha_consulta"])

# Preprocess
silver_df_active = silver_df[silver_df["active"] == True]
top_institutions = silver_df_active.groupby("institucion")["salario"].sum().sort_values(ascending=False).head(10).reset_index(name="total_salary")
avg_salary = silver_df_active.groupby("institucion")["salario"].mean().sort_values(ascending=False).head(10).reset_index()

evolution = bronze_df.set_index('fecha_consulta').groupby('institucion').resample('1W').total_salary.sum().replace(0,None).bfill().reset_index()

# Monokai color scheme
colors = {
    "background": "#272822",
    "text": "#F8F8F2",
    "accent": "#66D9EF",
    "plot_bg": "#1E1F1C"
}

app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

app.layout = dbc.Container([
    html.H1("Panama Comptroller Dashboard", style={"color": colors["accent"], "textAlign": "center"}),

    dbc.Row([
        dbc.Col(dcc.Graph(
            figure=px.bar(top_institutions, x="total_salary", y="institucion", orientation="h", title="Top 10 Institutions by Total Salary")
            .update_layout(paper_bgcolor=colors["background"], plot_bgcolor=colors["plot_bg"], font_color=colors["text"])
        ), md=6),

        dbc.Col(dcc.Graph(
            figure=px.bar(avg_salary, x="salario", y="institucion", orientation="h", title="Top Institutions by Average Salary")
            .update_layout(paper_bgcolor=colors["background"], plot_bgcolor=colors["plot_bg"], font_color=colors["text"])
        ), md=6)
    ]),

    dbc.Row([
        dbc.Col(dcc.Graph(
            figure=px.area(evolution, x="fecha_consulta", y="total_salary",color='institucion', title="Salary Evolution")
            .update_layout(paper_bgcolor=colors["background"], plot_bgcolor=colors["plot_bg"], font_color=colors["text"])
        ), md=12),

    ])
], fluid=True, style={"backgroundColor": colors["background"], "padding": "20px"})

if __name__ == "__main__":
    app.run(debug=True, port=8050)
