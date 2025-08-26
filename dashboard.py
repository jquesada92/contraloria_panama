import pandas as pd 
from dash import Dash, dcc, html, Input, Output, State, MATCH, Patch, callback
import plotly.express as px



lst_inst = pd.read_csv('lista_instituciones.csv').institucion.unique()
bronze_df = pd.read_csv('bronze_layer.csv', parse_dates=["start_date", "fecha_actualizacion", "fecha_consulta"])
agg_institucion_salarios_df = bronze_df.set_index('fecha_consulta').groupby('institucion').resample('1W').total_salary.sum().replace(0,None).bfill().reset_index()
app = Dash(__name__)



app.layout = html.Div([
    html.Button("Add Filter", id="dynamic-add-filter-btn", n_clicks=0),
     dcc.Dropdown(
        options = lst_inst,
         id = 'dropdown-instituciones',
            multi=True,
        ),
    dcc.Graph(id='graph-hist-salaries')
])




@callback(
    Output({'type': 'inst-dynamic-output', 'index': MATCH}, 'children'),
    Input({'type': 'inst-dynamic-dropdown', 'index': MATCH}, 'value'),
    State({'type': 'inst-dynamic-dropdown', 'index': MATCH}, 'id'),
)
def display_output(value, id):
    return html.Div(f"Dropdown {id['index']} = {value}")

@app.callback(
        Output('graph-hist-salaries', 'figure'),
        Input('dropdown-instituciones', 'value')
    )
def update_graph(selected_value:list):
    df = agg_institucion_salarios_df 
    print(selected_value)
    if selected_value:
        df = df.loc[df.institucion.isin(selected_value)]
        
    fig =px.area(df , x="fecha_consulta", y="total_salary",color='institucion', title="Salary Evolution")
    return fig

app.run(debug=True)