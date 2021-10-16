import dash
import dash_bootstrap_components as dbc
from dash import dcc
from dash import html
from dash import dash_table
from dash_bootstrap_templates import load_figure_template
from get_gap import get_gap_bnc_df,get_gap_ftx_df,get_gap_okex_df,merge_table
from dash.dependencies import Input, Output
from dash.exceptions import PreventUpdate

load_figure_template("lumen")

app = dash.Dash(external_stylesheets=[dbc.themes.LUMEN],suppress_callback_exceptions = True,)




app.layout = html.Div([
    dbc.Row([html.Div([dcc.Interval(id='interval-component',interval=2.5*1000*60,n_intervals=0),
                       # dbc.Button("Merge 새로고침", color="primary", className="mr-1",id='refresh_merge'),
                       # dbc.Button("Binance 새로고침", color="primary", className="mr-1",id='refresh_bnc'),
                       # dbc.Button("FTX 새로고침", color="primary", className="mr-1",id='refresh_ftx'),
                       # dbc.Button("OKEX 새로고침", color="primary", className="mr-1",id='refresh_okex'),
                       html.H2('Spot-Short Kimp Gap'),
                       html.Div([html.Div(id='merge_table')],style={'margin-top': '40px'}),
                       html.Div([dbc.Spinner(html.Div(id='binance_table'))],style={'margin-top': '40px'}),
                       html.Div([dbc.Spinner(html.Div(id='ftx_table'))],style={'margin-top': '40px'}),
                       html.Div([dbc.Spinner(html.Div(id='okex_table'))],style={'margin-top': '40px'}),

                       ]),
             ],),
    # dbc.Row([]),
    # dbc.Row([]),
    ],
    style={ 'display': 'flex', 'align-items': 'center', 'justify-content': 'center'}
)

@app.callback(
    Output('binance_table','children'),
    [Input('refresh_bnc','n_clicks')]
)
def refresh_binance(n):
    if n is None:
        PreventUpdate
    else:
        gap_df = get_gap_bnc_df()
        gap_df=gap_df.sort_values(by=['gap'], axis=0, ascending=True)
        table1 = dash_table.DataTable(
            # id='datatable-interactivity',
            id={'type': 'datatable-interactivity', 'index': 1},
            columns=[{"name": i, "id": i} for i in gap_df.columns],
            data=gap_df.to_dict('records'),
            fixed_rows={'headers': True},
            sort_action="native",
            style_table={
                'height': 500,
                'width':1000,
                'overflowY': 'scroll'
            },
            style_as_list_view=True,
            style_cell={'padding': '5px', 'textAlign': 'left', 'font-family': 'Roboto'},
            style_header={
                'backgroundColor': 'white',
                'fontWeight': 'bold'
            },
            # row_selectable="single",
            css=[
                {'selector': '.dash-spreadsheet-menu', 'rule': 'position:absolute;top:-60px;right:-9px;'},
                {'selector': '.show-hide-menu', 'rule': 'left:50px;'},
                {'selector': '.show-hide','rule': 'display: none'},
                ],
        )

        return table1

@app.callback(
    Output('ftx_table','children'),
    [Input('refresh_ftx','n_clicks')]
)
def refresh_ftx(n):
    if n is None:
        PreventUpdate
    else:
        gap_df = get_gap_ftx_df()
        gap_df=gap_df.sort_values(by=['gap'], axis=0, ascending=True)
        new_col=['symbol', 'limit', 'ftx_size', 'upbit_size',
                 'ftx_avg_price','upbit_avg_price', 'kimp_by_avg', 'gap']
        gap_df.columns=new_col
        table1 = dash_table.DataTable(
            # id='datatable-interactivity',
            id={'type': 'datatable-interactivity', 'index': 1},
            columns=[{"name": i, "id": i} for i in new_col],
            data=gap_df.to_dict('records'),
            fixed_rows={'headers': True},
            sort_action="native",
            style_table={
                'height': 500,
                'width':1000,
                'overflowY': 'scroll'
            },
            style_as_list_view=True,
            style_cell={'padding': '5px', 'textAlign': 'left', 'font-family': 'Roboto'},
            style_header={
                'backgroundColor': 'white',
                'fontWeight': 'bold'
            },
            # row_selectable="single",
            css=[
                {'selector': '.dash-spreadsheet-menu', 'rule': 'position:absolute;top:-60px;right:-9px;'},
                {'selector': '.show-hide-menu', 'rule': 'left:50px;'},
                {'selector': '.show-hide','rule': 'display: none'},
                ],
        )

        return table1

@app.callback(
    Output('okex_table','children'),
    [Input('refresh_okex','n_clicks')]
)
def refresh_ftx(n):
    if n is None:
        PreventUpdate
    else:
        gap_df = get_gap_okex_df()
        gap_df=gap_df.sort_values(by=['gap'], axis=0, ascending=True)
        new_col=['symbol', 'limit', 'okex_size', 'upbit_size',
                 'okex_avg_price','upbit_avg_price', 'kimp_by_avg', 'gap']
        gap_df.columns=new_col
        table1 = dash_table.DataTable(
            # id='datatable-interactivity',
            id={'type': 'datatable-interactivity', 'index': 1},
            columns=[{"name": i, "id": i} for i in new_col],
            data=gap_df.to_dict('records'),
            fixed_rows={'headers': True},
            sort_action="native",
            style_table={
                'height': 500,
                'width':1000,
                'overflowY': 'scroll'
            },
            style_as_list_view=True,
            style_cell={'padding': '5px', 'textAlign': 'left', 'font-family': 'Roboto'},
            style_header={
                'backgroundColor': 'white',
                'fontWeight': 'bold'
            },
            # row_selectable="single",
            css=[
                {'selector': '.dash-spreadsheet-menu', 'rule': 'position:absolute;top:-60px;right:-9px;'},
                {'selector': '.show-hide-menu', 'rule': 'left:50px;'},
                {'selector': '.show-hide','rule': 'display: none'},
                ],
        )

        return table1

@app.callback(
    Output('merge_table','children'),
    [Input('interval-component','n_intervals')]
)
def refresh_merge(n):
    print('refresh')
    gap_df = merge_table()
    gap_df=gap_df.sort_values(by=['gap'], axis=0, ascending=True)

    table1 = dash_table.DataTable(
        # id='datatable-interactivity',
        id={'type': 'datatable-interactivity', 'index': 1},
        columns=[{"name": i, "id": i} for i in gap_df],
        data=gap_df.to_dict('records'),
        fixed_rows={'headers': True},
        sort_action="native",
        style_table={
            'height': 500,
            'width':1000,
            'overflowY': 'scroll'
        },
        style_as_list_view=True,
        style_cell={'padding': '5px', 'textAlign': 'left', 'font-family': 'Roboto'},
        style_header={
            'backgroundColor': 'white',
            'fontWeight': 'bold'
        },
        # row_selectable="single",
        css=[
            {'selector': '.dash-spreadsheet-menu', 'rule': 'position:absolute;top:-60px;right:-9px;'},
            {'selector': '.show-hide-menu', 'rule': 'left:50px;'},
            {'selector': '.show-hide','rule': 'display: none'},
            ],
    )

    return table1

if __name__ == "__main__":
    app.run_server(debug=True, port=7777)