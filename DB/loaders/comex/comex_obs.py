# import from system
from datetime import datetime as dt
from typing import List, Optional
from io import StringIO

# import from packages
import pandas as pd
import requests

# import from app
from DB.transactions import add_obs


URL = "https://balanca.economia.gov.br/balanca/IPQ/arquivos/Dados_totais_mensal.csv"


def fetch(tickers: List[str], limit:Optional[int]=None):
    """
    fetches the observations from the comex. Data is obtained from
    spreadsheet as found in URL. If limit different than None, adds all observations
    for tickers list, else only the last n-limit observations
    """
    srs = [tck.split(".")[1] for tck in tickers]
    resp = requests.get(URL, verify=False)
    if resp.ok:
        df = pd.read_csv(StringIO(resp.text), delimiter=";", decimal=",")
    else:
        print("Data not available")
    df["dates"] = df.loc[:, ["CO_ANO", "CO_MES"]].apply(lambda x: dt(x[0], x[1], 1), axis=1)
    df["series"] = df.loc[:, ["TIPO", "TIPO_INDICE"]].apply(lambda x: f"{x[0]}_{x[1]}", axis=1)
    dff = df.pivot(index="dates", columns="series", values="INDICE").loc[:, srs]
    df_final = dff if limit is None else dff.tail(limit)
    for col in df_final:
        for ind in df_final.index:
            add_obs(f"COMEX.{col}", ind, df_final.loc[ind, col])
    print("Observation from COMEX added to the database")
    
