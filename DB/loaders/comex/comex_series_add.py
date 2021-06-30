# import from system
from datetime import datetime as dt
from io import StringIO
# import from packages
import pandas as pd
import requests

# import from app
from DB.transactions import add_series


series = {'EXP_DESSAZONALIZADA_QUANTUM': "Index de volume das exportações com ajuste sazonal. Brasil" , 
          'EXP_PRECO': "Index de volume das exportações com ajuste sazonal. Brasil", 
          'EXP_QUANTUM': "Index de volume das exportações com ajuste sazonal. Brasil",
          'IMP_DESSAZONALIZADA_QUANTUM': "Index de volume das exportações com ajuste sazonal. Brasil", 
          'IMP_PRECO': "Index de volume das exportações com ajuste sazonal. Brasil", 
          'IMP_QUANTUM': "Index de volume das exportações com ajuste sazonal. Brasil",
          'TERMOS_DE_TROCA_PRECO':"Index de volume das exportações com ajuste sazonal. Brasil"}

URL = "https://balanca.economia.gov.br/balanca/IPQ/arquivos/Dados_totais_mensal.csv"


def fetch_series(url:str):
    resp = requests.get(url, verify=False)
    if resp.ok:
        df = pd.read_csv(StringIO(resp.text), delimiter=";", decimal=",")
    else:
        print("Data not available")
    
    df["dates"] = df.loc[:, ["CO_ANO", "CO_MES"]].apply(lambda x: dt(x[0], x[1], 1), axis=1)
    df["series"] = df.loc[:, ["TIPO", "TIPO_INDICE"]].apply(lambda x: f"{x[0]}_{x[1]}", axis=1)
    df_final = df.pivot(index="dates", columns="series", values="INDICE")
    return [(f"COMEX.{col}", series[col], "COMEX", "ECON", "SERIES-TEMPORAIS") 
            for col in df_final.columns]


def ingest_series(url: str):
    srs = fetch_series(url)
    [add_series(*s) for s in srs]


if __name__ == "__main__":
    ingest_series(URL)
