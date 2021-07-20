# import from python system
from concurrent.futures import ThreadPoolExecutor as executor

# import from packages
import requests
import pandas as pd

# import from app
from DB.transactions import add_series

URL = 'https://www4.bcb.gov.br/pom/demab/cronograma/vencdata_csv.asp?'


def _process(resp:requests.models.Response) -> pd.DataFrame:
    """
    process a request response into a dataframe
    """
    txt = [l.split(";") for l in (resp.text).split("\n")]
    df = pd.DataFrame(txt).iloc[3:-3, :-1]
    cols = df.iloc[0,:].values
    df.drop(axis=0, labels=[3], inplace=True)
    df.columns = cols
    df.set_index(["VENCIMENTO"], inplace=True)
    df.index = pd.to_datetime(df.index, format="%d/%m/%Y")
    return df.applymap(lambda x: 0 if (x=="-") else float(x))


def insert():
    """
    insert the series from vencimentos into the DataBase
    """
    global input
    resp = requests.get(URL)
    df = _process(resp).columns
    for col in df:
        input = (f"BCB.{col}", 
                 f"Vencimento dos Títulos {col} (R$mn)",
                 "BCB", "VENC", "TITULOS-PUBLICOS")
        add_series(*input)


if __name__ == "__main__":
    insert()              


    

    
