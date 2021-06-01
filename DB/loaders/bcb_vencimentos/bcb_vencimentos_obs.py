# import from python system
from concurrent.futures import ThreadPoolExecutor as executor
from typing import Optional

# import from packages
import requests
import pandas as pd
import pendulum

# import from app
from DB.transactions import add_batch_obs
from DB.db import db


__all__ = ['fetch']


url = 'https://www4.bcb.gov.br/pom/demab/cronograma/vencdata_csv.asp?'


def _process(resp:requests.models.Response):
    txt = [l.split(";") for l in (resp.text).split("\n")]
    df = pd.DataFrame(txt).iloc[3:-3, :-1]
    cols = [f"BCB.{col}" for col in df.iloc[0,:].values]
    df.columns = cols
    df.drop(axis=0, labels=[3], inplace=True)
    df.set_index(["BCB.VENCIMENTO"], inplace=True)
    df.index.name = "Date"
    df.index = pd.to_datetime(df.index, format="%d/%m/%Y")
    return df


def fetch(tickers:list, limit:Optional=None):
    resp= requests.get(url)
    df = _process(resp)
    df = df if limit is None else df.tail(limit)
    df.replace(to_replace="^-", regex=True, inplace=True, value=pd.NA)
    
    def _add(tck: str):
        """
        Help function that upserts data on vencimentos,
        first cleaning up and them upserting
        """
        if (series:=db.Series.get(ticker=tck)):
            orm.delete(o for o in db.Observation if o.series == series)
        add_batch_obs(tck, df.loc[: ,[tck]].dropna(axis=0))

    [_add(tck) for tck in tickers]

    


    

    
