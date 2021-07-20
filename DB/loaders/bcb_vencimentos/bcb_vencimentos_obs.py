# import from python system
from concurrent.futures import ThreadPoolExecutor as executor
from typing import Optional

# import from packages
import requests
import pandas as pd
import pendulum
from pony import orm

# import from app
from DB.transactions import add_batch_obs
from DB.db import db


__all__ = ['fetch']


URL = 'https://www4.bcb.gov.br/pom/demab/cronograma/vencdata_csv.asp?'


def _process(resp:requests.models.Response):
    """
    process response from csv-file with information about
    public bonds aggregate maturities
    """
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
    """
    Fetches updated information on public bond's maturity. Prior to
    insert info, erases the old information
    """
    resp= requests.get(URL)
    df = _process(resp)
    df = df if limit is None else df.tail(limit)
    df.replace(to_replace="^-", regex=True, inplace=True, value=pd.NA)
    
    @orm.db_session
    def _add(tck: str):
        """
        Help function that upserts data on vencimentos,
        first cleaning up and them upserting
        """
        if (series:=db.Series.get(ticker=tck)):
            orm.delete(o for o in db.Observation if o.series == series)
        add_batch_obs(tck, df.loc[: ,[tck]].dropna(axis=0))

    [_add(tck) for tck in tickers]

    print("Dados de vencimento do titulos publicos inseridos")
    print("##################################################")
    


    

    
