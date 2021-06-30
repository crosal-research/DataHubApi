################################################################################
# fetches data from cepea database
# and adds to the economic database
#
# Obs:
# for clarification in regards to the olefile module check
# this stackoverflow thread: https://stackoverflow.com/questions/
# 58336366/compdocerror-when-importing-xls-file-format-to-python-using-pandas-read-excel
################################################################################

# import form the system
from concurrent.futures import ThreadPoolExecutor as executor
import time
from typing import Optional

# import from packages
import requests
import olefile
import pandas as pd
import pendulum

#app imports
from DB.transactions import add_obs


__all__ = ["fetch", "tickers"]

t1 = time.time()


info = {"77":"milho",
        "53": "acucar",
        "111": "etanol-diario-paulinia", 
        "2":"boi-gordo"}

# tickers = [f"cepea.{n}" for _, n in info]


def build_url(ticker) -> str:
    """builds the relevant url for the security in case

    """
    number = ticker.split(".")[1]
    return f'https://www.cepea.esalq.usp.br/br/indicador/series/{info[number]}.aspx?id={number}'


def process(resp:requests.models.Response, limit=None) -> pd.DataFrame:
    """
    fetchs the series related to the url used as input. Return a 
    dataframe
    """
    if resp.ok:
        try:
            ole = olefile.OleFileIO(resp.content)
            df = pd.read_excel(ole.openstream("Workbook")).iloc[3:, [0,1]].dropna()
            df.columns = ["data", "value"]
            df.set_index(["data"], inplace=True)
            df.index = pd.to_datetime(df.index)
            return df.applymap(lambda v: float(v)) if limit is None else df.applymap(lambda v: float(v)).tail(limit)
        except:
            print(f"Couldn't not process response form {resp.url}")
    else:
        print("Could not reaches {resp.url}")


def fetch(tickers: list, limit: Optional[int]) -> None:
    """Upserts data, fetching from sourcing and adding into the database.
    tickers defines which series should be upserted, and limit are the 
    tail observations to be inserted. If limit is None, all
    observations are inserted
    """
    urls = [build_url(tck) for tck in tickers]
    with requests.session() as session:
        with executor() as e:
            dfs = list(e.map(lambda url:process(session.get(url), limit=limit), 
                             list(urls)))

    for tck, df in zip(tickers, dfs):
        for ind in df.index:
            add_obs(tck, ind.to_pydatetime(), df.loc[ind, "value"])
    
    print("################################################################")
    print(f"series from cepea added to the database:{time.time()-t1}")    

##############################Main##############################

