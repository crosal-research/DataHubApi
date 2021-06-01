# import system
from concurrent.futures import ThreadPoolExecutor as executor
from typing import Optional
from datetime import datetime as dt
import time, json

# imports packages
import requests
import pandas as pd
import pendulum

# imports from app
from DB.transactions import add_batch_obs


__all__ = ["fetch"]

with open("./configuration.json") as fp:
    config = json.load(fp)



def build_fred(key, ticker, limit: Optional[int]=None):

    if not limit:
        return f"https://api.stlouisfed.org/fred/series/observations?" + \
            f"series_id={ticker}&api_key={key}&file_type=json"
    else:
        return f"https://api.stlouisfed.org/fred/series/observations?" + \
            f"series_id={ticker}&api_key={key}&file_type=json" + \
            "&limit=10&sort_order=desc"

def process(resp):
    dj = resp.json()["observations"]
    df = pd.DataFrame(dj).iloc[:, [2,3]].set_index(["date"])
    df.index = [dt.strptime(i, "%Y-%m-%d") for i in df.index]
    return (df.applymap(lambda v: float(v) if v != "." else None)).sort_index()


def fetch(tickers:str, limit: Optional[int] = None):
    key = config['ApiKeys']['fred']
    urls =[build_fred(key, tck.split(".")[1], limit) for tck in tickers]
    with requests.session() as session:
        with executor() as e:
            dfs = list(e.map(lambda url: process(session.get(url)), urls))
    for i,df in enumerate(dfs):
        df.columns = [tickers[i].upper()]

    # async adding of data
    t0 = time.time()
    with executor() as e1:
        def _add(df):
            try:
                add_batch_obs(df.columns[0], df)
                print(f'{df.columns[0]} added')
            except:
                print(f'{df.columns[0]} failed to be added')
        e1.map(_add, dfs)
    print(time.time() - t0)


