# import from system
from concurrent.futures import ThreadPoolExecutor as executor
import re
import json, time

# import from packges
import requests
import pandas as pd
import numpy as np
import pendulum

#import from app
from DB.transactions import add_batch_obs

def _build_url(tck:str, limit=None) -> str:
    tck_new = tck.split(".")[1]
    if not limit:
        return f"http://api.sidra.ibge.gov.br/values/t/{tck_new}/n1/1/f/a"
    return f"http://api.sidra.ibge.gov.br/values/t/{tck_new}/p/last {limit}/n1/1/f/a"

def _process(resp: requests.models.Response):
    try:
        table = (re.compile("\d+")).findall(resp.url)[0]
        df = pd.DataFrame(json.loads((resp.content).decode())[1:]).loc[:,["D1C", "V"]].set_index("D1C")
        df.index = pd.to_datetime(df.index, format="%Y%m")
        periods = len(np.unique(df.index.month))
        if table in ["1620", "1621", "1846"]:
            di = pd.period_range(df.index[0], periods=len(df), freq='Q')
            df.index = di.to_timestamp()
        else:
            di = pd.period_range(df.index[0], periods=len(df), freq='M')
            df.index = di.to_timestamp()
        df.index.name = 'date'
        df = df.replace(to_replace="^[\.|-]", regex=True, value=pd.NA).dropna()
        return df.applymap(lambda v: float(v))
    except:
        print(resp.url)


def fetch(tickers:list, limit=None) -> None:
    t1 = time.time()
    urls = [_build_url(tck) for tck in tickers]
    with requests.session() as session:
        with executor() as e:
            dfs = list(e.map(lambda url: _process(session.get(url)), urls))

    print("###################################################")
    print(f"Done downloading ibge data: {time.time() - t1} seconds")

    def _upsert_obs(z):
        try:
            add_batch_obs(*z)
            print(f"{z[0]} added to the database")
        except:
            print(f"{z[0]} resulted in a empty dataframe")
    # for dz in zip(tickers, dfs):
    #     try:
    #         add_batch_obs(*dz)
    #         print(f"{dz[0]} added to the database")
    #     except:
    #         print(f"{dz[0]} resulted in a empty dataframe")
    [_upsert_obs(dz) for dz in zip(tickers, dfs)]
    print(f"Done updating ibge data: {time.time() - t1} seconds")
##############################MAIN##############################
