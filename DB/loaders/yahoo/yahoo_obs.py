#import from system
from typing import List, Optional
from concurrent.futures import ThreadPoolExecutor as executor
from io import StringIO

# import from packages
import requests
import pandas as pd
import pendulum 


__all__ = ["fetch"]

list_quote:List[str] = [
    "yahoo.ES=F", 
    "yahoo.YM=F", 
    "yahoo.NQ=F", 
    "yahoo.RTY=F", 
    "yahoo.GC=F", 
    "yahoo.SI=F", 
    "yahoo.PL=F", 
    "yahoo.HG=F", 
    "yahoo.PA=F", 
    "yahoo.CL=F", 
    "yahoo.ZC=F", 
    "yahoo.ZO=F", 
    "yahoo.KE=F", 
    "yahoo.ZR=F", 
    "yahoo.ZS=F", 
    "yahoo.GF=F", 
    "yahoo.HE=F", 
    "yahoo.LE=F", 
    "yahoo.CC=F", 
    "yahoo.KC=F", 
    "yahoo.CT=F", 
    "yahoo.SB=F"]


props = ['Open', 'High', 'Low', 'Close', 'Adj_Close', 'Volume']

def fetch(tickers:List[str], 
                limit:Optional[int]=None)-> List[pd.DataFrame]: 
    """ For security 'quote' fetches daily: Open, High, Low, Close, Adj
    Close and volume, by return a data frame """
    global cols, dfs
    de = pendulum.today()
    di = de.subtract(days=limit) if limit else None
    params = {"period1": int(di.timestamp()) if di else None,
              "period2": int(de.timestamp()),
              "interval": "1d",
              "events": "history",
              "includeAdjustedClose": True}
    
    quotes = [tck.split(".")[1] for tck in tickers]
    urls = [f"https://query1.finance.yahoo.com/v7/finance/download/{q}"
            for q in quotes]

    with requests.session() as session:
        with executor()  as e:
            rs = e.map(lambda u: session.get(u, params=params), urls)

    dfs = []
    for r, q in zip(rs, quotes):
        cols = [f"yahoo.{q}/{y}" for y in props]
        if r.ok:
            df = pd.read_csv(StringIO(r.text), index_col=[0], parse_dates=True).dropna()
            df.columns = cols
            dfs.append(df)
    [add_batch_obs(df) for df in dfs]
    









