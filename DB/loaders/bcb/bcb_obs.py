# imports from system
from concurrent.futures import ThreadPoolExecutor as executor
import json, time
from typing import List, Optional
from datetime import datetime as dt

# import from packages
import requests
import pandas as pd


#import from app
from DB.transactions import add_obs


__all__ = ["fetch", "tickers"]


def build_url(fulltck: str, limit=None) -> str:
    """
    build the url of request to the BCB's api
    """""
    tck= fulltck.split(".")[1].upper()
    tck_new = tck if len(tck) > 2 else "0" + tck
    if not limit:
        return f"http://api.bcb.gov.br/dados/serie/bcdata.sgs.{tck_new}/dados?formato=json"
    return f"http://api.bcb.gov.br/dados/serie/bcdata.sgs.{tck_new}/dados/ultimos/{limit}?formato=json"


def _process(resp:requests.models.Response) -> Optional[dict]:
    """
    handles the successful response to a request the bcb api
    """
    if resp.ok:
        try:
            return resp.json()
        except:
            print(f"Could not process {resp.url}")
    else:
        print("Failed request {resp.url}")
        return None
        

def fetch(tickers: List[str], limit: Optional[int]=None) -> None:
    """
    Fetch the observations from the bcb's api. 
    """
    t1 = time.time()
    urls = (build_url(tck, limit=limit) for tck in tickers)
    with requests.session() as session:
        with executor() as e:
            js = list(e.map(lambda url:_process(session.get(url)), list(urls)))
    print(f"BCB's Data donwloaded: {time.time() - t1}")
    
    def _upsert_obs(tck, j):
        try:
            if j['valor'] != '':
                add_obs(tck, dt.strptime(j['data'], "%d/%m/%Y"), float(j['valor']))
        except ValueError as error:
            print(f"j['data'] and {tck}")
            print(error)

    def _upsert_series(tck, js):
        if js is not None:
            [_upsert_obs(tck, j) for j in js]
        else:
            print(f"{tck} data is empty")

    [_upsert_series(tck, j) for tck, j in zip(tickers, js)]

    print("##############################################")
    print(f"Done updating Observations for BCB: {time.time() - t1} seconds")

##############################MAIN##############################

