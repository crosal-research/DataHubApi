###################################
# Estudar API do IPEA
# http://www.ipeadata.gov.br/api/
###################################

# import from system
from concurrent.futures import ThreadPoolExecutor as executor
from datetime import datetime as dt
import time
from typing import Optional, List

#import from packages
import requests
import pandas as pd
import pendulum as pl

#app imports
from DB.transactions import add_obs


URL = "http://www.ipeadata.gov.br/api/odata4/"

# series to be fetched from api
tickers = ['ABRAS12_INVR12',
           'ACSP12_SCPCC12',
           'ACSP12_TELCH12',
           'ANDA12_VFERTILIZ12',
           'ANP12_CALCO12',
           'ANP12_CDEPET12',
           'ANP12_CGASOL12',
           'ANP12_CGLP12',
           'ANP12_CODP12',
           'ANP12_COLCOM12',
           'ANP12_COLDIE12',
           'ELETRO12_CEECO12',
           'ELETRO12_CEECOM12',
           'ELETRO12_CEEIND12',
           'ELETRO12_CEENE12',
           'ELETRO12_CEENO12',
           'ELETRO12_CEEOUT12',
           'ELETRO12_CEERES12',
           'ELETRO12_˜CEESE12',
           'ELETRO12_CEESU12',
           'ELETRO12_CEET12',
           'ELETRO12_CEETCOM12', 
           'ELETRO12_CEETIND12',
           'ELETRO12_CEETRES12',
           'ELETRO12_CEETT12',
           'FCESP12_IICA12',
           'FENABRAVE12_VENDAUTO12',
           'FENABRAVE12_VENDVETOT12',
           "GAC12_FBKFCAMI12", 
           'GAC12_FBKFCAMIDESSAZ12',
           "ABPO12_PAPEL12",
           "DIMAC_ECFLIQTOT12",
           "DIMAC_ILIQTOT112",
           "CNI12_ICEICA12",
           "CNI12_ICEIEXP12",
           "CNI12_ICEIGER12",
           "FCESP12_IIC12",
           "FCESP12_IICF12",
           "ABPO12_PAPEL12",
           'CNI12_INDE12', 
           'CNI12_INDEE12', 
           'CNI12_INDEP12', 
           'CNI12_INDP12', 
           'CNI12_INDTE12', 
           'CNI12_INDTP12',
           "BMF366_FUT1DI1366",
           "JPM366_EMBI366"]

def build_url(tck:str) -> str:  # adicionar a possibilidade de usar limit
    """
    builds the url to fetch the data at ipeadatas webpage. 
    """
    return  URL + f"Metadados('{tck}')/Valores"

def process(resp:str, limit:Optional[int]=None) -> pd.DataFrame:
    """
    fetch the data returning a dataframe
    """
    dd = resp.json()['value'][limit:] if limit is not None else resp.json()['value']
    if resp.ok:
        return [(dt.fromisoformat(d['VALDATA'].split("T")[0]), float(d["VALVALOR"])) for d in dd
                if d["VALVALOR"] is not None]
    else:
        print(f"Could not reach {resp.url}")

def fetch(tickers:List[str], limit:Optional[int]=None):
    """
    list of the tickers of ipea's series for which observations
    are update/fetched. If limit is None, fetch all observations,
    else fetches only the last limit observations
    """
    t0 = time.time()
    urls =[build_url(tck.split(".")[1]) for tck in tickers]
    with requests.session() as session:
        with executor() as e:
            ds = list(e.map(lambda u: process(session.get(u), limit), urls))
    for dd, tck in zip(ds, tickers):
        for d in dd:
            add_obs(tck, *d)
    print(f"series from IPEA added to the database: {time.time() -t0}")    

