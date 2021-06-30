#############################################
# Data from http://www.ipeadata.gov.br/api/ 
#############################################

# import from system
from concurrent.futures import ThreadPoolExecutor as executor
import time

#import from packages
import requests
import pandas as pd


#app imports
from DB.transactions import add_series


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
           'ELETRO12_CEESE12',
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
    

def build_url(tck:str) -> str:
    """
    builds the url to fetch the metadados of a series 
    from ipeadatas webpage. 
    """
    return URL + f"Metadados('{tck}')"
    

def process(resp:requests.models.Response) -> pd.DataFrame:
    """
    handles a response of a request to the ipea's ipea for
    metadados for a particular seires.
    """
    if resp.ok:
        return resp.json()['value'][0]
    else:
        print(f"Could not reach {resp.url}")


def ingest_series(tickers):
    """
    takes a list of tickers (without prefix IPEA),
    fetches meta information on them from IPEA api and 
    then inserts that information in the datebase
    """
    t0=time.time()
    urls =[build_url(tck) for tck in tickers]
    with requests.session() as session:
        with executor() as e:
            ds = list(e.map(lambda u: process(session.get(u)), urls))
    print(f"Done fetching data im {time.time() - t0} seconds")
    for d, tck in zip(ds, tickers):
        if tck in ["BMF366_FUT1DI1366", "JPM366_EMBI366"]:
            add_series(f"IPEA.{tck}",
                       d["SERNOME"],
                       "IPEA", 
                       "FIN", 
                       "FINANCE")
        else:
            add_series(f"IPEA.{tck}",
                       d["SERNOME"],
                       "IPEA", 
                       "ECON", 
                       "SERIES-TEMPORAIS")
            
    print(f"series from IPEA added to the database: {time.time() -t0}")    


if __name__ == "__main__":
    ingest_series(tickers)
