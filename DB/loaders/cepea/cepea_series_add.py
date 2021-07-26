################################################################################
# fetches data from cepea database
#
# Obs:
# for clarification in regards to the olefile module check
# this stackoverflow thread: https://stackoverflow.com/questions/
# 58336366/compdocerror-when-importing-xls-file-format-to-python-using-pandas-read-excel
################################################################################

# import form the system
from concurrent.futures import ThreadPoolExecutor as executor
from typing import List
import logging, logging.config

# import from packages
import requests
import olefile
import pandas as pd

#app imports
from DB.transactions import add_series

# logging
logging.config.fileConfig('./logging/logging.conf')
logger = logging.getLogger('dbLoaders')


# series to be included
info = [("milho", 77), 
        ("acucar", 53),
        ("etanol-diario-paulinia", 111), 
        ("boi-gordo", 2)]

tickers = [f"cepea.{n}" for _, n in info]


def build_url(name:str, number:int) -> str:
    """
    builds the relevant url for the ticker in case
    """
    return f'https://www.cepea.esalq.usp.br/br/indicador/series/{name}.aspx?id={number}'


def process(resp:requests.models.Response) -> pd.DataFrame:
    """
    fetchs the series related to the url used as input. Return a 
    dataframe
    """
    if resp.ok:
        ole = olefile.OleFileIO(resp.content)
        return  pd.read_excel(ole.openstream("Workbook"))
    logger.error(f"Unable to fetch data from {resp.url}")
    


def ingest_series(info: List[tuple]) -> None:
    """
    adds series information into the database
    """
    with requests.session() as session:
        with executor() as e:
            urls = []
            for p in info:
                urls.append(build_url(*p))
            dfs = list(e.map(lambda u: process(session.get(u)), urls, timeout=60))

    for n,tck in enumerate(tickers):
        add_series(tck, dfs[n].columns[0], "CEPEA", "AGRI", "SERIES-TEMPORAIS")


if __name__ == '__main__':
    ingest_series(info)
    


