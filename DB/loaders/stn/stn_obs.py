
# import from the system
from typing import Optional, List
from datetime import datetime as dt
import json

# imports from packages
import requests
import pandas as pd
from bs4 import BeautifulSoup as bs
import pendulum as pl

#import from app
from DB.transactions import add_obs


__all__ = ["fetch_obs"]

today = pl.today()

def fetch_sheet(date:Optional[dt]=today) -> Optional[pd.DataFrame]:
    """fetches numbers from Treasury Monthly results performing a series
    of requests until finding the appropriate url is disclosed and
    finally fetching the spreadsheet. data format is "%Y/%m"
    """
    dat = date.format("Y/M")
    origin = f"https://www.tesourotransparente.gov.br/publicacoes/boletim-resultado-do-tesouro-nacional-rtn/{dat}"
    resp = requests.get(origin)
    
    itr = 0
    while not resp.ok:
        date = date.subtract(months=1)
        dat = date.format("Y/M")
        origin = f"https://www.tesourotransparente.gov.br/publicacoes/boletim-resultado-do-tesouro-nacional-rtn/{dat}"
        resp = requests.get(origin)
        itr += 1
        print(itr)
        if itr > 20:
            print("Number of Attempts Exhausted")


    soup = bs(resp.text, "html.parser")
    for link in soup.find_all('a'):
        if "title" in link.attrs.keys():
            if "serie_historica"in link.attrs["title"]:
                url = link.attrs["href"]
                break
    res = requests.get(url)
    content = bs(res.text, "html.parser")
    new_url = content.find_all("frame")[0].attrs["src"]
    return pd.read_excel(new_url, sheet_name="1.1", index_col=[0], header=[0],
                             skiprows=[0, 1, 2, 3, 73, 74, 75, 76, 77, 78, 79, 80]).T


def fetch(tickers:List[str], limit:Optional[int]=None):
    """
    Fetches observations of list of tickers for stn 
    in the database
    """
    def _add_obs(df: pd.DataFrame, limit:Optional[int]=None) -> None:
        """
        Adds fetched observations into the database
        """
        for c in dl.columns:
            for ind in dl.index:
                add_obs(c, ind.to_pydatetime(), dl.loc[ind, c])

    df = fetch_sheet()
    df.columns = [f"STN.{''.join(((c.split(' ')[0]).split('.')))}" for c in df.columns]
    dl = df.tail(limit) if limit else df
    _add_obs(dl.loc[:, tickers])
    print("STN updated")

            

