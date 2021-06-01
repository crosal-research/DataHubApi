# import from system
import re, time
from datetime import datetime as dt
from concurrent.futures import ThreadPoolExecutor

import json, time
from typing import Optional


# import from packges
import requests
import pandas as pd
import numpy as np
import pendulum


# import from app
from DB.transactions import add_obs


# information necessary to build the url to fetch the observation in block
data = {"IPCA":["7060/p/all/v/63,66/c315/all", # new
                "1419/p/all/v/63,66/c315/all"],# old
        "IPCA15":["7062/p/all/v/355,357/c315/all", # new
                  "1705/p/all/v/355,357/c315/all"]} # old


def _build_url(indicator:str, dat:str, new:bool=True) -> str:
    """
    For indicator {IPCA, IPCA-15}, nth-limit last observations, 
    and whether we should fetch the old or new observations of the 
    indicator (that is, before or after 2020-01). 
    Returns the url for that particular request.
    """
    ticker = data[indicator][0 if new else 1]
    tck_new = ticker.replace("all", f"{dat}", 1)
    return f"http://api.sidra.ibge.gov.br/values/t/{tck_new}/n1/1/f/a"


def _process(url:str, s:requests.Session) -> Optional[pd.DataFrame]:
    """
    From a specific url to acess IBGE's api, fetch the data in a
    dictionary form, and return a dataframe with the necessary information
    and filtered data.
    output:
    pd.DataFrameo
    - index = [1, 2, ...]
    - columns = [ticker, data, value]
    """
    l = 20
    attempt =  0
    while (attempt <= l):
        try:
            resp = s.get(url, stream=True)
            break
        except:
            print("Data no yet available")
            time.sleep(0.5)
            attemp += 1
            if attemp > l:
                return None

    url = resp.url
    tbl = re.search(r"values/t/(\d{4})/", url).group(1)
    indic = {"7060":"IPCA", "1419":"IPCA", "7062":"IPCA15", "1705":"IPCA15"}[tbl]
    rp = b""
    for line in resp.iter_lines():
        rp = rp + line
    df = pd.DataFrame(json.loads(rp.decode("utf-8"))[1:]).loc[:,["D1C", "D2C", "D3C",  "V"]] #ver essa linha para optimizar codigo.
    dc = df.apply(lambda x: f"ibge.{indic}/p/all/v/{x[1]}/c315/{x[2]}".upper(), 
                  axis=1).to_frame()
    dc.columns = ["tickers"]
    dn = pd.concat([dc, df.loc[:, ['D1C', 'V']]], axis=1)
    dn["D1C"] = pd.to_datetime(dn["D1C"], format="%Y%m")
    dn = dn.replace(to_replace="...", regex=False, value=pd.NA).dropna()
    dn["V"] = dn["V"].apply(lambda v: float(v))
    return dn


def fetch(s:requests.Session, 
          indicator:str, dat:Optional[str]=None,
          new:bool=True, ) -> pd.DataFrame:
    """
    Fetches the data for weigh and changes from the  IBGE api
    for a specific inflation indicator. Limit is that last=limit observations
    new=True is to fetch the new indicator, from valid from 2020-10. 
    Returns pandas dataframe
    """
    
    url = _build_url(indicator, dat=dat, new=new)
    return _process(url, s)


def _add_data_frame(df: pd.DataFrame) -> None:
    """
    takes a data frame with the information about observations on a 
    particular indicator {IPCA, IPCA15} and adds to the database.
    """
    # turn this code into something more efficient
    for i in range(0, df.shape[0]):
        try:
            input = df.iloc[i,:].values
            add_obs(input[0], input[1].to_pydatetime(), float(input[2]))
        except:
            print(f"failed to add series {df.iloc[i,:].values[0]}")



def fetch_obs(cpi:str, ini: Optional[str]=None, 
              end:Optional[str]=None) -> None:
    """
    cpi is the kind of indicator [IPCA, IPCA15], 
    end is the last time observation to be updated
    and ini is the first
    """
    import pendulum
    cpi = cpi.upper()
    end = pendulum.parse(end)
    init = pendulum.datetime(2012,1,1) if cpi == "IPCA" else pendulum.datetime(2012,2,1)
    if ini is not None:
        ini = pendulum.parse(ini) if pendulum.parse(ini) >= init else init
    else:
        ini = end
    period = pendulum.period(ini, end)
    months = [p.format("YYYYMM") for p in period.range('months')]

    s = requests.Session()
    s.mount('http://', 
            requests.adapters.HTTPAdapter(pool_connections=1,
                                          pool_maxsize=len(months)))

    def _insert_ipca(indicator, month):
        if cpi == "IPCA":
            if (pendulum.from_format(month, "YYYYMM") <= pendulum.datetime(2019,12,1)):
                df = fetch(s, cpi, dat=month, new=False)
            else:
                df = fetch(s, cpi, dat=month, new=True)
        else:
            if (pendulum.from_format(month, "YYYYMM") <= pendulum.datetime(2020,1,1)):
                df = fetch(s, cpi, dat=month, new=False)
            else:
                df = fetch(s, cpi, dat=month, new=True)
        _add_data_frame(df)
        print(month)

    [_insert_ipca(cpi, m) for m in  months]
    s.close()
        

if __name__ == "__main__":
    import sys
    print(f"adding {sys.argv[1]} starting from {sys.argv[2]} in the DataBase")
    fetch_obs("IPCA", "2021-03-01", "2021-04-01")

