############################################################
# Module com transações especificas para computar
# nucleos de inflação
############################################################


# import from system
import re, json
from typing import Optional, List
from datetime import datetime as dt

# import from packages
import pandas as pd
from pony import orm

#import from app
from DB.db import db

# Constants
DATE_INI = "1900-01-01" # good for when no initial date is provided
DATE_END = "2100-01-01" # good for when no final  date is provided

# series
@orm.db_session
def fetch_all(kind="VARIACAO", indicator="IPCA", 
              date_ini:Optional[str] = None, date_end:Optional[str]=None):
    """
    fetch all the series for some kind and some indicator. 
    """
    date_ini = dt.fromisoformat(date_ini) if date_ini is not None else dt.fromisoformat(DATE_INI)
    date_end = dt.fromisoformat(date_end) if date_end is not None else dt.fromisoformat(DATE_END)

    dd = orm.select((o.data, o.value, o.series.ticker) for o in db.Observation
                    if (o.series.kind==kind and 
                        o.series.indicator==indicator and 
                        o.data >= date_ini and 
                        o.data <= date_end and 
                        o.series.group != "NUCLEO"))
    return pd.DataFrame(dd, columns=["date", "change", 
                                     "ticker"]).set_index(["date"]).pivot(values="change", 
                                                                          columns="ticker")


@orm.db_session
def fetch_group(group="GRUPO", kind="VARIACAO", indicator="IPCA",
                ticker=False, date_ini:Optional[str] = None, 
                date_end:Optional[str] = None) -> pd.DataFrame:
    """
    group = [GERAL, GRUPO, SUBGRUPO, ITEM, SUBITEM, NUCLEO]
    kind = [VARIACA0, PESO]
    indicator =[IPCA, IPCA15]
    tickers = Boolean is headers should be tickers 
    date = str (ex: 2020-01-01)
    date_end = str (ex: 2021-01-01)
    """
    date_ini = dt.fromisoformat(date_ini) if date_ini is not None else dt.fromisoformat(DATE_INI)
    date_end = dt.fromisoformat(date_end) if date_end is not None else dt.fromisoformat(DATE_END)
    Ugroup = group.upper()
    Uindicator = indicator.upper()
    
    if not ticker:
        dd = orm.select((o.data, o.value, o.series.description) for o in db.Observation
                    if (o.series.group==Ugroup and 
                        o.series.kind==kind and 
                        o.series.indicator==Uindicator and 
                        o.data >= date_ini and 
                        o.data <= date_end))
        df = pd.DataFrame(dd, columns=["date", "change", 
                                       "description"]).set_index(["date"]).pivot(values="change", 
                                                                                 columns="description")
        if (len(df.columns) > 1):
            if Ugroup !="NUCLEO":
                df.columns = [re.search(r",\s\d+\.(.*)", c).group(1) for c in df.columns]
        else:
            df.columns = ["INDICE GERAL"]
        return df    
    else:
        dd = orm.select((o.data, o.value, o.series.ticker) for o in db.Observation 
                            if (o.series.group==Ugroup and 
                                o.series.kind==kind and 
                                o.series.indicator==Uindicator and 
                                o.data >= date_ini and 
                                o.data <= date_end))
        return pd.DataFrame(dd, columns=["date", "change", 
                                         "ticker"]).set_index(["date"]).pivot(values="change", 
                                                                                 columns="ticker")


