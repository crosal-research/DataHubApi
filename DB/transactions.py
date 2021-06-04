# import from system
import re, json
from typing import Optional, List
from datetime import datetime as dt
from concurrent.futures import ThreadPoolExecutor as executor

# import from packages
import pandas as pd
from pony import orm

#import from app
from DB.db import db

# Constants
DATE_INI = "1900-01-01" # good for when no initial date is provided
DATE_END = "2100-01-01" # good for when no final  date is provided

# series
# upserts 
@orm.db_session
def add_source(source:str, description:str, survey: str, 
               database:str):
    """
    Add source to the base with info in capital letters
    """
    Usource = source.upper()
    Udatabase = database.upper()
    Usurvey = survey.upper()
    if (src:= db.SourceDB.get(source=Usource, survey=Usurvey,
                              database=Udatabase)) is None:
        db.SourceDB(source=Usource, description=description, 
                    survey=Usurvey, database=Udatabase)
        print(f"New source ({Usource}, {Usurvey}, {Udatabase}) added to the Database")
    else:
        src.description = description
        print(f"SourceDB ({Usource}, {Usurvey}, {Udatabase}) already in the Database. Description Updated")


@orm.db_session
def add_series(ticker:str, description:str, source: str, survey: str, database:str,
               group:Optional[str]=None, kind:Optional[str]=None) -> None:
    """
    Adds a series to the database, with ticker, descript, group in the 
    cpi, kind (peso/variacao) and indicators (cpi, cpi-15).
    """
    Uticker = ticker.upper()
    Usource = source.upper()
    Usurvey = survey.upper()
    Udescription = description.upper()
    Udatabase = database.upper()
    Ugroup = group.upper() if group is not None else None
    Ukind = kind.upper() if kind is not None else None
    Usrc = db.SourceDB.get(source=Usource, survey=Usurvey, database=Udatabase)

    if Usrc.database == "INFLACAO" and Usrc.source == "IBGE":
        if (tck:= db.SeriesInflation.get(ticker=Uticker)) is None:
            db.SeriesInflation(ticker=Uticker, description=Udescription, 
                               group=Ugroup, kind=Ukind, 
                               source=Usrc)
            print(f"Series {ticker} added to the Database {(Usrc.source, Usrc.survey, Usrc.database)}")
        else:
            tck.description = Udescription
            tck.group = Ugroup
            tck.kind = Ukind
            print(f"Series {ticker} updated in the Database {(Usrc.source, Usrc.survey, Usrc.database)}")
    else: # if ticker doesn't belong to database inflacao
        if (tck:= db.Series.get(ticker=Uticker)) is None:
            db.Series(ticker=Uticker, description=Udescription, 
                      source=db.SourceDB.get(source=Usource, survey=Usurvey, database=Udatabase))
            print(f"Series {ticker} added to the Database {(Usrc.source, Usrc.survey, Usrc.database)}")
        else:
            tck.description = Udescription
            print(f"Series {ticker} updated in the Database {(Usrc.source, Usrc.survey, Usrc.database)}")


@orm.db_session        
def add_obs(ticker:str, data:dt, value:Optional[float]) -> None:
    """adds a particular observation for the series given by ticker, for
    a particular data.
    """
    Uticker = ticker.upper()
    if "IPCA" in Uticker:
        srs = db.SeriesInflation.get(ticker=Uticker)
    else:
        srs = db.Series.get(ticker=Uticker)

    if srs is not None:
        if (obs := db.Observation.get(series=srs, data=data)) is not None:
            obs.value = value
        else:
            db.Observation(series=srs, data=data, value=value)
    else:
        print(f"Ticker {Uticker} is not in the database")


def add_batch_obs(ticker:str, df:pd.DataFrame):
    """
    Adds dataframe n x 1 into the database. The index should be
    a datetime.index and the observations of the data column float numbers
    
    """
    if df.shape[1] == 1:
        [add_obs(ticker.upper(), ind, float(df.loc[ind].values)) for ind in df.index]
    else:
        print(f"Data Frame with of {ticker} has wrong dimension")



# Queries
@orm.db_session
def fetch_info_by_ticker(ticker:str) -> pd.DataFrame:
    """
    Fetch the information for a particular series defined by the
    its ticker
    """
    obs = orm.select((s.ticker, 
                      s.description, 
                      s.group, 
                      s.kind, 
                      s.indicator) for s in db.Series if s.ticker==ticker.upper())
    return pd.DataFrame(list(obs), 
                        columns=["ticker", 
                                 "description", 
                                 "group", 
                                 "kind", 
                                 'indicator'])


@orm.db_session
def fetch_series_list(source:str, survey: str, database:str) -> List[str]:
    src = db.Series.select(lambda s:  (s.source.source == source.upper() and
                                       s.source.survey == survey.upper() and
                                       s.source.database == database.upper()))
    return [s.ticker for s in src]

     

@orm.db_session
def fetch_by_ticker(tickers: List[str], 
                    date_ini: Optional[str]=None, 
                    date_end: Optional[str]=None) -> pd.DataFrame:
    """
    index = list of indexes
    kind = [VARIACA0, PESO]
    indicator =[IPCA, IPCA15]
    date_int: example "2020-01-01
    date_end: example "2020-04-01
    """
    Utickers = [tck.upper() for tck in tickers]
    date_ini = dt.fromisoformat(date_ini) if date_ini is not None else dt.fromisoformat(DATE_INI)
    date_end = dt.fromisoformat(date_end) if date_end is not None else dt.fromisoformat(DATE_END)
    obs = orm.select((o.data, o.value, o.series.ticker)
                     for o in db.Observation 
                     if ((o.series.ticker in Utickers) and 
                         (o.data >= date_ini) and 
                         (o.data <= date_end)))
    df = pd.DataFrame(data=obs, 
                      columns=["date", 
                               "values", 
                               "tickers"]).pivot(index="date", columns="tickers", values="values")
    return df.reindex(Utickers, axis=1)


# deletes
@orm.db_session
def delete_observations(source:str, survey:str, 
                        database:str, 
                        date_ini:str, date_end:str):
    """
    deletes all the obervations of pertaining to a indicator 
    [IPCA, IPCA15] at a particular date
    """
    date_ini = dt.fromisoformat(date_ini)
    date_end = dt.fromisoformat(date_end)
    sourcedb = db.SourceDB.get(source=source.upper(), 
                               survey=survey.upper(), 
                               database=database.upper())
    obs = orm.select(o for o in db.Observation 
                     if ((o.series.source == sourcedb) and 
                         (o.data >= date_ini) and 
                         (o.data <= date_end)))
    if len(obs) >1:
        obs.delete(bulk=True)
        return "ok"
    else:
        return "not available"


# tables
# upserts
@orm.db_session
def create_tbl(tticker:str, description: str, 
               tickers:List[str]) -> None:
    """
    creates a table of name 'name' and with tickers
    """
    Utticker = tticker.upper()
    Utickers = [tck.upper() for tck in tickers]
    if not (tbl:= db.TableDb.get(tticker=Utticker)):
        tbl = db.TableDb(tticker=Utticker,
                         tbl_description=description.upper())
        for tck in Utickers:
            srs = db.Series.get(ticker=tck) 
            tbl.series.add(srs)
        return "ok"
    else:
        return None


@orm.db_session
def modify_tbl(tticker:str, tickers:List[str], 
               description:Optional[str]=None) -> None:
    """
    creates or update (if it is already in the database)
    a table of name 'name' and with tickers
    """
    Utticker = tticker.upper()
    Utickers = [tck.upper() for tck in tickers]
    tbl = db.TableDb.get(tticker=Utticker)
    if tbl:
        tbl.series.clear()
        for tck in Utickers:
            if (srs:=db.Series.get(ticker=tck)):
                tbl.series.add(srs)
        if description:
            tbl.tbl_description = description
        return "ok"
    else:
        return None


# delete
@orm.db_session
def delete_tbl(tticker:str) -> None:
    """
    deletes table 'name' from the database
    """
    if (tbl:=db.TableDb.get(tticker=tticker.upper())):
        tbl.delete()
        return "ok"
    else:
        return None


# fetch
@orm.db_session
def fetch_tbl(tticker:Optional[str]) -> None:
    if tticker:
        Utticker = tticker.upper()
        if (tbl:=db.TableDb.get(tticker=Utticker)):
            srs = [srs.ticker for srs in tbl.series]
            return {"ticker": Utticker, 
                    "description": tbl.tbl_description,
                    "series":  srs}
    else:
        tbls = orm.select(t for t in db.TableDb)
        return [{"ticker": t.tticker, 
                 "description": t.tbl_description, 
                 "series":[s.ticker for s in t.series]} for t in tbls]


# fetch indexes to be ingested by search engine    
@orm.db_session
def fetch_all_series() -> pd.DataFrame:
    srs = orm.select((s.ticker, 
                      s.description, 
                      s.kind,
                      s.group) for s in db.Series)
    lsrs = [{"ticker": s[0], 
             "description": s[1], 
             "group": s[3]} for s in srs]
    
    with open("./configuration.json") as fj:
        fpath = json.load(fj)["Search"]["data"]
    
    with open(fpath, "w") as f:
        json.dump(lsrs, f, ensure_ascii=False)


if __name__ == "__main__":
    fetch_all_series()
