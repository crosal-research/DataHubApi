# import from system
import json
from typing import List, Optional
import datetime

# import from packages
from fastapi import APIRouter, Query, Response
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

# import from app
from DB.transactions import fetch_by_ticker, delete_observations, add_obs
from DB.transactions import fetch_tbl, delete_tbl, create_tbl, modify_tbl
from DB.transactions import fetch_all_series
from DB.loaders.fetch_obs import fetch_obs

router = APIRouter()

#classes
class Ind_obs(BaseModel):
    """
    model to modifying, creating observations for a particular
    indicator: refers to all observations at a particular date for one
    indicator [IPCA, IPCA15] at a date
    """
    survey: str = Field(...)
    date_ini: datetime.date = Field(...)
    date_end: datetime.date = Field(...)


class Tabledb(BaseModel):
    """
    model for modifying tables
    """
    ticker: str = Field(..., regex="^tbl\..+" )
    description: Optional[str] = Field(None)
    series: Optional[List[str]] = Field(None, regex="^.+\..+")


# fetch resources
@router.get("/api/v0.1/inflation")
async def get_series(tickers: List[str]=Query(..., regex="^.+\..+"),
                     date_ini:datetime.date=Query(None), #, regex="^\d{4}-\d{2}-\d{2}$"), 
                     date_end:datetime.date=Query(None), #, regex="^\d{4}-\d{2}-\d{2}$"), 
                     form:str="csv"):
    """Get observations for a List of tickers from database or for table,
    date gives the head lower limit for the for the time seires and
    from gives the string format (csv or json)
    """
    if (len(tickers) == 1 and tickers[0].split(".")[0] == "tbl"):
        df = fetch_by_ticker(fetch_tbl(tickers[0])["series"], 
                             date_ini= date_ini.isoformat() if date_ini else None, 
                             date_end= date_end.isoformat()  if date_end else None)
    else:
        df = fetch_by_ticker(tickers, 
                             date_ini=date_ini.isoformat() if date_ini else None,
                             date_end=date_end.isoformat() if date_end else None)
    if form == "csv":
        return Response(df.to_csv(), media_type="application/text")
    return Response(df.to_json(), media_type="application/text")


# management of resources
# series
@router.post("/api/v0.1/inflation")
async def add_obs_indicator(indobs: Ind_obs):
    """
    add all observations for an indicator and a particular time as in indbos
    """
    ind = indobs.survey
    core = "CORES" if ind == "IPCA" else "CORES15"
    fetch_obs("IBGE", ind, "INFLACAO", 
              ini=(indobs.date_ini).strftime("%Y-%m-%d"), 
              end=(indobs.date_end).strftime("%Y-%m-%d"))
    fetch_obs("IBGE", core, "INFLACAO", 
              ini=(indobs.date_ini).strftime("%Y-%m-%d"),
              end=(indobs.date_end).strftime("%Y-%m-%d"))
    return f"{indobs.survey} from {indobs.date_ini} to {indobs.date_end} successfully added" 


@router.delete("/api/v0.1/inflation")
async def delete_obs_indicator(indobs: Ind_obs):
    """
    the deletes all observations for an indicator a particular time as in indbos
    """
    ind = indobs.survey
    core = "CORES" if ind == "IPCA" else "CORES15"

    r = delete_observations("IBGE", ind, "INFLACAO",
                            (indobs.date_ini).strftime("%Y-%m-%d"),
                            (indobs.date_end).strftime("%Y-%m-%d"))
    if r == "ok":
        print(f"{indobs.survey} at {indobs.date_ini} successfully deleted")
    print(f"{indobs.survey} at {indobs.date_ini} no longer at the database")

    r = delete_observations("IBGE", core, "INFLACAO",
                            (indobs.date_ini).strftime("%Y-%m-%d"),
                            (indobs.date_end).strftime("%Y-%m-%d"))
    if r == "ok":
        print(f"{ind} at {indobs.date_ini} successfully deleted")
    print(f"{ind} at {indobs.date_ini} no longer at the database")

    r = delete_observations("IBGE", core, "INFLACAO",
                            (indobs.date_ini).strftime("%Y-%m-%d"),
                            (indobs.date_end).strftime("%Y-%m-%d"))
    if r == "ok":
        print(f"{core} at {indobs.date_ini} successfully deleted")
    print(f"{ind} at {indobs.date_ini} no longer at the database")


# tables
@router.get("/api/v0.1/inflation/tables")
async def get_table(ticker:str=Query(None)):
    """
    fetches all the info (tickers, description) for a 
    table with ticker
    """
    return fetch_tbl(ticker)


@router.post("/api/v0.1/inflation/tables")
async def create_table(table:Tabledb):
    """
    creates a table with ticker, description, series
    and adds into the database
    """
    status = create_tbl(table.ticker, table.description, table.series)
    if status:
        return f"Table {table.ticker} created!"
    return f"Table {table.ticker} is not in the database"


@router.put("/api/v0.1/inflation/tables")
async def madify_table(table:Tabledb):
    """
    modifies a table by changing either the 
    description or the series in the table
    """
    if (status:=modify_tbl(table.ticker, table.series)):
        return f"Table {table.ticker} modified!"
    return f"Table {table.ticker} not in the database"


@router.delete("/api/v0.1/inflation/tables")
async def delete_table(table:Tabledb):
    """
    deletes a table with a particular ticker
    """
    if (status:=delete_tbl(table.ticker)):
        return f"Table {table.ticker} deleted!"
    return f"Table {table.ticker} not in the database"
