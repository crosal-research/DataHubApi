# import from system
import json
from typing import List, Optional
import datetime

# import from packages
from fastapi import APIRouter, Query, Response
from pydantic import BaseModel, Field

# import from app
from DB.transactions import fetch_tbl, delete_tbl, create_tbl, modify_tbl
from DB.loaders import fetch_obs

router = APIRouter()


class Tabledb(BaseModel):
    """
    model for modifying tables
    """
    ticker: str = Field(..., regex="^tbl\..+" )
    description: Optional[str] = Field(None)
    series: Optional[List[str]] = Field(None, regex="^.+\..+")


# tables
@router.get("/api/v0.1/tables")
async def get_table(ticker:str=Query(None)):
    """
    fetches all the info (tickers, description) for a 
    table with ticker
    """
    return fetch_tbl(ticker)


@router.post("/api/v0.1/tables")
async def create_table(table:Tabledb):
    """
    creates a table with ticker, description, series
    and adds into the database
    """
    status = create_tbl(table.ticker, table.description, table.series)
    if status:
        return f"Table {table.ticker} created!"
    return f"Table {table.ticker} is not in the database"


@router.put("/api/v0.1/tables")
async def madify_table(table:Tabledb):
    """
    modifies a table by changing either the 
    description or the series in the table
    """
    if (status:=modify_tbl(table.ticker, table.series)):
        return f"Table {table.ticker} modified!"
    return f"Table {table.ticker} not in the database"


@router.delete("/api/v0.1/tables")
async def delete_table(table:Tabledb):
    """
    deletes a table with a particular ticker
    """
    if (status:=delete_tbl(table.ticker)):
        return f"Table {table.ticker} deleted!"
    return f"Table {table.ticker} not in the database"
