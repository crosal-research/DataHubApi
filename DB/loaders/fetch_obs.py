# import from system
from functools import wraps
from typing import Optional

# import from packages
import pendulum

# imports from app
from DB.loaders.fred import fred_obs #bcb, cepea, ibge, ons, fred, ipea
# from Loaders.Observations import mobility_apple, bcb_exp, covid, pnad_covid
# from Loaders.Observations import bcb_vencimentos
from DB.loaders.inflation import inflation_obs
from DB.loaders.ibge import ibge_obs
from DB.loaders.bcb_vencimentos import bcb_vencimentos_obs
from DB.transactions import fetch_series_list

__all__ = ["fetch_obs"]


INI="1900-01-01"
END="2100-12-31"

source_dict = {("FRED", "SERIES-TEMPORAIS"): fred_obs.fetch,
               ("IBGE", "SERIES-TEMPORAIS"): ibge_obs.fetch,
               ("BCB", "VENCIMENTOS"): bcb_vencimentos_obs.fetch,
               ("IBGE", "INFLACAO"): inflation_obs.fetch}

# source_dict = {"BCB": bcb.fetch,
#                "IPEA": ipea.fetch, 
#                "CEPEA": cepea.fetch, 
#                "COVID": covid.fetch, 
#                "PNAD_COVID": pnad_covid.fetch, 
#                "APPLE": mobility_apple.fetch, 
#                "ONS": ons.fetch, 
#                "BCB_EXP": bcb_exp.fetch}


def fetch_obs(source: str, database: str, limit:Optional[int]=None) -> None:
    """fetches and updates (inserts) observations of a particular source
    (ex:BCB) and for last (limit) observations. If limit = None, all
    observations are updated. Does the upserts through side effects
    """
    Usource = source.upper()
    Udatabase = database.upper()
    start = pendulum.now().to_datetime_string()
    tickers = fetch_series_list(Usource, Udatabase)
    results = source_dict[(Usource, Udatabase)](tickers, limit=limit)


if __name__ == "__main__":
    import sys
    fetch_obs(sys.argv[1], sys.argv[2])
