# import from system
from typing import Optional

# import from packages
import pendulum

# imports from app
from DB.loaders.fred import fred_obs #bcb, cepea, ons, ipea
from DB.loaders.inflation import inflation_obs
from DB.loaders.inflation import nucleos_calculos_obs
from DB.loaders.ibge import ibge_obs
from DB.loaders.bcb_vencimentos import bcb_vencimentos_obs
from DB.transactions import fetch_series_list

__all__ = ["fetch_obs"]


INI="1900-01-01"
END="2100-12-31"

source_dict = {("FRED", "ECON", "SERIES-TEMPORAIS"): fred_obs.fetch,
               ("FRED", "FIN", "SERIES-TEMPORAIS"): fred_obs.fetch,
               ("IBGE", "CN", "SERIES-TEMPORAIS"): ibge_obs.fetch,
               ("IBGE", "PIM", "SERIES-TEMPORAIS"): ibge_obs.fetch,
               ("IBGE", "PMC", "SERIES-TEMPORAIS"): ibge_obs.fetch,
               ("IBGE", "PNAD", "SERIES-TEMPORAIS"): ibge_obs.fetch,
               ("BCB", "VENC", "VENCIMENTOS"): bcb_vencimentos_obs.fetch,
               ("IBGE", "IPCA", "INFLACAO"): inflation_obs.fetch,
               ("IBGE", "IPCA15", "INFLACAO"): inflation_obs.fetch,
               ("IBGE", "CORES", "INFLACAO"): nucleos_calculos_obs.add_cores,
               ("IBGE", "CORES15", "INFLACAO"): nucleos_calculos_obs.add_cores}


def fetch_obs(source: str, survey: str, database: str, limit:Optional[int]=None,
              ini: Optional[str]=None, end:Optional[str]=None) -> None:
    """fetches and updates (inserts) observations of a particular sourcedb
    (ex:IBGE, PNAD, SERIES-TEMPORAIS) and for last (limit)
    observations. If limit = None, or withing date range ini and end,
    or else all observations are updated. Does the upserts through
    side effects
    """
    Usource = source.upper()
    Udatabase = database.upper()
    Usurvey = survey.upper()
    start = pendulum.now().to_datetime_string()
    if (source, database) == ("IBGE", "INFLACAO"):
        source_dict[(Usource, Usurvey, Udatabase)](Usurvey, ini=ini, end=end)
    else:
        tickers = fetch_series_list(Usource, Usurvey, Udatabase)
        source_dict[(Usource, Usurvey, Udatabase)](tickers, limit=limit)


if __name__ == "__main__":
    for k in source_dict:
        if k[2] == "INFLACAO":
            fetch_obs(k[0], k[1], k[2], ini="2012-01-01", end="2021-04-01")
        else:
            fetch_obs(*k)
