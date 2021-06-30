# import system
from concurrent.futures import ThreadPoolExecutor as executor
import json, os

# import from packages
import requests

# import from app
from DB.transactions import add_series



with open("./configuration.json") as fp:
    config = json.load(fp)
_key_fred = config["ApiKeys"]["fred"]


def build_fred(key, ticker):
    return f"https://api.stlouisfed.org/fred/series?series_id={ticker}"  \
        + f"&api_key={key}&file_type=json"

def process(url):
    resp = requests.get(url).json()["seriess"][0]
    sea = resp['seasonal_adjustment']
    title = resp['title']
    freq = resp['frequency']
    units= resp['units']
    return f"{title}, {sea}, {freq}, {units}"


if __name__ == "__main__":
    p = os.path.dirname(__file__)
    with open(p + "/fred_series.json") as fp:
        tickers = json.load(fp)

    urls = [build_fred(_key_fred, tck) for tck in tickers[0]]
    Atickers = [f"FRED.{tck}" for tck in tickers[0]]

    with executor() as e:
        infos = list(e.map(process, urls))

    for num, tck in enumerate(Atickers):
        if tickers[0][tck.split(".")[1]] == "ECON":
            add_series(tck, infos[num], "FRED", tickers[0][tck.split(".")[1]], 'SERIES-TEMPORAIS')
        else:
            add_series(tck, infos[num], "FRED", tickers[0][tck.split(".")[1]], 'FINANCE')
    

