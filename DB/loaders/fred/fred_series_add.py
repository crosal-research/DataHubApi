# import system
from concurrent.futures import ThreadPoolExecutor as executor
import json

# import from packages
import requests

# import from app
from DB.transactions import add_series

with open("./configuration.json") as fp:
    config = json.load(fp)
_key_fred = config["ApiKeys"]["fred"]
    
with open("./DB/loaders/fred/fred_series.json") as fp:
    tickers = json.load(fp)


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

urls = [build_fred(_key_fred, tck) for tck in tickers]
Atickers = [f"FRED.{tck}" for tck in tickers]

with executor() as e:
    infos = list(e.map(process, urls))

for num, tck in enumerate(Atickers):
    add_series(tck, infos[num], "FRED", 'SERIES-TEMPORAIS')
    print(f"Series {tck} added to the Database")
    

