# import from system
import json, os

# import from app
from DB.transactions import add_series

p = os.path.dirname(__file__)
with open(p + "/series.json") as fj:
    srs = json.load(fj)

for s in srs:
    k = list(s.keys())[0]
    add_series(f"stn.{k}".upper(), 
               s[k].upper(), 
               "STN", 
               "RES-FISCAL", 
               "SERIES-TEMPORAIS")
