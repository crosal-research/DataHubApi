# import from app
from DB.transactions import add_series


list_quote = {
    "yahoo.ES=F": "Mini S&P 500 next maturing contract",
    "yahoo.YM=F": "Dow Jones Industrial next maturing contract",
    "yahoo.NQ=F": "Nasdaq 100 next maturing contract", 
    "yahoo.RTY=F": "E-mini Russell 2000 next maturing contract", 
    "yahoo.GC=F": "Gold next maturing contract",	
    "yahoo.SI=F": "Silver next maturing contract",	
    "yahoo.PL=F": "Platinum next maturing contract", 
    "yahoo.HG=F": "Copper next maturing contract", 
    "yahoo.PA=F": "Palladium next maturing contract", 
    "yahoo.CL=F": "Crude Oil next maturing contract",	
    "yahoo.ZC=F": "Corn Futures next maturing contract",
    "yahoo.ZO=F": "Oat Futures",
    "yahoo.KE=F": "KC HRW Wheat next maturing contract",
    "yahoo.ZR=F": "Rough Rice next maturing contract",
    "yahoo.ZS=F": "Soybean Futures next maturing contract",
    "yahoo.GF=F": "Feeder Cattle next maturing contract",
    "yahoo.HE=F": "Lean Hogs Futures next maturing contract",
    "yahoo.LE=F": "Live Cattle Futures next maturing contract",
    "yahoo.CC=F": "Cocoa next maturing contract",
    "yahoo.KC=F": "Coffee next maturing contract",
    "yahoo.CT=F": "Cotton next maturing contract",
    "yahoo.LB=F": "Lumber next maturing contract",
    "yahoo.OJ=F": "Orange Juice next maturing contract",
    "yahoo.SB=F": "Sugar next maturing contract"}

props = ['Open', 'High', 'Low', 'Close', 'Adj_Close', 'Volume']
tickers = [(f"{x}/{y}", f"{list_quote[x]}, Daily {y}", "YAHOO", "YAHOO-FUTURES", "FINANCE") for x in list_quote for y in props]


if __name__ == "__main__":
    [add_series(*tck) for tck in tickers]

