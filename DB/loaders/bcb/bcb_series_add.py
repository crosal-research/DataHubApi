# imports from python's system
from concurrent.futures import ThreadPoolExecutor as executor
from typing import Optional, List
from datetime import datetime as dt
import time, pickle, os

# import from packages
import requests
import pandas as pd
import suds.client
import suds_requests

# import from app
from DB.transactions import add_series

__all__ = ['series_add']

tcks_off = []

gestores = {"DSTAT/DIFIN/SUFIP": ("BCB", "FISCAL", "SERIES-TEMPORAIS"),
           "DSTAT/DIMOB/SUSIF": ("BCB", "MERCADO-ABERTO", "TITULOS-PUBLICOS"),
           "DSTAT/DIMOB/SUCRE": ("BCB", "CREDITO/PMON", "SERIES-TEMPORAIS"),
           "DSTAT/DIMOB/SUMON": ("BCB", "CREDITO/PMON", "SERIES-TEMPORAIS"),
           "DEMAB/DIGER/SUSIS": ("BCB", "CREDITO/PMON", "SERIES-TEMPORAIS"),
           "DEMAB/DIGER/SUEST": ("BCB", "CREDITO/PMON", "SERIES-TEMPORAIS"),
            "DEMAB/DICEL": ("BCB", "CREDITO/PMON", "SERIES-TEMPORAIS"),
           "DEMAB/DIGER": ("BCB", "CREDITO/PMON", "SERIES-TEMPORAIS"),
           "DESIG/GERIM/DIRIM/COLIQ-01": ("BCB", "ESTABILIDADE/FIN", "SERIES-TEMPORAIS"),
           "DESIG/GERIM/DIRIM/CORIM": ("BCB", "ESTABILIDADE/FIN", "SERIES-TEMPORAIS"),
           "DESIG/GESEG/DIMAC/COMOC": ("BCB", "ESTABILIDADE/FIN", "SERIES-TEMPORAIS"),
           "DESIG/GESEG/DISEF/COMOR": ("BCB", "ESTABILIDADE/FIN", "SERIES-TEMPORAIS"),
           "DESIG/GERIM/DIRIM/CORAC": ("BCB", "ESTABILIDADE/FIN", "SERIES-TEMPORAIS"),
           "DESIG/GERIS/DIRIM/COPAR": ("BCB", "ESTABILIDADE/FIN", "SERIES-TEMPORAIS"),
           "DESIG/GERIS/DIRIS/COPAR": ("BCB", "ESTABILIDADE/FIN", "SERIES-TEMPORAIS"),
           "DSTAT/DIBAP/SUBAP": ("BCB", "SETOR-EXTERNO", "SERIES-TEMPORAIS"),
           "DEPIN/GEROP/DICAM": ("BCB", "SETOR-EXTERNO", "SERIES-TEMPORAIS"),
           "DSTAT/DIBAP/SUDEX": ("BCB", "SETOR-EXTERNO", "SERIES-TEMPORAIS"),
           "DEPIN/GEROP/DILIF": ("BCB", "SETOR-EXTERNO", "SERIES-TEMPORAIS"),
           "DEPEC/COACE": ("BCB", "ECON", "SERIES-TEMPORAIS"),
           "DEPEC/GECON/COSUL": ("BCB", "ECON", "SERIES-TEMPORAIS"),
           "DEPEC/COACE/COATI": ("BCB", "ECON", "SERIES-TEMPORAIS"),
           "DEPEC/COACE/COPRE": ("BCB", "ECON", "SERIES-TEMPORAIS"),
           "DEPEC/GECON/COSUD/NUCMG": ("BCB", "ECON", "SERIES-TEMPORAIS")}



fonte_in =  ["MF-STN", 
             "Fipe", 
             "Anfavea", 
             "Anbima", 
             "ANP", 
             "BCB e FGV", 
             "Sisbacen PESP300", 
             "Eletrobras", 
             "Fiergs", 
             "PTAX",
             "Cetip", 
             "FGC",
             "BCB-Desig", #ok
             "FGV", 
             "Copom", 
             "MF-Cotepe", 
             "Sisbacen PTAX800", 
             "CNI", 
             "BCB-Depin", #ok
             "Fiesp", 
             "ME", 
             "Dieese", 
             "Fenabrave", 
             "BCB-Depec", 
             "BCB-DSTAT", 
             "BCB-Derin", 
             "BCB-Demab", 
             "BCB",
             "BCB-Depep", 
             "Abraciclo"]

remove = {"tickers":
          ['1175',
           '1177',
           '405', 
           '406', 
           '407', 
           '408', 
           '409',
           '25623',
           '25624',
           '3543',
           '3455',
           '12461', # andima
           '12462', 
           '12463', 
           '12464', 
           '12465', 
           '194',  #dieese
           "7348",
           '1344', 
           '1345', 
           '1346', 
           '1347', 
           '1348', 
           '1349', 
           '1350', 
           '1351', 
           '1352', 
           '1353', 
           '1354', 
           '1355', 
           '1356', 
           '1357', 
           '1358', 
           '1359', 
           '1360', 
           '1361', 
           '1362', 
           '1363', 
           '1364', 
           '1365', 
           '1366', 
           '1367', 
           '1368', 
           '1369', 
           '1341',
           '1370', 
           '7343', 
           '7344', 
           '7345', 
           '7346', 
           '7347', 
           '7353', 
           '24246', 
           '24350', 
           '2256', # MF-STN
           '2257', 
           '2258', 
           '2259', 
           '2260', 
           '2261', 
           '2262', 
           '2263', 
           '2264', 
           '2265', 
           '2266', 
           '2267', 
           '2268', 
           '2269', 
           '2270', 
           '2271', 
           '2272', 
           '2273', 
           '2274', 
           '2275', 
           '2276', 
           '2277', 
           '2278', 
           '2279', 
           '2280', 
           '2281', 
           '2282', 
           '2283', 
           '2284', 
           '2285', 
           '2286', 
           '2287', 
           '2288', 
           '2289', 
           '2290', 
           '2291', 
           '2292', 
           '2293', 
           '2294', 
           '2295', 
           '2296', 
           '2297', 
           '2298', 
           '2299', 
           '2300',           
           '4353', 
           '4354', 
           '4355', 
           '4356', 
           '4357', 
           '4358', 
           '4359', 
           '4360', 
           '4361', 
           '4362', 
           '4363', 
           '4364', 
           '4365', 
           '4366', 
           '4367', 
           '4368', 
           '4369', 
           '4370', 
           '4371', 
           '4372', 
           '4373', 
           '4374', 
           '4375', 
           '4376', 
           '4377', 
           '4378', 
           '4379',
           '7544', 
           '7545', 
           '7546', 
           '7547', 
           '7548', 
           '7549', 
           '7550', 
           '7551', 
           '7552', 
           '7553', 
           '7554', 
           '7555', 
           '7556', 
           '7557', 
           '7558', 
           '7559', 
           '7560', 
           '7561', 
           '7562', 
           '7563', 
           '7564', 
           '7565', 
           '7566', 
           '7567', 
           '7568', 
           '7569', 
           '7570', 
           '7571', 
           '7572', 
           '7573', 
           '7574', 
           '7575', 
           '7576', 
           '7577', 
           '7578', 
           '7579', 
           '7580', 
           '7581', 
           '7582', 
           '7583', 
           '7584', 
           '7585', 
           '7586', 
           '7587', 
           '7588', 
           '7589', 
           '7590', 
           '7591', 
           '7592', 
           '7593', 
           '7594', 
           '7595', 
           '7596', 
           '7597', 
           '7598', 
           '7599', 
           '7600', 
           '7601', 
           '7602', 
           '7603', 
           '7604', 
           '7605', 
           '7606', 
           '7607', 
           '7608', 
           '7609', 
           '7610', 
           '7611', 
           '7612', 
           '7613'
           '24389', 
           '24390', 
           '24391', 
           '24392', 
           '24393', 
           '14001', 
           '21559',
           '27603']}



def _cleasing(series: Optional[dict], freq:list) -> dict:
    """
    Keep series to be added in the db based on not been in series
    and have freq
    """
    if series is not None:
        if series["freq"] in freq:
            if series["final"].year == 2021:
                if series["fonte"] in fonte_in:
                    if str(series["number"]) not in remove['tickers']:
                        return series


def _process_info(resp: suds.sudsobject) -> dict:
    """
    process resp from suds response (last observation)
    and grabs information for the series
    """
    last = resp.ultimoValor
    return dict(fonte = str(resp.fonte),
                gestor = str(resp.gestorProprietario),
                freq = str(resp.periodicidadeSigla),
                nome = str(resp.nomeCompleto),
                number = int(resp.oid),
                final = dt(last.ano, last.mes, last.dia))


def fetch_series(tickers: List[str]) -> List[dict]:
    """
    Fetches the meta information for ticker list and returns
    a list of dictionaries with the information
    """
    with requests.Session() as session:
        c = suds.client.Client(
            'https://www3.bcb.gov.br/sgspub/JSP/sgsgeral/FachadaWSSGS.wsdl',
            transport=suds_requests.RequestsTransport(session))
        
        def _fetch(tck):
            try:
                resp = c.service.getUltimoValorVO(tck)
                if resp is not None:
                    return _process_info(resp)
            except:
                tcks_off.append(tck)

        with executor() as e:
            ls = list(e.map(_fetch, tickers))
        return ls


def final_series():
    """
    Generates the final list of series to be used (inserted)
    in the database. Raw input (by side effect) comes from 
    file codigos.xlsx
    """
    tickers = pd.read_excel(os.path.abspath(os.path.dirname(__file__)) +"./codigos.xlsx", 
                        header=[0]).values.flatten()
    # tickers = pd.read_excel("./codigos.xlsx", 
    #                     header=[0]).values.flatten()
    ls = fetch_series(list(set(tickers)))
    net_series = [s for s in ls if _cleasing(s, ["D", "M"]) is not None]
    p = os.path.abspath(os.path.dirname(__file__))
    with  open(p + "/series_bcb", "wb") as f:
        pickle.dump(net_series, f)
    # with  open("./series_bcb", "wb") as f:
    #     pickle.dump(net_series, f)        


def series_ingestion(series:List[dict]) -> None:
    """
    add series for bcb into the database
    """
    for srs in series:
        try:
            add_series("BCB." + str(srs['number']), 
                       srs['nome'], 
                       *gestores[srs['gestor']]) 
        except:
            print(f"Unable to add series BCB.{srs['number']}")



if __name__ == "__main__":
    p = os.path.abspath(os.path.dirname(__file__))
    with open(p + "/series_bcb", "rb") as fp:
        lsseries = pickle.load(fp)
    series_ingestion(lsseries)
