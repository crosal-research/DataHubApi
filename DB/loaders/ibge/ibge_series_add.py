# imports from sistem
import re, json
from concurrent.futures import ThreadPoolExecutor, as_completed

#import from packages
import requests
from bs4 import BeautifulSoup as bs

# import from app
from DB.transactions import add_series

data = {1620:{"v":"", "c":"", "c1": None, "s":"CN"}, #contas nacionais
        1621:{"v":"", "c":"", "c1": None, "s": "CN"}, #contas nacionas, des
        1846:{"v":"", "c":"", "c1": None, "s": "CN"}, #contas nacionas nominal
        3653:{"v":[0, 1], "c": "", "c1": None, "s": "PIM"}, #PIM setorial
        3650:{"v":[0, 1], "c": "", "c1": None, "s": "PIM"}, #PIM setorial
        3651:{"v":[0, 1], "c": "", "c1": None, "s": "PIM"}, #PIM setorial
        3652:{"v":[0, 1], "c": None, "c1": None, "s": "PIM"}, # PIM construção civil
        3415:{"v":[0, 1], "c": "", "c1": None, "s": "PMC"}, #PMC material de construção
        3416:{"v":[0, 1], "c":[0, 1], "c1": None, "s": "PMC"}, #PMC narrow
        3417:{"v":"", "c":[0, 1], "c1": None, "s": "PMC"}, #PMC headline
        3419:{"v":"", "c":[0, 1], "c1": "", "s": "PMC"}, #PMC
        6443:{"v":[1], "c":[0, 1], "c1": "", "s": "PMS"}, #, #PMS
        4093:{"v":[0, 4, 8, 12, 16, 20, 24], "c": None, "c1": None, "s": "PNAD"}, #pnad mensal 
        5440:{"v": [0, 2], "c":"", "c1": None, "s": "PNAD"}} #pnad rendimento



# series to be added to database 
series: list = [] # ex:[[ticker, description, country, source]]

for t in data.keys():
    kind = len(data[t]) - 1
    url = f"http://api.sidra.ibge.gov.br/desctabapi.aspx?c={t}"
    html = requests.get(url).content
    soup = bs(html,"html.parser")
    group = soup.select("span#lblNomePesquisa")[0].get_text()

    #fetch variables
    if (v:=data[t]["v"]) is not None:
        if v == "":
            nvariables = range(0, int(re.search("\d+", 
                                   soup.select("span#lblVariaveis")[0].get_text()).group()))
        else:
            nvariables = v

    for v in nvariables:
        vnumber = soup.select(f"span#lstVariaveis_lblIdVariavel_{v}")[0].get_text()
        vdescription = soup.select(f"span#lstVariaveis_lblNomeVariavel_{v}")[0].get_text()
        
        #fetch categories c
        if (cl:= data[t]["c"]) is not None:
            classe = soup.select(f"span#lstClassificacoes_lblIdClassificacao_{0}")[0].get_text()
            if cl == "":
                nclasse = range(0,int(re.search("\d+",
                                                soup.select(f"span#lstClassificacoes_lblQuantidadeCategorias_{0}")[0].get_text()).group()))
            else:
                nclasse = cl
                
            for c in nclasse:
                ticker = f"IBGE.{t}/p/all/v/{vnumber}"
                tag_id = f"span#lstClassificacoes_lstCategorias_{0}_lblIdCategoria_{c}"
                tag_name = f"span#lstClassificacoes_lstCategorias_{0}_lblNomeCategoria_{c}"
                cdescription = soup.select(tag_name)[0].get_text()
                ind = soup.select(tag_id)[0].get_text()
                ticker = f"IBGE.{t}/p/all/v/{vnumber}/c{classe}/{ind}"
                description = f"{vdescription}, {cdescription}"

                #fetch categories c1
                if (cl1:= data[t]["c1"]) is not None:
                    classe1 = soup.select(f"span#lstClassificacoes_lblIdClassificacao_{1}")[0].get_text()
                    #fetch classes
                    if cl1 == "":
                        nclasse1 = range(0,int(re.search("\d+",
                                                         soup.select(f"span#lstClassificacoes_lblQuantidadeCategorias_{1}")[0].get_text()).group()))
                    else:
                        nclasse1 = cl1
                    for c1 in nclasse1:
                        tag_id = f"span#lstClassificacoes_lstCategorias_{1}_lblIdCategoria_{c1}"
                        tag_name = f"span#lstClassificacoes_lstCategorias_{1}_lblNomeCategoria_{c1}"
                        c1description = soup.select(tag_name)[0].get_text()
                        ind1 = soup.select(tag_id)[0].get_text()
                        description = f"{cdescription}, {c1description}, {vdescription}"
                        ticker = f"IBGE.{t}/p/all/v/{vnumber}/c{classe}/{ind}/c{classe1}/{ind1}"
                        series.append([ticker, description, "IBGE", data[t]["s"], "SERIES-TEMPORAIS"]) #, description, "Brasil", "IBGE"])
                else:
                    series.append([ticker, description, "IBGE", data[t]["s"], "SERIES-TEMPORAIS"]) #, description, "Brasil", "IBGE"])
        else:
            ticker = f"IBGE.{t}/p/all/v/{vnumber}"
            description = f"{vdescription}"
            series.append([ticker, description, "IBGE", data[t]["s"], "SERIES-TEMPORAIS"])
        
print(f"done fetching {len(series)} series information!")        


[add_series(*s) for s in series]
# with ThreadPoolExecutor() as executor:
#     executor.map(lambda s: add_series(*s), series)
    
print("series added to database")    
