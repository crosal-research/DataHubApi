# import from app
from DB.transactions import add_series


for cpi in ["IPCA", "IPCA15"]:
    nucleos = {f"{cpi}.core_EXO": 
              ["nucleo exclusão, EX0",
               "IBGE",
               "Inflacao",
               "nucleo",
               "variacao",
               f"{cpi}"],
              f"{cpi}.core_EX1": 
              ["nucleo exclusão, EX1",
               "IBGE",
               "Inflacao",
               "nucleo",
               "variacao",
               f"{cpi}"],
              f"{cpi}.core_EX2": 
              ["nucleo exclusão, EX2",
               "IBGE",
               "Inflacao",
               "nucleo",
               "variacao",
               f"{cpi}"],
              f"{cpi}.core_EX3": 
              ["nucleo exclusão, EX3",
               "IBGE",
               "Inflacao",
               "nucleo",
               "variacao",
               f"{cpi}"],
               f"{cpi}.servicos": 
              [" Items servicos",
               "IBGE",
               "Inflacao",
               "nucleo",
               "variacao",
               f"{cpi}"],
              f"{cpi}.core_servicos": 
              ["nucleo servicos",
               "IBGE",
               "Inflacao",
               "nucleo",
               "variacao",
               f"{cpi}"],
              f"{cpi}.core_duraveis": 
              ["nucleo duravies",
               "IBGE",
               "Inflacao",
               "nucleo",
               "variacao",
               f"{cpi}"],
              f"{cpi}.core_tradables": 
              ["nucleo comercializaveis",
               "IBGE",
               "Inflacao",
               "nucleo",
               "variacao",
               f"{cpi}"],
              f"{cpi}.core_difusao": 
              ["nucleo difusao",
               "IBGE",
               "Inflacao",
               "nucleo",
               "variacao",
               f"{cpi}"],
              f"{cpi}.core_p55": 
              ["nucleo p55, Banco Central",
               "IBGE",
               "Inflacao",
               "nucleo",
               "variacao",
               f"{cpi}"],
              f"{cpi}.core_aparadas": 
              ["nucleo de media aparadas, Banco Central",
               "IBGE",
               "Inflacao",
               "nucleo",
               "variacao",
               f"{cpi}"],
              f"{cpi}.core_aparadas_suavizadas": 
              ["nucleo de media aparadas suavizadas",
               "IBGE",
               "Inflacao",
               "nucleo",
               "variacao",
               f"{cpi}"],
              f"{cpi}.core_dp": 
              ["nucleo de dupla ponderacao",
               "IBGE",
               "Inflacao",
               "nucleo",
               "variacao",
               f"{cpi}"], 
              f"{cpi}.core_monitorados": 
              ["nucleo bens monitorados",
               "IBGE",
               "Inflacao",
               "nucleo",
               "variacao",
               f"{cpi}"],
               f"{cpi}.core_livres": 
              ["nucleo bens livres",
               "IBGE",
               "Inflacao",
               "nucleo",
               "variacao",
               f"{cpi}"], 
               f"{cpi}.core_industriais": 
              ["nucleo bens Industriais",
               "IBGE",
               "Inflacao",
               "nucleo",
               "variacao",
               f"{cpi}"]}
    for k in nucleos:
        add_series(*[n.upper() for n in [k, *nucleos[k]]])
