# import from app
from DB.transactions import add_series


for cpi in ["IPCA", "IPCA15"]:
    nucleos = {f"{cpi}.core_EXO": 
              ["nucleo exclusão, EX0",
               "IBGE",
               cpi,
               "Inflacao",
               "nucleo",
               "variacao"],
              f"{cpi}.core_EX1": 
              ["nucleo exclusão, EX1",
               "IBGE",
               cpi,
               "Inflacao",
               "nucleo",
               "variacao"],
              f"{cpi}.core_EX2": 
              ["nucleo exclusão, EX2",
               "IBGE",
               cpi,
               "Inflacao",
               "nucleo",
               "variacao"],
              f"{cpi}.core_EX3": 
              ["nucleo exclusão, EX3",
               "IBGE",
               cpi,
               "Inflacao",
               "nucleo",
               "variacao"],
               f"{cpi}.servicos": 
              [" Items servicos",
               "IBGE",
               cpi,
               "Inflacao",
               "nucleo",
               "variacao"],
              f"{cpi}.core_servicos": 
              ["nucleo servicos",
               "IBGE",
               cpi,
               "Inflacao",
               "nucleo",
               "variacao"],
              f"{cpi}.core_duraveis": 
              ["nucleo duravies",
               "IBGE",
               cpi,
               "Inflacao",
               "nucleo",
               "variacao"],
              f"{cpi}.core_tradables": 
              ["nucleo comercializaveis",
               "IBGE",
               cpi,
               "Inflacao",
               "nucleo",
               "variacao"],
              f"{cpi}.core_difusao": 
              ["nucleo difusao",
               "IBGE",
               cpi,
               "Inflacao",
               "nucleo",
               "variacao"],
              f"{cpi}.core_p55": 
              ["nucleo p55, Banco Central",
               "IBGE",
               cpi,
               "Inflacao",
               "nucleo",
               "variacao"],
              f"{cpi}.core_aparadas": 
              ["nucleo de media aparadas, Banco Central",
               "IBGE",
               cpi,
               "Inflacao",
               "nucleo",
               "variacao"],
              f"{cpi}.core_aparadas_suavizadas": 
              ["nucleo de media aparadas suavizadas",
               "IBGE",
               cpi,
               "Inflacao",
               "nucleo",
               "variacao"],
              f"{cpi}.core_dp": 
              ["nucleo de dupla ponderacao",
               "IBGE",
               cpi,
               "Inflacao",
               "nucleo",
               "variacao"],
              f"{cpi}.core_monitorados": 
              ["nucleo bens monitorados",
               "IBGE",
               cpi,
               "Inflacao",
               "nucleo",
               "variacao"],
               f"{cpi}.core_livres": 
              ["nucleo bens livres",
               "IBGE",
               cpi,
               "Inflacao",
               "nucleo",
               "variacao"],
               f"{cpi}.core_industriais": 
              ["nucleo bens Industriais",
               "IBGE",
               cpi,
               "Inflacao",
               "nucleo",
               "variacao"]}
    for k in nucleos:
        add_series(*[n.upper() for n in [k, *nucleos[k]]])
