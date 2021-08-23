from DB.transactions import add_source
# IBGE
add_source("IBGE",  "Instituto Brasileiro de Geografia e Estatistica, Pesquisa IPCA", "IPCA", "Inflacao")
add_source("IBGE",  "Instituto Brasileiro de Geografia e Estatistica, Pesquisa IPCA-15", "IPCA15","Inflacao")
add_source("IBGE",  "Nucleos do IPCA, calculados na casa", "CORES", "Inflacao")
add_source("IBGE",  "Nucleos do IPCA-15, calculados na casa", "CORES15", "Inflacao")
add_source("FRED",  "Federal Research Economic Data", "FRED-FIN", "FINANCE")
add_source("FRED",  "Federal Research Economic Data", "ECON", "Series-temporais")
add_source("IBGE",  "Instituto Brasiliero de Geografia e Estatistica, Contas Nacionais", "CN", "Series-temporais")
add_source("IBGE",  "Instituto Brasiliero de Geografia e Estatistica, Pesquisa Industrial Mensal", "PIM", "Series-temporais")
add_source("IBGE",  "Instituto Brasiliero de Geografia e Estatistica, Pesquisa Mensal do Comercio", "PMC", "Series-temporais")
add_source("IBGE",  "Instituto Brasiliero de Geografia e Estatistica, Pesquisa Mensal de Servicos", "PMS", "Series-temporais")
add_source("IBGE",  "Instituto Brasiliero de Geografia e Estatistica, Pesquisa Nacional por Amostra de Domicílios", "PNAD", "Series-temporais")

# BCB
add_source("BCB", "Banco Central do Brasil, Dados de Vencimentos dos Títulos Públicos", "VENC", "TITULOS-PUBLICOS")
add_source("BCB", "Banco Central do Brasil, Estatísticas Fiscais", "FISCAL", "SERIES-TEMPORAIS")
add_source("BCB", "Banco Central do Brasil, Estatísticas do Setor Externo", "SETOR-EXTERNO", "SERIES-TEMPORAIS")
add_source("BCB", "Banco Central do Brasil, Estatísticas Monetárias e de Crétito", "CREDITO/PMON", "SERIES-TEMPORAIS")
add_source("BCB", "Banco Central do Brasil, Estatisticas do Mercado Aberto", "MERCADO-ABERTO", "TITULOS-PUBLICOS")
add_source("BCB", "Banco Central do Brasil, Estatísticas Econômicas", "ECON", "SERIES-TEMPORAIS")
add_source("BCB", "Banco Central do Brasil, Dados de Estabilidade Finaceira", "ESTABILIDADE/FIN", "SERIES-TEMPORAIS")


#IPEA
add_source("IPEA",  "Instituto de Pesquisa de Economia Aplicada", "ECON", "SERIES-TEMPORAIS")
add_source("IPEA",  "Instituto de Pesquisa de Economia Aplicada", "FIN", "FINANCE") # MUDAR ISSO de SERIES-TEMPORAIS para FINANCE


#CEPEA
add_source("CEPEA", "Centro de Estudos Avançados em Economia Aplicada", "AGRI", "SERIES-TEMPORAIS")


#Comex
add_source("COMEX", "Ministerio da Industrial, Comercio Exterior e Serviços", "ECON", "SERIES-TEMPORAIS")


# STN
add_source("STN", "Secretaria do Tesouro Nacional, Resultado Fiscal", "RES-FISCAL", "Series-Temporais")


# YAHOO
add_source("YAHOO", "Preços do Mercado Futuros, Yahoo", "Yahoo-Futures", "Finance")

# BEA
