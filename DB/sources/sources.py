from DB.transactions import add_source
add_source("IBGE",  "Instituto Brasileiro de Geografia e Estatistica, Pesquisa IPCA", "IPCA", "Inflacao")
add_source("IBGE",  "Instituto Brasileiro de Geografia e Estatistica, Pesquisa IPCA-15", "IPCA15","Inflacao")
add_source("IBGE",  "Nucleos do IPCA, calculados na casa", "CORES", "Inflacao")
add_source("IBGE",  "Nucleos do IPCA-15, calculados na casa", "CORES15", "Inflacao")
add_source("FRED",  "Federal Research Economic Data", "FIN", "Series-temporais")
add_source("FRED",  "Federal Research Economic Data", "ECON", "Series-temporais")
add_source("IBGE",  "Instituto Brasiliero de Geografia e Estatistica, Contas Nacionais", "CN", "Series-temporais")
add_source("IBGE",  "Instituto Brasiliero de Geografia e Estatistica, Pesquisa Industrial Mensal", "PIM", "Series-temporais")
add_source("IBGE",  "Instituto Brasiliero de Geografia e Estatistica, Pesquisa Mensal do Comercio", "PMC", "Series-temporais")
add_source("IBGE",  "Instituto Brasiliero de Geografia e Estatistica, Pesquisa Nacional por Amostra de Domicílios", "PNAD", "Series-temporais")
add_source("BCB", "Banco Central do Brasil, Dados de Vencimentos dos Títulos Públicos", "VENC", "vencimentos")

