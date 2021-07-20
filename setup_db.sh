#! /usr/bin/

# sets up the database
echo "Sets up the database"
python3 -m DB.db
echo "DB setup"
echo -e "-----\n"

# # sets up series into the Database
echo "Building up inflation source..."
python3 -m DB.sources.sources
echo "Done with source"
echo -e "--------\n"


# Series
# # sets up series into the Database
echo "Building up series..."
python3 -m DB.loaders.fred.fred_series_add
echo -e "---"
python3 -m DB.loaders.bcb_vencimentos.bcb_vencimentos_series_add
echo -e "---"
python3 -m DB.loaders.ibge.ibge_series_add
echo -e "---"
python3 -m DB.loaders.inflation.inflation_series_add
echo -e "---"
python3 -m DB.loaders.inflation.nucleos_series_add
echo -e "---"
python3 -m DB.loaders.bcb.bcb_series_add
echo -e "---"
python3 -m DB.loaders.stn.stn_series_add
echo -e "---"
python3 -m DB.loaders.cepea.cepea_series_add
echo -e "---"
python3 -m DB.loaders.comex.comex_series_add
echo -e "---"
python3 -m DB.loaders.ipea.ipea_series_add
echo -e "---"
python3 -m DB.loaders.yahoo.yahoo_series_add
echo "Done with series"
echo -e "--------\n"

# Observations
echo "Loading observations..."
python3 -m DB.loaders.fetch_obs
echo "Done with Observations!"
echo -e "--------\n"


# Tables
# sets up of basic tables into the Database
echo "Build up built-in tables..."
python3 -m DB.loaders.inflation.add_tables
echo "Done with tables\n"

echo -e "--------\n"
echo "Done with setting up!"
