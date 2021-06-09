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
# Inflation Database
# # sets up series into the Database
echo "Building up series..."
python3 -m DB.loaders.fred.fred_series_add
echo -e "---"
python3 -m DB.loaders.bcb_vencimentos.bcb_vencimentos_series_add
echo -e "---"
python3 -m DB.loaders.ibge.ibge_series_add
echo -e "---"
python3 -m DB.loaders.inflation.series_add
echo -e "---"
python3 -m DB.loaders.inflation.nucleos_series_add
echo "Done with series"
echo -e "--------\n"


# Observations
echo "Loading observations..."
python3 -m DB.loaders.fetch_obs
echo "Done with setting up!"
echo -e "--------\n"


# Tables
# # sets up of basic tables into the Database
echo "Build up built-in tables..."
python3 -m DB.loaders.inflation.add_tables
echo "Done with cores"
echo -e "--------\n"
echo "Done with setting up!"
