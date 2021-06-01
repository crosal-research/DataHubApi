#! /usr/bin/
date_initial="2021-03-01"    # initial date
date_final="2021-04-01"      # final date


# add inflation series
# for inf in "IPCA" "IPCA15"; do
#     python3 -m DB.loaders.inflation.inflation_obs $inf $date_initial $date_final
# done


# add core series
# for inf in "IPCA" "IPCA15"; do
#     python3 -m DB.loaders.inflation.nucleos_calculos $inf $date_initial $date_final
# done

# add fred series
python3 -m DB.loaders.fetch_obs "FRED" "SERIES-TEMPORAIS"
# python3 -m DB.transactions
