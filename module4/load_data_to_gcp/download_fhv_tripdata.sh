#!/bin/bash

# define URL
BASE_URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-"

# download files
for month in {01..12}; do
  aria2c -x 16 -s 16 -j 1 "${BASE_URL}${month}.csv.gz" -o "fhv_tripdata_2019-${month}.csv.gz" &
done
wait
