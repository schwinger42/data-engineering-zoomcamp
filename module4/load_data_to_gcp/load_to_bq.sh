bq load \
  --source_format=CSV \
  --autodetect \
  ny-taxi-450717:trips_data_all.fhv_tripdata \
  "gs://fhv_trip/fhv_tripdata_*.csv.gz"
