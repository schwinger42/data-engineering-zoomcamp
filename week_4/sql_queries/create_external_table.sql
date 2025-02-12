CREATE OR REPLACE EXTERNAL TABLE `ny-taxi-450717.yellow_taxi_data.external_yellow_trips`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dezoomcamp_hw3_2025_schwinger42/yellow_tripdata_2024-*.parquet']
);
