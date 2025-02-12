CREATE OR REPLACE TABLE `ny-taxi-450717.yellow_taxi_data.yellow_trips_optimized`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
AS
SELECT *
FROM `ny-taxi-450717.yellow_taxi_data.yellow_trips`;
