SELECT
    DISTINCT VendorID
FROM
    `ny-taxi-450717.yellow_taxi_data.materialized_yellow_trips`
WHERE
    DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

-- partitioned and clustered table
SELECT
    DISTINCT VendorID
FROM
    `ny-taxi-450717.yellow_taxi_data.yellow_trips_optimized`
WHERE
    DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';