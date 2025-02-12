-- Load data from external table into a native table
CREATE OR REPLACE TABLE `ny-taxi-450717.yellow_taxi_data.yellow_trips` AS
SELECT * FROM `ny-taxi-450717.yellow_taxi_data.external_yellow_trips`;

-- Create a materialized view on the native table
CREATE MATERIALIZED VIEW `ny-taxi-450717.yellow_taxi_data.materialized_yellow_trips`
AS
SELECT * FROM `ny-taxi-450717.yellow_taxi_data.yellow_trips`;