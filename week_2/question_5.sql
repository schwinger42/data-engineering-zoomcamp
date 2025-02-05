SELECT COUNT(*)
FROM public.yellow_tripdata
WHERE EXTRACT(YEAR FROM tpep_pickup_datetime) = 2021
  AND EXTRACT(MONTH FROM tpep_pickup_datetime) = 3;