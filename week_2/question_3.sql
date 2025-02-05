SELECT COUNT(*)
FROM public.yellow_tripdata
WHERE EXTRACT(YEAR FROM tpep_pickup_datetime) = 2020;



