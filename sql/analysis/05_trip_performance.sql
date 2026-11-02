-- Duration and speed summaries. Excludes zero/negative distance and invalid duration.
WITH valid_trips AS (
    SELECT trip_distance,
           EXTRACT(EPOCH FROM (dropoff_datetime-pickup_datetime))/60.0 AS duration_minutes,
           fare_amount, total_amount, passenger_count
    FROM fact_trips
    WHERE trip_distance > 0
      AND dropoff_datetime > pickup_datetime
      AND EXTRACT(EPOCH FROM (dropoff_datetime-pickup_datetime)) BETWEEN 60 AND 21600
)
SELECT COUNT(*) AS valid_trips,
       AVG(duration_minutes) AS average_duration_minutes,
       PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY duration_minutes) AS median_duration_minutes,
       AVG(trip_distance) AS average_distance_miles,
       PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY trip_distance) AS median_distance_miles,
       AVG(trip_distance / NULLIF(duration_minutes/60.0,0)) AS average_speed_mph,
       AVG(fare_amount / NULLIF(trip_distance,0)) AS average_fare_per_mile
FROM valid_trips;

-- Passenger count distribution
SELECT passenger_count, COUNT(*) AS trips,
       AVG(total_amount) AS average_total
FROM fact_trips
GROUP BY 1
ORDER BY 1;

-- Potential outliers for review, not automatic proof of bad data
SELECT trip_key, pickup_datetime, dropoff_datetime, trip_distance,
       fare_amount, total_amount,
       EXTRACT(EPOCH FROM (dropoff_datetime-pickup_datetime))/60.0 AS duration_minutes
FROM fact_trips
WHERE trip_distance > 100
   OR total_amount > 1000
   OR dropoff_datetime <= pickup_datetime
ORDER BY total_amount DESC
LIMIT 200;
