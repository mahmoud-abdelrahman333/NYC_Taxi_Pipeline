-- Top pickup zones
SELECT l.borough, l.zone, COUNT(*) AS trips,
       SUM(f.total_amount) AS revenue,
       AVG(f.total_amount) AS average_trip_total
FROM fact_trips f
JOIN dim_location l ON f.pickup_location_key = l.location_key
GROUP BY 1,2
ORDER BY trips DESC
LIMIT 20;

-- Top dropoff zones
SELECT l.borough, l.zone, COUNT(*) AS trips,
       SUM(f.total_amount) AS revenue
FROM fact_trips f
JOIN dim_location l ON f.dropoff_location_key = l.location_key
GROUP BY 1,2
ORDER BY trips DESC
LIMIT 20;

-- Most frequent pickup/dropoff pairs
SELECT p.borough AS pickup_borough, p.zone AS pickup_zone,
       d.borough AS dropoff_borough, d.zone AS dropoff_zone,
       COUNT(*) AS trips,
       AVG(f.total_amount) AS average_trip_total
FROM fact_trips f
JOIN dim_location p ON f.pickup_location_key = p.location_key
JOIN dim_location d ON f.dropoff_location_key = d.location_key
GROUP BY 1,2,3,4
ORDER BY trips DESC
LIMIT 30;
