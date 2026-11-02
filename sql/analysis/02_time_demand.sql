-- Daily demand and revenue
SELECT pickup_datetime::date AS trip_date,
       COUNT(*) AS trips,
       SUM(total_amount) AS revenue,
       AVG(total_amount) AS average_trip_total
FROM fact_trips
GROUP BY 1
ORDER BY 1;

-- Demand by weekday and hour
SELECT TO_CHAR(pickup_datetime, 'Dy') AS weekday,
       EXTRACT(ISODOW FROM pickup_datetime)::int AS weekday_number,
       EXTRACT(HOUR FROM pickup_datetime)::int AS pickup_hour,
       COUNT(*) AS trips,
       SUM(total_amount) AS revenue
FROM fact_trips
GROUP BY 1,2,3
ORDER BY 2,3;

-- Weekday versus weekend
SELECT CASE WHEN EXTRACT(ISODOW FROM pickup_datetime) IN (6,7)
            THEN 'Weekend' ELSE 'Weekday' END AS day_type,
       COUNT(*) AS trips,
       SUM(total_amount) AS revenue,
       AVG(total_amount) AS average_trip_total
FROM fact_trips
GROUP BY 1
ORDER BY 1;
