-- Executive overview: one row for the full January 2024 dataset.
SELECT
    COUNT(*) AS total_trips,
    SUM(total_amount) AS gross_revenue,
    AVG(total_amount) AS average_total_per_trip,
    AVG(fare_amount) AS average_fare,
    AVG(trip_distance) AS average_distance_miles,
    AVG(EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime))/60.0) AS average_duration_minutes,
    AVG(tip_amount) AS average_tip,
    SUM(tip_amount) AS total_tips
FROM fact_trips;
