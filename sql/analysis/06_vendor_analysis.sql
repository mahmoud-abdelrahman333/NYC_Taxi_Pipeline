SELECT v.vendor_name,
       COUNT(*) AS trips,
       SUM(f.total_amount) AS total_revenue,
       AVG(f.total_amount) AS average_trip_total,
       AVG(f.trip_distance) AS average_distance,
       AVG(f.tip_amount) AS average_tip,
       AVG(EXTRACT(EPOCH FROM (f.dropoff_datetime-f.pickup_datetime))/60.0) AS average_duration_minutes
FROM fact_trips f
LEFT JOIN dim_vendor v ON f.vendor_key = v.vendor_key
GROUP BY 1
ORDER BY total_revenue DESC;
