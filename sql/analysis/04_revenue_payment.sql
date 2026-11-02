-- Revenue components. These components are reported separately:
-- do not assume their sum must equal total_amount for every TLC record.
SELECT
    SUM(fare_amount) AS fare_revenue,
    SUM(extra) AS extras,
    SUM(mta_tax) AS mta_tax,
    SUM(tip_amount) AS tips,
    SUM(tolls_amount) AS tolls,
    SUM(improvement_surcharge) AS improvement_surcharge,
    SUM(congestion_surcharge) AS congestion_surcharge,
    SUM(airport_fee) AS airport_fees,
    SUM(total_amount) AS total_amount
FROM fact_trips;

-- Payment method usage and value
SELECT p.payment_description,
       COUNT(*) AS trips,
       SUM(f.total_amount) AS total_revenue,
       AVG(f.total_amount) AS average_trip_total,
       AVG(f.tip_amount) AS average_tip
FROM fact_trips f
LEFT JOIN dim_payment p ON f.payment_key = p.payment_key
GROUP BY 1
ORDER BY trips DESC;

-- Tip rate among positive-fare trips
SELECT p.payment_description,
       COUNT(*) AS trips,
       AVG(f.tip_amount / NULLIF(f.fare_amount,0)) FILTER (WHERE f.fare_amount > 0) AS average_tip_to_fare_ratio
FROM fact_trips f
LEFT JOIN dim_payment p ON f.payment_key = p.payment_key
GROUP BY 1
ORDER BY trips DESC;
