
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE UNIQUE NOT NULL,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    day INTEGER,
    day_name VARCHAR(15)
);

CREATE TABLE IF NOT EXISTS dim_location (
    location_key INTEGER PRIMARY KEY,
    locationid INTEGER UNIQUE NOT NULL,
    borough VARCHAR(100),
    zone VARCHAR(150),
    service_zone VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS dim_payment (
    payment_key INTEGER PRIMARY KEY,
    payment_type INTEGER UNIQUE,
    payment_description VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS dim_rate (
    rate_key INTEGER PRIMARY KEY,
    ratecodeid INTEGER UNIQUE,
    rate_description VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS dim_vendor (
    vendor_key INTEGER PRIMARY KEY,
    vendorid INTEGER UNIQUE,
    vendor_name VARCHAR(150)
);

CREATE TABLE IF NOT EXISTS fact_trips (
    trip_key VARCHAR(64) PRIMARY KEY,

    pickup_date_key INTEGER REFERENCES dim_date(date_key),
    dropoff_date_key INTEGER REFERENCES dim_date(date_key),

    pickup_location_key INTEGER REFERENCES dim_location(location_key),
    dropoff_location_key INTEGER REFERENCES dim_location(location_key),

    payment_key INTEGER REFERENCES dim_payment(payment_key),
    rate_key INTEGER REFERENCES dim_rate(rate_key),
    vendor_key INTEGER REFERENCES dim_vendor(vendor_key),

    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,

    passenger_count INTEGER,
    trip_distance DOUBLE PRECISION,

    fare_amount DOUBLE PRECISION,
    extra DOUBLE PRECISION,
    mta_tax DOUBLE PRECISION,
    tip_amount DOUBLE PRECISION,
    tolls_amount DOUBLE PRECISION,
    improvement_surcharge DOUBLE PRECISION,
    total_amount DOUBLE PRECISION,
    congestion_surcharge DOUBLE PRECISION,
    airport_fee DOUBLE PRECISION,

    store_and_fwd_flag VARCHAR(10)
);
