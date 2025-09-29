
-- This model reads from the silver table to create the gold aggregation
SELECT
    usertype,
    CAST(start_time AS date) AS trip_date,
    count(*) AS total_trips
FROM {{ ref('divvy_trips_silver') }}
GROUP BY 1, 2
