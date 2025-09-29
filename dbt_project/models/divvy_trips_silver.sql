
-- This model reads from the bronze source and creates the silver table
SELECT
    trip_id,
    CAST(start_time AS timestamp) AS start_time,
    CAST(end_time AS timestamp) AS end_time,
    bikeid,
    (CAST(regexp_replace(tripduration, ',', '') AS double) / 60) AS tripduration_minutes,
    from_station_name,
    to_station_name,
    usertype,
    gender,
    CAST(birthyear AS int) AS birthyear,
    source_file
FROM {{ source('divvy_source', 'divvy_trips_bronze') }}
-- This is our data quality rule, now implemented in SQL
WHERE (CAST(regexp_replace(tripduration, ',', '') AS double) / 60) > 0
