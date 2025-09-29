# Databricks notebook source
# MAGIC %run "/Users/amirzaharoon@gmail.com/divvy_dlt_pipeline/divvy_pipeline_source/notebooks/_config"

# COMMAND ----------

import os

CATALOG_NAME = "dev_catalog"
SCHEMA_NAME = "divvy_analytics"

host = spark.conf.get("spark.databricks.workspaceUrl")

profiles_dir = os.path.expanduser("~/.dbt")
os.makedirs(profiles_dir, exist_ok=True)

profiles_yml_path = os.path.join(profiles_dir, "profiles.yml")

with open(profiles_yml_path, "w") as f:
    f.write(f"""
dbt_project:
  outputs:
    dev:
      type: databricks
      host: {host}
      http_path: {db_http_path}
      token: {db_token}
      catalog: {CATALOG_NAME}
      schema: {SCHEMA_NAME}
      threads: 4
  target: dev
""")

print(f"dbt profiles.yml file created successfully at {profiles_yml_path}")

# COMMAND ----------

models_dir = "../dbt_project/models"
os.makedirs(models_dir, exist_ok=True)

sources_yml_content = """
version: 2
sources:
  - name: divvy_source
    database: dev_catalog
    schema: divvy_analytics
    tables:
      - name: divvy_trips_bronze
"""

silver_sql_content = """
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
"""

gold_sql_content = """
-- This model reads from the silver table to create the gold aggregation
SELECT
    usertype,
    CAST(start_time AS date) AS trip_date,
    count(*) AS total_trips
FROM {{ ref('divvy_trips_silver') }}
GROUP BY 1, 2
"""

schema_yml_content = """
version: 2
models:
  - name: divvy_trips_silver
    columns:
      - name: trip_id
        tests:
          - unique
          - not_null
  - name: trips_by_usertype_gold
    columns:
      - name: total_trips
        tests:
          - not_null
"""

with open(os.path.join(models_dir, "sources.yml"), "w") as f:
    f.write(sources_yml_content)

with open(os.path.join(models_dir, "divvy_trips_silver.sql"), "w") as f:
    f.write(silver_sql_content)

with open(os.path.join(models_dir, "trips_by_usertype_gold.sql"), "w") as f:
    f.write(gold_sql_content)

with open(os.path.join(models_dir, "schema.yml"), "w") as f:
    f.write(schema_yml_content)

print("All dbt model and schema files created successfully.")

# COMMAND ----------

# MAGIC %sh
# MAGIC cd ../dbt_project && dbt run

# COMMAND ----------

# MAGIC %sh
# MAGIC cd ../dbt_project && dbt test