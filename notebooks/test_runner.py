# Databricks notebook source
# This entire script runs in a single notebook cell.

from pyspark.sql import Row
from pyspark.sql.functions import col, lit
import pytest # We can still use pytest's features, just run it as a normal function

# --- 1. Copy the function you want to test directly into this notebook ---

FINAL_BRONZE_COLUMNS = [
    "trip_id", "start_time", "end_time", "bikeid", "tripduration",
    "from_station_name", "to_station_name", "usertype", "gender", "birthyear",
    "source_file"
]

def standardize_columns(df, source_file_name):
    """
    Takes a DataFrame with an inconsistent schema and conforms it to the standard format.
    """
    rename_map = {
        "01 - Rental Details Rental ID": "trip_id", "ride_id": "trip_id",
        "01 - Rental Details Local Start Time": "start_time", "started_at": "start_time",
        "01 - Rental Details Local End Time": "end_time", "ended_at": "end_time",
        "01 - Rental Details Bike ID": "bikeid",
        "01 - Rental Details Duration In Seconds UNCAPPED": "tripduration",
        "03 - Rental Start Station Name": "from_station_name", "start_station_name": "from_station_name",
        "02 - Rental End Station Name": "to_station_name", "end_station_name": "to_station_name",
        "User Type": "usertype", "member_casual": "usertype",
        "Member Gender": "gender",
        "05 - Member Details Member Birthday Year": "birthyear"
    }
    for old_name, new_name in rename_map.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)
    if "tripduration" not in df.columns and "end_time" in df.columns and "start_time" in df.columns:
        df = df.withColumn("tripduration", (col("end_time").cast("long") - col("start_time").cast("long")).cast("string"))
    for col_name in FINAL_BRONZE_COLUMNS:
        if col_name not in df.columns:
            df = df.withColumn(col_name, lit(None).cast("string"))
    df = df.withColumn("source_file", lit(source_file_name))
    return df.select(*FINAL_BRONZE_COLUMNS)

# --- 2. Define and run your test function ---

def test_standardize_columns_handles_messy_schema():
    """
    Tests if the function correctly renames a messy schema.
    The 'spark' session is automatically available in Databricks notebooks.
    """
    print("--- Setting up test data ---")
    messy_schema_data = [(
        "id_123", "2019-04-01 12:00:00", "2019-04-01 12:30:00", "bike_abc",
        "1800", "Messy Station A", "Messy Station B", "Customer", "Female", "1990"
    )]
    messy_df = spark.createDataFrame(messy_schema_data, [
        "01 - Rental Details Rental ID", "01 - Rental Details Local Start Time",
        "01 - Rental Details Local End Time", "01 - Rental Details Bike ID",
        "01 - Rental Details Duration In Seconds UNCAPPED", "03 - Rental Start Station Name",
        "02 - Rental End Station Name", "User Type", "Member Gender",
        "05 - Member Details Member Birthday Year"
    ])

    print("--- Running transformation function ---")
    standardized_df = standardize_columns(messy_df, "messy_file.zip")
    
    print("--- Running assertions ---")
    final_columns = standardized_df.columns
    assert "trip_id" in final_columns
    assert "start_time" in final_columns
    assert "01 - Rental Details Rental ID" not in final_columns
    assert standardized_df.count() == 1
    assert standardized_df.first()["trip_id"] == "id_123"
    
    print("\n--- ✅ All tests passed successfully! ---")

# --- 3. Execute the test ---
test_standardize_columns_handles_messy_schema()