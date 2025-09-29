# In tests/test_transformations.py

import pytest
from pyspark.sql import SparkSession

# The import will now work because the runner notebook is setting the path.
from dlt_pipeline.ingest_and_transform import standardize_columns

def test_standardize_columns_handles_messy_schema(spark):
    """
    Tests if the function correctly renames the messy 2019_Q2 schema.
    """
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

    standardized_df = standardize_columns(messy_df, "messy_file.zip")
    
    final_columns = standardized_df.columns
    assert "trip_id" in final_columns
    assert "start_time" in final_columns
    assert "01 - Rental Details Rental ID" not in final_columns
    assert standardized_df.count() == 1
    assert standardized_df.first()["trip_id"] == "id_123"