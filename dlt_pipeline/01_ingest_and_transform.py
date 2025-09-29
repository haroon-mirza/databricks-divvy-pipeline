import dlt
import requests
import zipfile
import os
from io import BytesIO
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, expr
from functools import reduce

CATALOG_NAME = "dev_catalog"
SCHEMA_NAME = "divvy_analytics"
VOLUME_NAME = "raw_divvy_files"
VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{VOLUME_NAME}"

DOWNLOAD_URIS = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip"
]

FINAL_BRONZE_COLUMNS = [
    "trip_id", "start_time", "end_time", "bikeid", "tripduration",
    "from_station_name", "to_station_name", "usertype", "gender", "birthyear",
    "source_file"
]

def standardize_columns(df, source_file_name):
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

def download_and_process_files():
    os.makedirs(VOLUME_PATH, exist_ok=True)
    for uri in DOWNLOAD_URIS:
        try:
            response = requests.get(uri, timeout=60)
            response.raise_for_status()
            with zipfile.ZipFile(BytesIO(response.content)) as z:
                csv_filename = [f for f in z.namelist() if f.endswith('.csv') and not f.startswith('__MACOSX')][0]
                csv_content_bytes = z.read(csv_filename)
                temp_file_path = os.path.join(VOLUME_PATH, csv_filename)
                with open(temp_file_path, "wb") as f:
                    f.write(csv_content_bytes)
                df = spark.read.csv(temp_file_path, header=True, inferSchema=True)
                source_file_name = uri.split('/')[-1]
                yield standardize_columns(df, source_file_name)
        except Exception as e:
            print(f"CRITICAL ERROR processing {uri}.")
            raise e

@dlt.table(
    name="divvy_trips_bronze",
    comment="Standardized raw data."
)
def divvy_trips_bronze():
    df_list = list(download_and_process_files())
    if not df_list:
        return spark.createDataFrame([])
    return reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), df_list)