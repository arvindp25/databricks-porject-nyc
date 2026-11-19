import dlt
from pyspark.sql.functions import col,current_timestamp
from typing import Any


LANDING_PATH = "/Volumes/workspace/default/my-vol/nyc-taxi/"


#  Bronze layer tables


@dlt.table(
    name = "bronze_yellow_trips",
    comment = "raw data from nyc dataset",
    table_properties = {
        "layer": "bronze",
        "delta.feature.timestampNtz": "supported"
    }
)
def bronze_yellow_trips() -> Any:
    # read files from our volume and then  _ingest_file_name, timestamp
    df = spark.readStream.format("cloudfiles").option("cloudfiles.format", "parquet").option("pathGlobFilter", "yellow_tripdata_2026-*.parquet").load(LANDING_PATH)
    df = df.withColumn("_ingest_file_name", col("_metadata.file_path")).withColumn("_ingest_timestamp", current_timestamp())

    return df

