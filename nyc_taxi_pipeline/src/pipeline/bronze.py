import dlt



# only to get the landing paths
def get_landing_path(month: str) -> str:
    return f"/Volumes/workspace/default/my-vol/nyc-taxi/yellow_tripdata_2026_{month}"


#  Bronze layer tables


@dlt.tables(
    name = "bronze_yellow_trips",
    comment = "raw data from nyc dataset"
    table_properties = {

    }
)
def bronze_yellow_trips() --> Any:
    # read files from our volume and then drop _ingest_file_name, timestamp
    pass

    