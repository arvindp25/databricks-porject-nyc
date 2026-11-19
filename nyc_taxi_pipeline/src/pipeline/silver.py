import dlt
from pyspark.sql.functions import col,lit,create_map

@dlt.table(
    name="silver_yellow_trips",
    comment = "Cleansed and validated NYC yellow Taxi dta",
    table_properties = {
        "layer": "silver",
        "delta.feature.timestampNtz": "supported"
    }
)
def silver_yellow_trips():
    df = spark.readStream.table("bronze_yellow_trips")
    df = df.filter(
            col("tpep_pickup_datetime").isNotNull()
            & col("tpep_dropoff_datetime").isNotNull()
            & (col("trip_distance") > 0)
            & (col("fare_amount") >= 0)
            & (col("tpep_dropoff_datetime") > col("tpep_pickup_datetime"))
    )
    return df


@dlt.table(
    name="silver_payment_type_taxanomy",
    comment = "Cleansed and validated NYC yellow Taxi dta",
    table_properties = {
        "layer": "silver",
        "delta.feature.timestampNtz": "supported"
    }
)
def silver_payment_type_taxanomy():

    payment_data = [
        (0, "Flex Fare trip"),
        (1, "Credit card"),
        (2, "Cash"),
        (3, "No charge"),
        (4, "Dispute"),
        (5, "Unknown"),
        (6, "Voided trip")
    ]

    payment_df = spark.createDataFrame(
        payment_data,
        ["payment_type", "payment_type_desc"]
    )
    return payment_df
