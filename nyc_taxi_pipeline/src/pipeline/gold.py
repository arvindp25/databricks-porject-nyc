# monhtly revenue.
import dlt
from pyspark.sql import functions as F

@dlt.table(name="gold_monthly_revenue",
           comment = "Monthly taxi revenue and trip summary",
           table_properties ={"layer": "gold",
                              "delta.feature.timestampNtz": "supported"
                              }
           )
def gold_monthly_revenue():
    df = spark.readStream.table("silver_yellow_trips")
    return ( df.withColumn("trip_month", F.date_format("tpep_pickup_datetime", "yyyy-MM")) 
    .groupBy("trip_month").agg(F.count("*").alias("total_trips"), 
                                F.sum(F.col('fare_amount')).alias("total_fare"),
                                F.sum(F.col('tip_amount')).alias("total_tip"),
                                F.sum(F.col('trip_distance')).alias("avg_trip_distance"),

                                ).withColumn('month_revenue', F.col('total_fare') + F.col('total_tip')) )



@dlt.table(name="gold_payment_analysis",
           comment = "Trip and revenue analysis by payment type",
           table_properties ={"layer": "gold",
                              "delta.feature.timestampNtz": "supported"
                              }
           )
def gold_payment_analysis():
    df_payment = spark.read.table("silver_payment_type_taxanomy")

    df = spark.readStream.table("silver_yellow_trips")
    
    df = df.groupBy(F.col("payment_type")).agg(F.count("*").alias("total_trips"), 
                                F.sum(F.col('fare_amount')).alias("total_fare"), 
                                F.sum(F.col('tip_amount')).alias("total_tip"),
                                F.sum(F.col('trip_distance')).alias("avg_trip_distance"),

                                ).withColumn('month_revenue', F.col('total_fare') + F.col('total_tip'))

    df = df.join(
        df_payment,
        on=["payment_type"],
        how="left"
    )
    df = df.drop("payment_type")
    return df

