from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, to_date
from datetime import datetime, timedelta
from utils.spark import load_spark_app
from utils.cluster_config import get_cluster_config

def main(cluster_config):
    # Start Spark session
    spark = load_spark_app("spark_agg_daily", cluster_config)

    # Read the maximum day present in the summary table
    max_day_summary = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="dailysummary", keyspace=cluster_config.keyspace) \
        .load() \
        .select(max("day").alias("max_day")) \
        .collect()[0]["max_day"]

    # Calculate the time range for the last 24 hours before the maximum day in the summary table
    end_time = (datetime.now() + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    
    if max_day_summary:
        start_time = max_day_summary
    else:
        # If no maximum day found, default to current day
        start_time = end_time - timedelta(days=1)

    # Read events data for the last 24 hours before the maximum day
    events_data = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="vehicleevents", keyspace=cluster_config.keyspace) \
        .load() \
        .filter((col("event_time") >= start_time) & (col("event_time") <= end_time))

    # Calculate vehicle's last event time and type for each day from start_time until today
    last_event_per_day = events_data \
        .withColumn("day", to_date(col("event_time"))) \
        .groupBy("vehicle_id", "day") \
        .agg(max("event_time").alias("last_event_time"), max("event_type").alias("last_event_type"))

    last_event_per_day.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="dailysummary", keyspace=cluster_config.keyspace) \
        .mode("append") \
        .save()

if __name__ == "__main__":
    # Get cluster configuration
    cluster_config = get_cluster_config()
        
    main(cluster_config)
