from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, StructType, TimestampType, ArrayType
from utils.cluster_config import get_cluster_config
import argparse
import os


def main(data_path, cluster_config):
    # Initialize SparkSession
    spark = SparkSession.builder \
    .appName("StreamingFileLoader") \
    .config("spark.cassandra.connection.host", cluster_config.host)\
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.sql.catalog.mycatalog", "com.datastax.spark.connector.datasource.CassandraCatalog") \
    .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1") \
    .getOrCreate()

   # Define the schema for vehicle_events streaming
    vehicle_events_schema = StructType() \
        .add("vehicle_id", StringType()) \
        .add("event_time", TimestampType()) \
        .add("event_source", StringType()) \
        .add("event_type", StringType()) \
        .add("event_value", StringType()) \
        .add("event_extra_data", StringType())
    
    vehicle_status_schema = StructType() \
        .add("vehicle_id", StringType()) \
        .add("report_time", TimestampType()) \
        .add("status_source", StringType()) \
        .add("status", StringType())

    # Read streaming data from JSON files for vehicle_events
    vehicle_events_stream = spark.readStream \
        .option("multiline","true") \
        .schema(vehicle_events_schema) \
        .json(os.path.join(data_path, "vehicles_events_*.json"))
    
    # Read streaming data from JSON files for vehicle_events
    vehicle_status_stream = spark.readStream \
        .option("multiline","true") \
        .schema(vehicle_status_schema) \
        .json(os.path.join(data_path, "vehicles_status_*.json"))
        
    # Write streaming data to Cassandra table for vehicle_events
    vehicle_events_query = vehicle_events_stream.writeStream.foreachBatch(lambda batch_df, batch_id: 
        batch_df.write.format("org.apache.spark.sql.cassandra").options(
            table="vehicleevents", keyspace=cluster_config.keyspace).mode("append").save()
    ).start()

    # Write streaming data to Cassandra table for vehicle_status
    vehicle_status_query = vehicle_status_stream.writeStream.foreachBatch(lambda batch_df, batch_id: 
        batch_df.write.format("org.apache.spark.sql.cassandra").options(
            table="vehiclestatus", keyspace=cluster_config.keyspace).mode("append").save()
    ).start()

    vehicle_events_query.awaitTermination()
    vehicle_status_query.awaitTermination()

if __name__ == "__main__":
        
    # Argument parser for specifying the data path
    parser = argparse.ArgumentParser(description='Generate vehicle events and statuses JSON files.')
    parser.add_argument('--data-path', type=str, help='Path to save vehicle events and statuses JSON files')
    args = parser.parse_args()

    if not args.data_path:
        raise ValueError("data_path must be provided.")

    # makedir if not exists
    if not os.path.exists(args.data_path):
        os.makedirs(args.data_path)
        
    # Get cluster configuration
    cluster_config = get_cluster_config()
        
    main(args.data_path, cluster_config)
