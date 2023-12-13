from pyspark.sql import SparkSession


def load_spark_app(name: str, cluster_config):
    spark = SparkSession.builder \
        .appName(name) \
        .config("spark.cassandra.connection.host", cluster_config.host)\
        .config("spark.cassandra.connection.port", cluster_config.port) \
        .config("spark.sql.catalog.mycatalog", "com.datastax.spark.connector.datasource.CassandraCatalog") \
        .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1") \
        .getOrCreate()
    return spark