import os
from dotenv import load_dotenv
from collections import namedtuple

load_dotenv()  # Load environment variables from .env file

ClusterConfig = namedtuple("ClusterConfig", ["host", "secure_connect_bundle", "username", "password", "keyspace"])

def get_cluster_config():
    return ClusterConfig(
        secure_connect_bundle=os.getenv("CASSANDRA_SECURE_CONNECT_BUNDLE"),
        username=os.getenv("CASSANDRA_USERNAME"),
        password=os.getenv("CASSANDRA_PASSWORD"),
        keyspace=os.getenv("CASSANDRA_KEYSPACE"),
        host=os.getenv("CASSANDRA_HOST")
    )
