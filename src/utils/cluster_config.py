import os
from dotenv import load_dotenv
from collections import namedtuple

load_dotenv()  # Load environment variables from .env file

ClusterConfig = namedtuple("ClusterConfig", ["host", "keyspace"])

def get_cluster_config():
    return ClusterConfig(
        keyspace=os.getenv("CASSANDRA_KEYSPACE"),
        host=os.getenv("CASSANDRA_HOST")
    )