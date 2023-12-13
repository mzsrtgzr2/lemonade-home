from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from pyssandra import Pyssandra
from models.vehicle_status import VehicleStatus
from models.vehicle_events import VehicleEvent
from src.utils.cassandra import sync_table_safely
from utils.cluster_config import get_cluster_config


def main():
    # Get cluster configuration
    cluster_config = get_cluster_config()

    # auth_provider = PlainTextAuthProvider(username=cluster_config.username, password=cluster_config.password)
    cluster = Cluster([cluster_config.host])  # tbd, for production use secure_connect_bundle, auth etc
    session = cluster.connect()

    db = Pyssandra(session, cluster_config.keyspace)
    setup_models(db)
    
def setup_models(db: Pyssandra):
    @db.table(partition_keys=["vehicle_id"], index=["vehicle_id", "status_source"])
    class VehicleStatusModel(VehicleStatus):
        pass

    @db.table(partition_keys=["vehicle_id"], index=["vehicle_id", "event_source"])
    class VehicleEventModel(VehicleEvent):
        pass

    # Create DB Tables.
    # Sync VehicleStatusModel
    sync_table_safely(db, VehicleStatusModel)

    # Sync VehicleEventModel
    sync_table_safely(db, VehicleEventModel)

if __name__ == "__main__":
    main()
