#!/bin/bash
set -e

SERVER="my_cassandra_server";
DB="my_database";
WAIT_TIMEOUT=60;
WAIT_INTERVAL=5;
WAIT_TIME=0;

echo "Stopping and removing old Docker container [$SERVER] and starting a new fresh instance of [$SERVER]"
(docker kill $SERVER || :) && \
  (docker rm $SERVER || :) && \
  docker run --name $SERVER \
  -p 9042:9042 \
  -d cassandra

# Wait for Cassandra to start
echo "Waiting for Cassandra server [$SERVER] to start";

until docker exec -i $SERVER cqlsh -e "DESCRIBE KEYSPACES;" &> /dev/null || [ "$WAIT_TIME" -ge "$WAIT_TIMEOUT" ]; do
    sleep $WAIT_INTERVAL;
    WAIT_TIME=$((WAIT_TIME+WAIT_INTERVAL));
done

if [ "$WAIT_TIME" -ge "$WAIT_TIMEOUT" ]; then
    echo "Timed out waiting for Cassandra to start.";
    exit 1;
fi

echo "Cassandra up :)"

# Create the keyspace
echo "CREATE KEYSPACE $DB WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };" | docker exec -i $SERVER cqlsh

echo "created db $DB"

# Create tables
echo "Creating Cassandra tables..."

docker exec -i $SERVER cqlsh -e "
USE $DB;

CREATE TABLE IF NOT EXISTS vehicleevents (
    vehicle_id TEXT,
    event_time TIMESTAMP,
    event_source TEXT,
    event_type TEXT,
    event_value TEXT,
    event_extra_data TEXT,  
    PRIMARY KEY (vehicle_id, event_time)
);

CREATE TABLE IF NOT EXISTS vehiclestatus (
    vehicle_id TEXT PRIMARY KEY,
    report_time TIMESTAMP,
    status_source TEXT,
    status TEXT
);

CREATE TABLE IF NOT EXISTS dailysummary (
    vehicle_id TEXT,
    day DATE,
    last_event_time TIMESTAMP,
    last_event_type TEXT,
    PRIMARY KEY (vehicle_id, day)
);"

echo "Cassandra tables created."