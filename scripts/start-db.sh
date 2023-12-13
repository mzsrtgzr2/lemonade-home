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

echo "create db $DB"