#!/bin/bash

echo "Starting Kafka."

cd kafka-docker/

docker compose up -d

sleep 4

# Opens Conduktor
open http://localhost:8080

sleep 60

# somtimes this command fails for a specific broker.
docker compose exec kafka1 kafka-topics --bootstrap-server kafka1:9092 --topic wikimedia.recentchange --create --replication-factor 3 --partitions 3
docker compose exec kafka2 kafka-topics --bootstrap-server kafka2:9093 --topic wikimedia.recentchange --create --replication-factor 3 --partitions 3
docker compose exec kafka3 kafka-topics --bootstrap-server kafka3:9094 --topic wikimedia.recentchange --create --replication-factor 3 --partitions 3

echo "OpenSearch Dashboard at http://localhost:5601/app/dev_tools#/console"

echo "Done"

exit 0