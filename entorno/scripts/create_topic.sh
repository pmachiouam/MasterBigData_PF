#!/bin/bash
#crear el topic telemetry
./../bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic telemetry

#Consumer por consola
#./../bin/kafka-console-consumer.sh --bootstrap-server localhost:9092  --topic telemetry

#Producer por consola
./../bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic telemetry < ../data/streams_example.json