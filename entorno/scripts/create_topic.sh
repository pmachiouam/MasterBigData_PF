#!/bin/bash
#crear el topic telemetry
sh /opt/kafka_2.13-2.8.1/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic telemetry

#Consumer por consola
#./../bin/kafka-console-consumer.sh --bootstrap-server localhost:9092  --topic telemetry

#Producer por consola
sh /opt/kafka_2.13-2.8.1/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic telemetry < /opt/kafka_2.13-2.8.1/data/streams_example.json