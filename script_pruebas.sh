#!/bin/bash
entorno/apps/Batch*.jar

#generar jar del batch
cd codigo
#estamos usando sbt 1.8.0 on java 11.0.2
sbt batch/clean
sbt batch/assembly
#copiar archivo jar
mv Batch_Layer/target/scala-2.12/Batch_Layer-assembly-0.1.0-SNAPSHOT.jar ../entorno/apps

cd ..
#arrancar docker compose
docker-compose -f entorno/docker-compose.yml up -d
#No muy ortodoxo pero le damos un tiempo a que arranque

sleep 10s
#ejecutar 
docker exec -it entorno_spark-worker_1 /spark/bin/spark-submit --class org.uam.masterbigdata.drivers.ToSubmitDriver --deploy-mode client --master spark://spark-master:7077 --verbose --supervise /opt/spark-apps/Batch_Layer-assembly-0.1.0-SNAPSHOT.jar /opt/spark-data/datos_2.json 

echo "RECUERDA PARAR EL DOCKER COMPOSE DE ENTORNO AL TERMINAR"
