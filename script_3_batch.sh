#!/bin/bash
#crear el jar del projecto de spark. Contiene la parte batch y la de Stream
cd codigo
sbt spark_proj/clean
sbt spark_proj/assembly
#copiar archivo jar
mv spark_proj/target/scala-2.12/spark_project.jar ../entorno/apps
#El primer parametro que se la pasa a la clase es la url donde se encuentran los datos, el segundo es la url con el modelo de clasificación guardado
docker exec -it entorno_spark-worker_1 /spark/bin/spark-submit --class org.uam.masterbigdata.drivers.ToSubmitDriver --deploy-mode client --master spark://spark-master:7077 --verbose --supervise /opt/spark-apps/spark_project.jar /opt/spark-data/datos_2.json /opt/spark-data/journeys_logreg_cv 

