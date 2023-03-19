#!/bin/bash
#crear el jar del projecto de spark. Contiene la parte batch y la de Stream
cd codigo
sbt spark_proj/clean
sbt spark_proj/assembly
#copiar archivo jar
mv spark_proj/target/scala-2.12/spark_project.jar ../entorno/apps

