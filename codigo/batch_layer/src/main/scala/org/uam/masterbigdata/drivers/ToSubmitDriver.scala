package org.uam.masterbigdata.drivers

import org.apache.spark.sql.{ DataFrame, SparkSession}
import org.uam.masterbigdata.{EventsHelper, JourneysHelper, Schemas}

/**
 * Usado para enviar al cluster de Spark
 * Recuerda en BatchDependecies establecer las depencias de spark como provided
 * */
object ToSubmitDriver extends Schemas with DatabaseWriter {

  def main(args: Array[String]): Unit = {

    if (args.length != 1) {
      println("Need 1) json path")
      System.exit(1)
    }
    println(s"Json path ${args(0)}")
    val spark = SparkSession.builder()
      .appName("ParseJourneys")
      .getOrCreate()

    /** Por parámetros le pasamos la dirección del archivo */
    //Lee el archivo,
    val telemetryDF:DataFrame = spark.read.option("mode", "FAILFAST").schema(telemetry_schema).json(args(0))

    /**Trayectos*/
    //convierte a trayecto
    val journeysDF:DataFrame = telemetryDF.transform(JourneysHelper.calculateJourneys())
    //Muestra los 3 primeros
    journeysDF.show(3)

    //Escribe en base de datos
    saveDataFrameInPostgresSQL(journeysDF, "journeys")

   /**Eventos*/
    val eventsDF: DataFrame = telemetryDF.transform(EventsHelper.createExcessiveThrottleEvent())
    //Muestra los 3 primeros
    eventsDF.show(3)
    //Escribe en base de datos
    saveDataFrameInPostgresSQL(eventsDF, "events")

  }
}
