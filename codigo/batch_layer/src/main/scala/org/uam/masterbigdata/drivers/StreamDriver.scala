package org.uam.masterbigdata.drivers

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.types.StringType
import org.uam.masterbigdata.{CommonTelemetryHelper, EventsHelper, Schemas}

object StreamDriver extends Schemas {
  val spark = SparkSession
    .builder()
    //quitar al hacer submit
    .master("local[*]")
    .appName("Streaming")
    .getOrCreate()

  def createStreamEvents(): Unit = {
    val streamDF: DataFrame = loadKafkaStrean()

    val telemetryFromSocketDF = streamDF.select(from_json(col("value"), telemetry_schema).as("json"))
      .selectExpr("json.*")

    val query: StreamingQuery = telemetryFromSocketDF
      .transform(EventsHelper.createFuelStealingEvent())
      .writeStream
      .outputMode(OutputMode.Append())
      .format("console")
      .start()

    query.awaitTermination()

  }


  private def loadKafkaStrean(): DataFrame = spark.readStream
    .format("kafka")
    .options(
      Map(
        "kafka.bootstrap.servers" -> "localhost:9092"
        , "subscribe" -> "telemetry"
      )
    ).load()
    .select(col("value").cast(StringType).as("value"))

  /** Usar para probar stream mediante socket, por ejemplo con netcat (nc -lk 12345) */
  private def readFromSocket(): DataFrame = spark.readStream
    .format("socket")
    .options(
      Map(
        "host" -> "127.0.0.1"
        , "port" -> "12345"
      )
    )
    .load()

  def main(args: Array[String]): Unit = {
    createStreamEvents()
  }
}
