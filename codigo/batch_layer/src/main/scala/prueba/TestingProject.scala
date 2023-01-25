package prueba

import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.UUID
import scala.util.Random

object TestingProject {
  def main(args: Array[String]):Unit = {
    val spark = SparkSession.builder().appName("Test Deploy App")
      .getOrCreate()
    /*
          val locationSchema: StructType = StructType(
             Array(
                 StructField("created", DateType, nullable = false)
                , StructField("address", StringType)
                , StructField("latitude", DoubleType, nullable = false)
                , StructField("longitude", DoubleType, nullable = false)
                , StructField("altitude", DoubleType)
                , StructField("speed", FloatType)
                , StructField("valid", BooleanType)
                , StructField("course", FloatType)
             )
          )

          val journeySchema:StructType = StructType(
             Array(
                 StructField("id", StringType, nullable = false)
                ,StructField("device_id", LongType, nullable = false)
                ,StructField("start_timestamp", DateType, nullable = false)
                ,StructField("start_location_address", StringType)
                ,StructField("start_location_latitude", DoubleType, nullable = false)
                ,StructField("start_location_longitude", DoubleType, nullable = false)
                ,StructField("end_timestamp", DateType)
                ,StructField("end_location_address", StringType)
                ,StructField("end_location_latitude", DoubleType)
                ,StructField("end_location_longitude", DoubleType)
                ,StructField("distance", LongType, nullable = false)
             )
          )




          val framesSchema:StructType = StructType(
             Array(
                StructField("id", LongType, nullable = false)
                ,StructField("device_id", LongType, nullable = false)
                ,StructField("created", DateType, nullable = false)
                ,StructField("received", DateType, nullable = false)
                ,StructField("location", locationSchema)
                ,StructField("ignition", BooleanType)
             )
          )

          val eventSchema:StructType = StructType(
             Array(
                StructField("id", LongType, nullable = false)
                , StructField("device_id", LongType, nullable = false)
                , StructField("created", DateType, nullable = false)
                , StructField("type_id", IntegerType, nullable = false)
                , StructField("location_address", StringType)
                , StructField("location_latitude", DoubleType, nullable = false)
                , StructField("location_longitude", DoubleType, nullable = false)
                , StructField("value", StringType)
             )
          )
          */

    def testSaveJourney():Unit = {
      val rawJourneyData = Seq((
        UUID.randomUUID().toString
        , 1L
        , "1970-01-01"
        , "AddresStart"
        , 12.12F
        , 35.123F
        , "1970-01-02"
        , "AddresEd"
        , 13.12F
        , 36.123F
        , 123L
      ))

      import spark.implicits._
      val journeyDF = rawJourneyData.toDF("id", "device_id", "start_timestamp", "start_location_address"
        , "start_location_latitude", "start_location_longitude", "end_timestamp", "end_location_address"
        , "end_location_latitude", "end_location_longitude", "distance")

      saveDataFrameInPostgreSQL(journeyDF, "journeys")
    }


    def saveDataFrameInPostgreSQL(df:DataFrame, tableName:String):Unit = {
      val props = Map(
        JDBCOptions.JDBC_DRIVER_CLASS -> "org.postgresql.Driver"
        , "url" -> "jdbc:postgresql://postgres:5432/tracking"
        , "user" -> "docker"
        , "password" -> "docker"
        , "stringtype" -> "unspecified" //Usar string para tipo uuid de postgresql
      )

      df.write
        .format("jdbc")
        .mode(SaveMode.Append)
        .options(props)
        .option("dbtable", s"public.$tableName").save()
    }

    def testSaveFrame():Unit = {
      val rawFrameData = Seq((
        Random.nextLong()
        ,Random.nextLong()
        , "1970-01-01"
        , "1970-02-01"
        , "1970-02-01"
        , "AddresStart"
        , 12.12F
        , 35.123F
        , 7600.123D
        ,99F
        , true
        , 360.0F
        , false
      ))

      import spark.implicits._
      val frameDF = rawFrameData.toDF("id", "device_id", "created", "received", "location_created"
        , "location_address", "location_latitude", "location_longitude", "location_altitude", "location_speed"
        , "location_valid", "location_course", "ignition")
      saveDataFrameInPostgreSQL(frameDF, "frames")
    }

    def testSaveEvent(): Unit = {
      val rawFrameData = Seq((
        Random.nextLong()
        , Random.nextLong()
        , "1970-01-01"
        , 2L
        ,"EnventAddress"
        , 34.123F
        ,56.2341F
        ,"Any value"
      ))

      import spark.implicits._
      val frameDF = rawFrameData.toDF("id", "device_id", "created", "type_id"
        , "location_address", "location_latitude", "location_longitude", "value")
      saveDataFrameInPostgreSQL(frameDF, "events")
    }


    testSaveJourney()
    testSaveFrame()
    testSaveEvent()
  }
}
