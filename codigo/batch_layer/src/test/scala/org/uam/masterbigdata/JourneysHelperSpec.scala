package org.uam.masterbigdata

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.scalatest.funspec.AnyFunSpec

import java.sql.Timestamp
import java.util.UUID
import java.util.ArrayList

class JourneysHelperSpec extends AnyFunSpec with DataFrameComparer with SparkSessionTestWrapper with Schemas {
  describe("calculateJourneys") {
    it("If ignition on and off set journey") {
      val journeyRaw: java.util.List[Row] = new ArrayList[Row]()
      journeyRaw.add(
        Row(UUID.randomUUID().toString
          , 1328414834680696832L
          , Timestamp.valueOf("2023-02-05 10:55:28.808")
          , "Avenida de la Vega, Tres Cantos, Comunidad de Madrid, 28760, Espa�a1"
          , 40.605957
          , -3.711923
          , Timestamp.valueOf("2023-02-05 10:57:28.808")
          , "Avenida de la Vega, Tres Cantos, Comunidad de Madrid, 28760, Espa�a3"
          , 40.605959
          , -3.711921
          , 12
          , null)
      )

      val expectedDF = spark.createDataFrame(journeyRaw, journey_schema)

      val sourceDF = spark.read.schema(telemetry_schema).json("batch_layer/src/test/resources/data/journeysHelperSpec/completeJourney.json")

      val currentDF = sourceDF.transform(JourneysHelper.calculateJourneys())

      assertSmallDataFrameEquality(expectedDF, currentDF)
    }

    it("If ignition just on, no journey is created") {
      ???
    }

    it("If ignition just off, no journey is created") {
      ???
    }
  }

  describe("setIgnitionStateChange") {
    val source_schema:StructType = StructType(
      Array(
        StructField("ignition", BooleanType, nullable = false)
        , StructField("attributes", StructType(
          Array(
            StructField("deviceId", IntegerType, nullable = false)
          )
        ), nullable = false
        )
        , StructField("timestamp", TimestampType, nullable = false)
      )
    )
    /**
     * pasa una cadena de texto en formato json a dataframe siguiendo el esquema source_schema
     * */
    def sourceJsonToDF(json:List[String]):DataFrame =
      spark.createDF(
        json
        , List(StructField("value", StringType))
      ).select(from_json(col("value"), source_schema).as("value_as_json"))
        .select(col("value_as_json").getField("ignition").as("ignition")
          , col("value_as_json").getField("attributes").as("attributes")
          , col("value_as_json").getField("timestamp").as("timestamp")
        )

    val expected_schema = StructType(
      Array(
        StructField("ignition", BooleanType, nullable = false)
        , StructField("attributes", StructType(
          Array(
            StructField("deviceId", IntegerType, nullable = false)
          )
        ), nullable = false
        )
        , StructField("timestamp", TimestampType, nullable = false)
        , StructField("state_changed", IntegerType)
      )
    )

    /**
     * pasa una cadena de texto en formato json a dataframe siguiendo el esquema expected_schema
     * */
    def expectedJsonToDF(json: List[String]): DataFrame =
      spark.createDF(
           json
        , List(StructField("value", StringType))
      ).select(from_json(col("value"), expected_schema).as("value_as_json"))
        .select(col("value_as_json").getField("ignition").as("ignition")
          , col("value_as_json").getField("attributes").as("attributes")
          , col("value_as_json").getField("timestamp").as("timestamp")
          , col("value_as_json").getField("state_changed").as("state_changed")
        )

    it("If the state of ignition doesn't change (always false) from previous frame to current the value is 0") {
      val sourceDF = sourceJsonToDF(
         List("""{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:01"}"""
          ,"""{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:02"}"""
          ,"""{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:03"}"""
          ,"""{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:04"}"""
         )
        )

      val actualDF = JourneysHelper.setIgnitionStateChange()(sourceDF)

      val expectedDF = expectedJsonToDF(
        List("""{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:01", "state_changed":0}"""
          , """{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:02", "state_changed":0}"""
          , """{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:03", "state_changed":0}"""
          , """{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:04", "state_changed":0}"""
          )
        )

      assertSmallDataFrameEquality(actualDF, expectedDF)
    }

    it("If the state of ignition doesn't change (always true) from previous frame to current the value is 0") {
      val sourceDF = sourceJsonToDF(
        List("""{"ignition":true, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:01"}"""
          , """{"ignition":true, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:02"}"""
          , """{"ignition":true, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:03"}"""
          , """{"ignition":true, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:04"}"""
        )
      )

      val actualDF = JourneysHelper.setIgnitionStateChange()(sourceDF)

      val expectedDF = expectedJsonToDF(
        List("""{"ignition":true, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:01", "state_changed":0}"""
          , """{"ignition":true, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:02", "state_changed":0}"""
          , """{"ignition":true, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:03", "state_changed":0}"""
          , """{"ignition":true, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:04", "state_changed":0}"""
        )
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)
    }

    it("If the state of ignition changes (from true to false) from previous frame to current the value is 1") {
      val sourceDF = sourceJsonToDF(
        List("""{"ignition":true, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:01"}"""
          , """{"ignition":true, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:02"}"""
          , """{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:03"}"""
          , """{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:04"}"""
        )
      )

      val actualDF = JourneysHelper.setIgnitionStateChange()(sourceDF)

      val expectedDF = expectedJsonToDF(
        List("""{"ignition":true, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:01", "state_changed":0}"""
          , """{"ignition":true, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:02", "state_changed":0}"""
          , """{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:03", "state_changed":1}"""
          , """{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:04", "state_changed":0}"""
        )
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)
    }

    it("If the state of ignition changes (from false to true) from previous frame to current the value is 1") {
      val sourceDF = sourceJsonToDF(
        List("""{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:01"}"""
          , """{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:02"}"""
          , """{"ignition":true, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:03"}"""
          , """{"ignition":true, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:04"}"""
        )
      )

      val actualDF = JourneysHelper.setIgnitionStateChange()(sourceDF)

      val expectedDF = expectedJsonToDF(
        List("""{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:01", "state_changed":0}"""
          , """{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:02", "state_changed":0}"""
          , """{"ignition":true, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:03", "state_changed":1}"""
          , """{"ignition":true, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:04", "state_changed":0}"""
        )
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)
    }

  }
}
