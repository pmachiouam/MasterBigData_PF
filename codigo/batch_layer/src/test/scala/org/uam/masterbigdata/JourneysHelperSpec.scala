package org.uam.masterbigdata

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.scalatest.funspec.AnyFunSpec

import java.sql.Timestamp
import java.util.UUID
import java.util.ArrayList

class JourneysHelperSpec extends AnyFunSpec with DataFrameComparer with SparkSessionTestWrapper with DataFrameTestHelper with Schemas {
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
    val source_schema: StructType = StructType(
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


    it("If the state of ignition doesn't change (always false) from previous frame to current the value is 0") {
      val sourceDF = jsonToDF(
        List("""{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:01"}"""
          , """{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:02"}"""
          , """{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:03"}"""
          , """{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:04"}"""
        )
        , source_schema
      )

      val actualDF = JourneysHelper.setIgnitionStateChange()(sourceDF)

      val expectedDF = jsonToDF(
        List("""{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:01", "state_changed":0}"""
          , """{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:02", "state_changed":0}"""
          , """{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:03", "state_changed":0}"""
          , """{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:04", "state_changed":0}"""
        )
        , expected_schema
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)
    }

    it("If the state of ignition doesn't change (always true) from previous frame to current the value is 0") {
      val sourceDF = jsonToDF(
        List("""{"ignition":true, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:01"}"""
          , """{"ignition":true, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:02"}"""
          , """{"ignition":true, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:03"}"""
          , """{"ignition":true, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:04"}"""
        )
        , source_schema
      )

      val actualDF = JourneysHelper.setIgnitionStateChange()(sourceDF)

      val expectedDF = jsonToDF(
        List("""{"ignition":true, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:01", "state_changed":0}"""
          , """{"ignition":true, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:02", "state_changed":0}"""
          , """{"ignition":true, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:03", "state_changed":0}"""
          , """{"ignition":true, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:04", "state_changed":0}"""
        )
        , expected_schema
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)
    }

    it("If the state of ignition changes (from true to false) from previous frame to current the value is 1") {
      val sourceDF = jsonToDF(
        List("""{"ignition":true, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:01"}"""
          , """{"ignition":true, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:02"}"""
          , """{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:03"}"""
          , """{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:04"}"""
        )
        , source_schema
      )

      val actualDF = JourneysHelper.setIgnitionStateChange()(sourceDF)

      val expectedDF = jsonToDF(
        List("""{"ignition":true, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:01", "state_changed":0}"""
          , """{"ignition":true, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:02", "state_changed":0}"""
          , """{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:03", "state_changed":1}"""
          , """{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:04", "state_changed":0}"""
        )
        , expected_schema
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)
    }

    it("If the state of ignition changes (from false to true) from previous frame to current the value is 1") {
      val sourceDF = jsonToDF(
        List("""{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:01"}"""
          , """{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:02"}"""
          , """{"ignition":true, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:03"}"""
          , """{"ignition":true, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:04"}"""
        )
        , source_schema
      )

      val actualDF = JourneysHelper.setIgnitionStateChange()(sourceDF)

      val expectedDF = jsonToDF(
        List("""{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:01", "state_changed":0}"""
          , """{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:02", "state_changed":0}"""
          , """{"ignition":true, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:03", "state_changed":1}"""
          , """{"ignition":true, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:04", "state_changed":0}"""
        )
        , expected_schema
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)
    }


    it("With 2 devices. If the state of ignition changes (from false to true) from previous frame to current the value is 1") {
      val sourceDF = jsonToDF(
        List("""{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:01"}"""
          , """{"ignition":false, "attributes":{"deviceId":2}, "timestamp":"2022-02-01 00:00:01"}"""
          , """{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:02"}"""
          , """{"ignition":false, "attributes":{"deviceId":2}, "timestamp":"2022-02-01 00:00:02"}"""
          , """{"ignition":true, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:03"}"""
          , """{"ignition":true, "attributes":{"deviceId":2}, "timestamp":"2022-02-01 00:00:03"}"""
          , """{"ignition":true, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:04"}"""
          , """{"ignition":true, "attributes":{"deviceId":2}, "timestamp":"2022-02-01 00:00:04"}"""
        )
        , source_schema
      )

      val actualDF = JourneysHelper.setIgnitionStateChange()(sourceDF)

      val expectedDF = jsonToDF(
        List("""{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:01", "state_changed":0}"""
          , """{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:02", "state_changed":0}"""
          , """{"ignition":true, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:03", "state_changed":1}"""
          , """{"ignition":true, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:04", "state_changed":0}"""
          , """{"ignition":false, "attributes":{"deviceId":2}, "timestamp":"2022-02-01 00:00:01", "state_changed":0}"""
          , """{"ignition":false, "attributes":{"deviceId":2}, "timestamp":"2022-02-01 00:00:02", "state_changed":0}"""
          , """{"ignition":true, "attributes":{"deviceId":2}, "timestamp":"2022-02-01 00:00:03", "state_changed":1}"""
          , """{"ignition":true, "attributes":{"deviceId":2}, "timestamp":"2022-02-01 00:00:04", "state_changed":0}"""
        )
        , expected_schema
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)
    }

    it("With the unordered frames. If the state of ignition changes (from false to true) from previous frame to current the value is 1") {
      val sourceDF = jsonToDF(
        List("""{"ignition":true, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:03"}"""
          , """{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:01"}"""
          , """{"ignition":true, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:04"}"""
          , """{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:02"}"""
        )
        , source_schema
      )

      val actualDF = JourneysHelper.setIgnitionStateChange()(sourceDF)

      val expectedDF = jsonToDF(
        List("""{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:01", "state_changed":0}"""
          , """{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:02", "state_changed":0}"""
          , """{"ignition":true, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:03", "state_changed":1}"""
          , """{"ignition":true, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:04", "state_changed":0}"""
        )
        , expected_schema
      )
      assertSmallDataFrameEquality(actualDF, expectedDF)
    }
  }

  describe("setGroupOfStateChangesToFrames") {
    val source_schema = StructType(
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
        , StructField("state_changed_group", LongType)
      )
    )

    it("There is no state change so there is no increment in the group identifier") {
      val sourceDF = jsonToDF(
        List("""{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:01", "state_changed":0}"""
          , """{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:02", "state_changed":0}"""
          , """{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:03", "state_changed":0}"""
          , """{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:04", "state_changed":0}"""
        )
        , source_schema
      )

      val actualDF = JourneysHelper.setGroupOfStateChangesToFrames()(sourceDF)

      val expectedDF = jsonToDF(
        List("""{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:01", "state_changed":0, "state_changed_group":0}"""
          , """{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:02", "state_changed":0, "state_changed_group":0}"""
          , """{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:03", "state_changed":0, "state_changed_group":0}"""
          , """{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:04", "state_changed":0, "state_changed_group":0}"""
        )
        , expected_schema
      )
      assertSmallDataFrameEquality(actualDF, expectedDF)
    }
    //con grupo
    it("There are two state changes so the group is increased by two") {
      val sourceDF = jsonToDF(
        List("""{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:01", "state_changed":0}"""
          , """{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:02", "state_changed":0}"""
          , """{"ignition":true, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:03", "state_changed":1}"""
          , """{"ignition":true, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:04", "state_changed":0}"""
          , """{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:05", "state_changed":1}"""
          , """{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:06", "state_changed":0}"""
        )
        , source_schema
      )

      val actualDF = JourneysHelper.setGroupOfStateChangesToFrames()(sourceDF)

      val expectedDF = jsonToDF(
        List("""{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:01", "state_changed":0, "state_changed_group":0}"""
          , """{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:02", "state_changed":0, "state_changed_group":0}"""
          , """{"ignition":true, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:03", "state_changed":1, "state_changed_group":1}"""
          , """{"ignition":true, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:04", "state_changed":0, "state_changed_group":1}"""
          , """{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:05", "state_changed":1, "state_changed_group":2}"""
          , """{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:06", "state_changed":0, "state_changed_group":2}"""
        )
        , expected_schema
      )
      assertSmallDataFrameEquality(actualDF, expectedDF)
    }

    //con grupo 2 dispositivos
    it("Two devices. There is one state change by device so there is one increment in the group identifier per device ") {
      val sourceDF = jsonToDF(
        List("""{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:01", "state_changed":0}"""
          , """{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:02", "state_changed":0}"""
          , """{"ignition":true, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:03", "state_changed":1}"""
          , """{"ignition":true, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:04", "state_changed":0}"""
          , """{"ignition":false, "attributes":{"deviceId":2}, "timestamp":"2022-02-01 00:00:01", "state_changed":0}"""
          , """{"ignition":false, "attributes":{"deviceId":2}, "timestamp":"2022-02-01 00:00:02", "state_changed":0}"""
          , """{"ignition":true, "attributes":{"deviceId":2}, "timestamp":"2022-02-01 00:00:03", "state_changed":1}"""
          , """{"ignition":true, "attributes":{"deviceId":2}, "timestamp":"2022-02-01 00:00:04", "state_changed":0}"""
        )
        , source_schema
      )

      val actualDF = JourneysHelper.setGroupOfStateChangesToFrames()(sourceDF)

      val expectedDF = jsonToDF(
        List("""{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:01", "state_changed":0, "state_changed_group":0}"""
          , """{"ignition":false, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:02", "state_changed":0, "state_changed_group":0}"""
          , """{"ignition":true, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:03", "state_changed":1, "state_changed_group":1}"""
          , """{"ignition":true, "attributes":{"deviceId":1}, "timestamp":"2022-02-01 00:00:04", "state_changed":0, "state_changed_group":1}"""
          , """{"ignition":false, "attributes":{"deviceId":2}, "timestamp":"2022-02-01 00:00:01", "state_changed":0, "state_changed_group":0}"""
          , """{"ignition":false, "attributes":{"deviceId":2}, "timestamp":"2022-02-01 00:00:02", "state_changed":0, "state_changed_group":0}"""
          , """{"ignition":true, "attributes":{"deviceId":2}, "timestamp":"2022-02-01 00:00:03", "state_changed":1, "state_changed_group":1}"""
          , """{"ignition":true, "attributes":{"deviceId":2}, "timestamp":"2022-02-01 00:00:04", "state_changed":0, "state_changed_group":1}"""
        )
        , expected_schema
      )
      assertSmallDataFrameEquality(actualDF, expectedDF)
    }
  }

  describe("setInitialStateChangeValues") {
    val source_schema = StructType(
      Array(
        StructField("ignition", BooleanType, nullable = false)
        , StructField("attributes", StructType(
          Array(
            StructField("deviceId", IntegerType, nullable = false)
          )
        ), nullable = false
        )
        , StructField("timestamp", TimestampType, nullable = false)
        , StructField("location_address", StringType, nullable = false)
        , StructField("location_latitude", DoubleType, nullable = false)
        , StructField("location_longitude", DoubleType, nullable = false)
        , StructField("state_changed", IntegerType, nullable = false)
        , StructField("state_changed_group", LongType, nullable = false)
      )
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
        , StructField("location_address", StringType, nullable = false)
        , StructField("location_latitude", DoubleType, nullable = false)
        , StructField("location_longitude", DoubleType, nullable = false)
        , StructField("state_changed", IntegerType, nullable = false)
        , StructField("state_changed_group", LongType, nullable = false)
        , StructField("start_timestamp", TimestampType, nullable = false)
        , StructField("start_location_address", StringType, nullable = false)
        , StructField("start_location_latitude", DoubleType, nullable = false)
        , StructField("start_location_longitude", DoubleType, nullable = false)
      )
    )

    it(
      """It sets the timestamp, location_address, location_latitude, location_longitude of the first frame (older timestamp) of frames with
         the same state_changed_group
        """) {
      val sourceDF = jsonToDF(
        List(
          """{"ignition":false, "attributes":{"deviceId":1}
            |, "timestamp":"2022-02-01 00:00:01", "location_address":"address1", "location_latitude":1.1, "location_longitude":2.1
            |, "state_changed":0, "state_changed_group":0}""".stripMargin
          ,
          """{"ignition":false, "attributes":{"deviceId":1}
            |, "timestamp":"2022-02-01 00:00:02", "location_address":"address2", "location_latitude":1.2, "location_longitude":2.2
            |, "state_changed":0, "state_changed_group":1}""".stripMargin
          ,
          """{"ignition":false, "attributes":{"deviceId":1}
            | ,"timestamp":"2022-02-01 00:00:03", "location_address":"address3", "location_latitude":1.3, "location_longitude":2.3
            | , "state_changed":0, "state_changed_group":1}""".stripMargin
          ,
          """{"ignition":false, "attributes":{"deviceId":1}
            |, "timestamp":"2022-02-01 00:00:04", "location_address":"address4", "location_latitude":1.4, "location_longitude":2.4
            |, "state_changed":0, "state_changed_group":2}""".stripMargin
        )
        , source_schema
      )

      val actualDF = JourneysHelper.setInitialStateChangeValues()(sourceDF)

      val expectedDF = jsonToDF(
        List(
          """{"ignition":false, "attributes":{"deviceId":1}
            |, "timestamp":"2022-02-01 00:00:01", "location_address":"address1", "location_latitude":1.1, "location_longitude":2.1
            |, "state_changed":0, "state_changed_group":0
            |, "start_timestamp":"2022-02-01 00:00:01", "start_location_address":"address1", "start_location_latitude":1.1, "start_location_longitude":2.1}""".stripMargin
          ,
          """{"ignition":false, "attributes":{"deviceId":1}
            |, "timestamp":"2022-02-01 00:00:02", "location_address":"address2", "location_latitude":1.2, "location_longitude":2.2
            |, "state_changed":0, "state_changed_group":1
            |, "start_timestamp":"2022-02-01 00:00:02", "start_location_address":"address2", "start_location_latitude":1.2, "start_location_longitude":2.2}""".stripMargin
          ,
          """{"ignition":false, "attributes":{"deviceId":1}
            | ,"timestamp":"2022-02-01 00:00:03", "location_address":"address3", "location_latitude":1.3, "location_longitude":2.3
            | , "state_changed":0, "state_changed_group":1
            | , "start_timestamp":"2022-02-01 00:00:02", "start_location_address":"address2", "start_location_latitude":1.2, "start_location_longitude":2.2}""".stripMargin
          ,
          """{"ignition":false, "attributes":{"deviceId":1}
            |, "timestamp":"2022-02-01 00:00:04", "location_address":"address4", "location_latitude":1.4, "location_longitude":2.4
            |, "state_changed":0, "state_changed_group":2
            |, "start_timestamp":"2022-02-01 00:00:04", "start_location_address":"address4", "start_location_latitude":1.4, "start_location_longitude":2.4}""".stripMargin
        )
        , expected_schema
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)
    }

    it("Pendiente con 2 dispositivos") {
      ???
    }
  }

  describe("setFinalStateChangeValues") {
    val source_schema = StructType(
      Array(
        StructField("ignition", BooleanType, nullable = false)
        , StructField("attributes", StructType(
          Array(
            StructField("deviceId", IntegerType, nullable = false)
          )
        ), nullable = false
        )
        , StructField("timestamp", TimestampType, nullable = false)
        , StructField("location_address", StringType, nullable = false)
        , StructField("location_latitude", DoubleType, nullable = false)
        , StructField("location_longitude", DoubleType, nullable = false)
        , StructField("state_changed", IntegerType, nullable = false)
        , StructField("state_changed_group", LongType, nullable = false)
      )
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
        , StructField("location_address", StringType, nullable = false)
        , StructField("location_latitude", DoubleType, nullable = false)
        , StructField("location_longitude", DoubleType, nullable = false)
        , StructField("state_changed", IntegerType, nullable = false)
        , StructField("state_changed_group", LongType, nullable = false)
        , StructField("end_timestamp", TimestampType, nullable = false)
        , StructField("end_location_address", StringType, nullable = false)
        , StructField("end_location_latitude", DoubleType, nullable = false)
        , StructField("end_location_longitude", DoubleType, nullable = false)
      )
    )
    it(
      """It sets the timestamp, location_address, location_latitude, location_longitude of the last frame (newer timestamp) of frames with
             the same state_changed_group
            """) {
      val sourceDF = jsonToDF(
        List(
          """{"ignition":false, "attributes":{"deviceId":1}
            |, "timestamp":"2022-02-01 00:00:01", "location_address":"address1", "location_latitude":1.1, "location_longitude":2.1
            |, "state_changed":0, "state_changed_group":0}""".stripMargin
          ,
          """{"ignition":false, "attributes":{"deviceId":1}
            |, "timestamp":"2022-02-01 00:00:02", "location_address":"address2", "location_latitude":1.2, "location_longitude":2.2
            |, "state_changed":0, "state_changed_group":1}""".stripMargin
          ,
          """{"ignition":false, "attributes":{"deviceId":1}
            | ,"timestamp":"2022-02-01 00:00:03", "location_address":"address3", "location_latitude":1.3, "location_longitude":2.3
            | , "state_changed":0, "state_changed_group":1}""".stripMargin
          ,
          """{"ignition":false, "attributes":{"deviceId":1}
            |, "timestamp":"2022-02-01 00:00:04", "location_address":"address4", "location_latitude":1.4, "location_longitude":2.4
            |, "state_changed":0, "state_changed_group":2}""".stripMargin
        )
        , source_schema
      )

      val actualDF = JourneysHelper.setFinalStateChangeValues()(sourceDF)

      val expectedDF = jsonToDF(
        List(
          """{"ignition":false, "attributes":{"deviceId":1}
            |, "timestamp":"2022-02-01 00:00:01", "location_address":"address1", "location_latitude":1.1, "location_longitude":2.1
            |, "state_changed":0, "state_changed_group":0
            |, "end_timestamp":"2022-02-01 00:00:01", "end_location_address":"address1", "end_location_latitude":1.1, "end_location_longitude":2.1}""".stripMargin
          ,
          """{"ignition":false, "attributes":{"deviceId":1}
            |, "timestamp":"2022-02-01 00:00:02", "location_address":"address2", "location_latitude":1.2, "location_longitude":2.2
            |, "state_changed":0, "state_changed_group":1
            |, "end_timestamp":"2022-02-01 00:00:03", "end_location_address":"address3", "end_location_latitude":1.3, "end_location_longitude":2.3}""".stripMargin
          ,
          """{"ignition":false, "attributes":{"deviceId":1}
            | ,"timestamp":"2022-02-01 00:00:03", "location_address":"address3", "location_latitude":1.3, "location_longitude":2.3
            | , "state_changed":0, "state_changed_group":1
            | , "end_timestamp":"2022-02-01 00:00:03", "end_location_address":"address3", "end_location_latitude":1.3, "end_location_longitude":2.3}""".stripMargin
          ,
          """{"ignition":false, "attributes":{"deviceId":1}
            |, "timestamp":"2022-02-01 00:00:04", "location_address":"address4", "location_latitude":1.4, "location_longitude":2.4
            |, "state_changed":0, "state_changed_group":2
            |, "end_timestamp":"2022-02-01 00:00:04", "end_location_address":"address4", "end_location_latitude":1.4, "end_location_longitude":2.4}""".stripMargin
        )
        , expected_schema
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)
    }

    it("Pendiente con 2 dispositivos"){
     ???
    }
  }
}
