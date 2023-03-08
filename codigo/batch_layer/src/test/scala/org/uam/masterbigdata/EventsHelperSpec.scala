package org.uam.masterbigdata

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.uam.masterbigdata.EventsHelper.{FuelStealingEventData, FuelStealingEventResponse, FuelStealingEventState, checkFuelStealing}

import java.sql.Timestamp

class EventsHelperSpec extends AnyFunSpec
  with DataFrameComparer
  with SparkSessionTestWrapper
  with DataFrameTestHelper
  with Schemas {
  describe("createExcessiveThrottleEvent") {
    val sourceSchema: StructType = StructType(
      Array(
        StructField("id", LongType, nullable = false)
        , StructField("timestamp", StringType, nullable = false)
        , StructField("attributes", StructType(
          Array(
            StructField("tenantId", StringType, nullable = false)
            , StructField("deviceId", StringType, nullable = false)
            , StructField("manufacturer", StringType, nullable = false)
            , StructField("model", StringType, nullable = false)
            , StructField("identifier", StringType, nullable = false)
          )
        ), nullable = false)
        , StructField("can", StructType(Array(StructField("vehicle", StructType(Array(StructField("pedals", StructType(Array(StructField("throttle", StructType(Array(StructField("level", IntegerType, nullable = false))), nullable = false))), nullable = false))), nullable = false))), nullable = false)
        , StructField("gnss", StructType(
          Array(
            StructField("type", StringType, nullable = false)
            , StructField("coordinate", StructType(
              Array(
                StructField("lat", DoubleType, nullable = false)
                , StructField("lng", DoubleType, nullable = false)
              )), nullable = false)
            , StructField("address", StringType, nullable = false)
          )), nullable = false)
      )
    )

    it("The throttle level is equal 20% threshold. The event is created") {
      val sourceDF: DataFrame = jsonToDF(
        List(
          """{"id":1628717018247143424
            |, "timestamp":"2023-02-23T11:22:50Z"
            |,"attributes": {
            |           "tenantId": "763738558589566976"
            |           , "deviceId": "1440702360799186944"
            |           , "manufacturer": "Teltonika"
            |           , "model": "TeltonikaFMB001"
            |           , "identifier": "352094083025970TSC"
            |}
            |,"can":{"vehicle":{"pedals":{"throttle":{"level":20}}}}
            |,"gnss":{"type":"Gps"
            |          ,"coordinate":{"lat":18.444129,"lng":-69.255797}
            |          ,"address":"Dirección de prueba"
            |}
            |}""".stripMargin
        )
        , sourceSchema
      )

      val actualDF: DataFrame = EventsHelper.createExcessiveThrottleEvent()(sourceDF)

      val expectedDF: DataFrame = jsonToDF(
        List(
          """{"id":"1", "device_id":1440702360799186944, "created":"2023-02-23 11:22:50", "type_id":1, "location_address":"Dirección de prueba", "location_latitude":18.444129, "location_longitude":-69.255797, "value":"20%" }"""
        )
        , event_schema
      )

      //The ids are set on the fly so we can not compare them
      assertSmallDataFrameEquality(actualDF.drop("id"), expectedDF.drop("id"), ignoreNullable = true)
    }

    it("The throttle level is over 20% threshold. The event is created") {
      val sourceDF: DataFrame = jsonToDF(
        List(
          """{"id":1628717018247143424
            |, "timestamp":"2023-02-23T11:22:50Z"
            |,"attributes": {
            |           "tenantId": "763738558589566976"
            |           , "deviceId": "1440702360799186944"
            |           , "manufacturer": "Teltonika"
            |           , "model": "TeltonikaFMB001"
            |           , "identifier": "352094083025970TSC"
            |}
            |,"can":{"vehicle":{"pedals":{"throttle":{"level":21}}}}
            |,"gnss":{"type":"Gps"
            |          ,"coordinate":{"lat":18.444129,"lng":-69.255797}
            |          ,"address":"Dirección de prueba"
            |}
            |}""".stripMargin
        )
        , sourceSchema
      )

      val actualDF: DataFrame = EventsHelper.createExcessiveThrottleEvent()(sourceDF)

      val expectedDF: DataFrame = jsonToDF(
        List(
          """{"id":"1", "device_id":1440702360799186944, "created":"2023-02-23 11:22:50", "type_id":1, "location_address":"Dirección de prueba", "location_latitude":18.444129, "location_longitude":-69.255797, "value":"21%" }"""
        )
        , event_schema
      )

      assertSmallDataFrameEquality(actualDF.drop("id"), expectedDF.drop("id"), ignoreNullable = true)
    }

    it("The throttle level is under 20% threshold. The event is not created") {
      val sourceDF: DataFrame = jsonToDF(
        List(
          """{"id":1628717018247143424
            |, "timestamp":"2023-02-23T11:22:50Z"
            |,"attributes": {
            |           "tenantId": "763738558589566976"
            |           , "deviceId": "1328414834680696832"
            |           , "manufacturer": "Teltonika"
            |           , "model": "TeltonikaFMB001"
            |           , "identifier": "352094083025970TSC"
            |}
            |,"can":{"vehicle":{"pedals":{"throttle":{"level":19}}}}
            |,"gnss":{"type":"Gps"
            |          ,"coordinate":{"lat":18.444129,"lng":-69.255797}
            |          ,"address":"Dirección de prueba"
            |}
            |}""".stripMargin
        )
        , sourceSchema
      )

      val actualDF: DataFrame = EventsHelper.createExcessiveThrottleEvent()(sourceDF)

      assert(actualDF.count() === 0)
    }
  }

  describe("createFuelStealingEvent") {
    val sourceSchema: StructType = StructType(
      Array(
         StructField("timestamp", StringType, nullable = false)
        , StructField("attributes", StructType(
          Array(
             StructField("deviceId", StringType, nullable = false)
          )
        ), nullable = false)
        , StructField("can", StructType(Array(StructField("fuel", StructType(Array(StructField("level", IntegerType, nullable = false))), nullable = false))), nullable = false)
        , StructField("gnss", StructType(
          Array(
             StructField("coordinate", StructType(
              Array(
                StructField("lat", DoubleType, nullable = false)
                , StructField("lng", DoubleType, nullable = false)
              )), nullable = false)
            , StructField("address", StringType, nullable = false)
          )), nullable = false)
      )
    )

    it("The fuel drop out 5% in 5 minutes. The frames are ordered. The event is created") {
      val sourceDF: DataFrame = jsonToDF(
        List(
          """{"timestamp":"2023-02-22 14:50:58"
            |  ,"attributes":{"deviceId":"1585401650862903296"}
            |  ,"can":{"fuel":{"level":64}}
            |  ,"gnss":{"coordinate":{"lat":18.444129,"lng":-69.255797}, "address":"Dirección de prueba","satellites":11}
            |}
            |""".stripMargin
          ,
          """{"timestamp":"2023-02-22 14:53:58"
            |  ,"attributes":{"deviceId":"1585401650862903296"}
            |  ,"can":{"fuel":{"level":62}}
            |  ,"gnss":{"coordinate":{"lat":18.444129,"lng":-69.255797}, "address":"Dirección de prueba","satellites":11}
            |}
            |""".stripMargin
          ,
          """{"timestamp":"2023-02-22 14:55:58"
            |  ,"attributes":{"deviceId":"1585401650862903296"}
            |  ,"can":{"fuel":{"level":59}}
            |  ,"gnss":{"coordinate":{"lat":18.444129,"lng":-69.255797}, "address":"Dirección de prueba","satellites":11}
            |}
            |""".stripMargin)
        , sourceSchema)

      val actualDF = EventsHelper.createFuelStealingEvent()(sourceDF)

      val expectedDF: DataFrame = jsonToDF(
        List(
          """{"id":"1", "device_id":1585401650862903296, "created":"2023-02-22 14:55:58", "type_id":2, "location_address":"Dirección de prueba", "location_latitude":18.444129, "location_longitude":-69.255797, "value":"5% in less or 5 minutes" }"""
        )
        , event_schema
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)
    }

    it("The fuel drop out 5% in less than minutes. The frames are ordered. The event is created") {
      val sourceDF: DataFrame = jsonToDF(
        List(
          """{"timestamp":"2023-02-22 14:50:57"
            |  ,"attributes":{"deviceId":"1585401650862903296"}
            |  ,"can":{"fuel":{"level":64}}
            |  ,"gnss":{"coordinate":{"lat":18.444129,"lng":-69.255797}, "address":"Dirección de prueba","satellites":11}
            |}
            |""".stripMargin
          ,
          """{"timestamp":"2023-02-22 14:53:58"
            |  ,"attributes":{"deviceId":"1585401650862903296"}
            |  ,"can":{"fuel":{"level":62}}
            |  ,"gnss":{"coordinate":{"lat":18.444129,"lng":-69.255797}, "address":"Dirección de prueba","satellites":11}
            |}
            |""".stripMargin
          ,
          """{"timestamp":"2023-02-22 14:55:58"
            |  ,"attributes":{"deviceId":"1585401650862903296"}
            |  ,"can":{"fuel":{"level":59}}
            |  ,"gnss":{"coordinate":{"lat":18.444129,"lng":-69.255797}, "address":"Dirección de prueba","satellites":11}
            |}
            |""".stripMargin)
        , sourceSchema)

      val actualDF = EventsHelper.createFuelStealingEvent()(sourceDF)

      val expectedDF: DataFrame = jsonToDF(
        List(
          """{"id":"1", "device_id":1585401650862903296, "created":"2023-02-22 14:55:58", "type_id":2, "location_address":"Dirección de prueba", "location_latitude":18.444129, "location_longitude":-69.255797, "value":"5% in less or 5 minutes" }"""
        )
        , event_schema
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)
    }

    it("The fuel drop out 4% in 5 minutes. The event is not create ") {
      val sourceDF: DataFrame = jsonToDF(
        List(
          """{"timestamp":"2023-02-22 14:50:58"
            |  ,"attributes":{"deviceId":"1585401650862903296"}
            |  ,"can":{"fuel":{"level":64}}
            |  ,"gnss":{"coordinate":{"lat":18.444129,"lng":-69.255797}, "address":"Dirección de prueba","satellites":11}
            |}
            |""".stripMargin
          ,
          """{"timestamp":"2023-02-22 14:53:58"
            |  ,"attributes":{"deviceId":"1585401650862903296"}
            |  ,"can":{"fuel":{"level":62}}
            |  ,"gnss":{"coordinate":{"lat":18.444129,"lng":-69.255797}, "address":"Dirección de prueba","satellites":11}
            |}
            |""".stripMargin
          ,
          """{"timestamp":"2023-02-22 14:55:58"
            |  ,"attributes":{"deviceId":"1585401650862903296"}
            |  ,"can":{"fuel":{"level":60}}
            |  ,"gnss":{"coordinate":{"lat":18.444129,"lng":-69.255797}, "address":"Dirección de prueba","satellites":11}
            |}
            |""".stripMargin)
        , sourceSchema)

      val actualDF = EventsHelper.createFuelStealingEvent()(sourceDF)

      assert(actualDF.count() === 0)
    }

    it("The fuel drop out 5% in 5 minutes. The frames are not ordered. The event is created") {
      val sourceDF: DataFrame = jsonToDF(
        List(
          """{"timestamp":"2023-02-22 14:55:58"
            |  ,"attributes":{"deviceId":"1585401650862903296"}
            |  ,"can":{"fuel":{"level":59}}
            |  ,"gnss":{"coordinate":{"lat":18.444129,"lng":-69.255797}, "address":"Dirección de prueba","satellites":11}
            |}
            |""".stripMargin
          ,
          """{"timestamp":"2023-02-22 14:50:58"
            |  ,"attributes":{"deviceId":"1585401650862903296"}
            |  ,"can":{"fuel":{"level":64}}
            |  ,"gnss":{"coordinate":{"lat":18.444129,"lng":-69.255797}, "address":"Dirección de prueba","satellites":11}
            |}
            |""".stripMargin
          ,
          """{"timestamp":"2023-02-22 14:53:58"
            |  ,"attributes":{"deviceId":"1585401650862903296"}
            |  ,"can":{"fuel":{"level":62}}
            |  ,"gnss":{"coordinate":{"lat":18.444129,"lng":-69.255797}, "address":"Dirección de prueba","satellites":11}
            |}
            |""".stripMargin
          )
        , sourceSchema)

      val actualDF = EventsHelper.createFuelStealingEvent()(sourceDF)

      val expectedDF: DataFrame = jsonToDF(
        List(
          """{"id":"1", "device_id":1585401650862903296, "created":"2023-02-22 14:55:58", "type_id":2, "location_address":"Dirección de prueba", "location_latitude":18.444129, "location_longitude":-69.255797, "value":"5% in less or 5 minutes" }"""
        )
        , event_schema
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)
    }

    it("The fuel drop out 5% in 5 minutes. Frames are have no order  and there frames from different devices. The event is created") {
      val sourceDF: DataFrame = jsonToDF(
        List(
          """{"timestamp":"2023-02-22 14:55:58"
            |  ,"attributes":{"deviceId":"1585401650862903296"}
            |  ,"can":{"fuel":{"level":59}}
            |  ,"gnss":{"coordinate":{"lat":18.444129,"lng":-69.255797}, "address":"Dirección de prueba","satellites":11}
            |}
            |""".stripMargin
          ,
          """{"timestamp":"2023-02-22 14:52:58"
            |  ,"attributes":{"deviceId":"6585401650862903297"}
            |  ,"can":{"fuel":{"level":64}}
            |  ,"gnss":{"coordinate":{"lat":18.444129,"lng":-69.255797}, "address":"Dirección de prueba","satellites":11}
            |}
            |""".stripMargin
          ,
          """{"timestamp":"2023-02-22 14:50:58"
            |  ,"attributes":{"deviceId":"1585401650862903296"}
            |  ,"can":{"fuel":{"level":64}}
            |  ,"gnss":{"coordinate":{"lat":18.444129,"lng":-69.255797}, "address":"Dirección de prueba","satellites":11}
            |}
            |""".stripMargin
          ,
          """{"timestamp":"2023-02-22 14:49:58"
            |  ,"attributes":{"deviceId":"7585401650832913297"}
            |  ,"can":{"fuel":{"level":64}}
            |  ,"gnss":{"coordinate":{"lat":18.444129,"lng":-69.255797}, "address":"Dirección de prueba","satellites":11}
            |}
            |""".stripMargin
          ,
          """{"timestamp":"2023-02-22 14:51:58"
            |  ,"attributes":{"deviceId":"6585401650832913298"}
            |  ,"can":{"fuel":{"level":64}}
            |  ,"gnss":{"coordinate":{"lat":18.444129,"lng":-69.255797}, "address":"Dirección de prueba","satellites":11}
            |}
            |""".stripMargin
          ,
          """{"timestamp":"2023-02-22 14:53:58"
            |  ,"attributes":{"deviceId":"1585401650862903296"}
            |  ,"can":{"fuel":{"level":62}}
            |  ,"gnss":{"coordinate":{"lat":18.444129,"lng":-69.255797}, "address":"Dirección de prueba","satellites":11}
            |}
            |""".stripMargin
        )
        , sourceSchema)

      val actualDF = EventsHelper.createFuelStealingEvent()(sourceDF)

      val expectedDF: DataFrame = jsonToDF(
        List(
          """{"id":"1", "device_id":1585401650862903296, "created":"2023-02-22 14:55:58", "type_id":2, "location_address":"Dirección de prueba", "location_latitude":18.444129, "location_longitude":-69.255797, "value":"5% in less or 5 minutes" }"""
        )
        , event_schema
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)
    }
  }

  describe("checkFuelStealing"){
    it("There are not previous states. The state is update but no event is created") {
      val data:FuelStealingEventData = FuelStealingEventData(1L, new Timestamp(0L), "address", 1.1, 1.2, 100)
      val states:List[FuelStealingEventState] = List()
      val result = checkFuelStealing(data, states)

      result._1.length should be (1)
      result._1.head.timestamp.getTime should be (0L)
      result._2 should be (null)
    }

    it("There is a previous state but is not older than 5 minutes. The state is update but no event is created") {
      //se añade la entrada a los estados
      val data: FuelStealingEventData = FuelStealingEventData(1L, new Timestamp(4*60000L + 59000L), "address new", 1.1, 1.2, 95)
      val states: List[FuelStealingEventState] = List(FuelStealingEventState(1L, new Timestamp(0L), "address old", 1.9, 2.9, 100))//cuatro minutos y 59 segundos de diferencia
      val result = checkFuelStealing(data, states)

      result._1.length should be (2)
      result._1.head.timestamp.getTime should be (0L)
      result._1.tail.head.timestamp.getTime should be (data.timestamp.getTime)
      result._2 should be (null)
    }

    it("There is a previous state but and is older than 5 minutes but the fuel level difference is not greater than 5%. The state is update but no event is created") {
      val data: FuelStealingEventData = FuelStealingEventData(1L, new Timestamp(5 * 60000L), "address new", 1.1, 1.2, 96)
      val states: List[FuelStealingEventState] = List(FuelStealingEventState(1L, new Timestamp(0L), "address old", 1.9, 2.9, 100)) //cuatro minutos y 59 segundos de diferencia
      val result = checkFuelStealing(data, states)

      result._1.length should be (1)
      result._1.head.timestamp.getTime should be (data.timestamp.getTime)
      result._2 should be (null)
    }

    it("There is a previous state but and is older than 5 minutes but the fuel level difference is greater than 5%. The state is update and an event is created") {
      val data: FuelStealingEventData = FuelStealingEventData(1L, new Timestamp(5* 60000L), "address new", 1.1, 1.2, 95)
      val states: List[FuelStealingEventState] = List(FuelStealingEventState(1L, new Timestamp(0L), "address old", 1.9, 2.9, 100)) //cuatro minutos y 59 segundos de diferencia
      val result = checkFuelStealing(data, states)

      result._1.length should be (1)
      result._1.head.timestamp.getTime should be (data.timestamp.getTime)
      result._2 should !==  (null)
      result._2.fuel_level_diff should be (5)
    }

  }

  describe("temp"){
    val sourceSchema: StructType = StructType(
      Array(
        StructField("timestamp", StringType, nullable = false)
        , StructField("attributes", StructType(
          Array(
            StructField("tenantId", StringType, nullable = false)//campo mínimo en attributes para poder hacer el sacar las deviceId y borrarla attributes. Ver CommonTelemetryHelper
            ,StructField("deviceId", StringType, nullable = false)
          )
        ), nullable = false)
        , StructField("can", StructType(Array(StructField("fuel", StructType(Array(StructField("level", IntegerType, nullable = false))), nullable = false))), nullable = false)
        , StructField("gnss", StructType(
          Array(
            StructField("type", StringType, nullable = false)//campo mínimo en GNSS para poder hacer el sacar las coordenas y dirección y borrarlas gnss. Ver CommonTelemetryHelper
            ,StructField("coordinate", StructType(
              Array(
                StructField("lat", DoubleType, nullable = false)
                , StructField("lng", DoubleType, nullable = false)
              )), nullable = false)
            , StructField("address", StringType, nullable = false)
          )), nullable = false)
      )
    )
    it("Transformaciones intermedias"){

      val sourceDF: DataFrame = jsonToDF(
        List(
          """{"timestamp":"2023-02-22T14:55:58Z"
            |  ,"attributes":{"tenantId":"123123564", "deviceId":"1585401650862903296"}
            |  ,"can":{"fuel":{"level":62}}
            |  ,"gnss":{"type":"GPS", "coordinate":{"lat":18.444129,"lng":-69.255797}, "address":"Dirección de prueba"}
            |}
            |""".stripMargin
        )
        , sourceSchema)

      val actualDF = EventsHelper.createFuelStealingEvent()(sourceDF)

      val expectedDF: DataFrame = jsonToDF(
        List(
          """{"id":"1", "device_id":1585401650862903296, "created":"2023-02-22 14:55:58", "type_id":2, "location_address":"Dirección de prueba", "location_latitude":18.444129, "location_longitude":-69.255797, "value":"5% in less or 5 minutes" }"""
        )
        , event_schema
      )

      assertSmallDataFrameEquality(actualDF.drop("id"), expectedDF.drop("id"), ignoreNullable = true)
    }
  }
}
