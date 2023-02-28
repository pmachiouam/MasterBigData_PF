package org.uam.masterbigdata

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.scalatest.funspec.AnyFunSpec

class EventsHelperSpec extends AnyFunSpec
  with DataFrameComparer
  with SparkSessionTestWrapper
  with DataFrameTestHelper
  with Schemas {
  describe("createExcessiveThrottleEvent"){
    val sourceSchema:StructType = StructType(
      Array(

      )
    )

    it("The throttle level is equal 20% threshold. The event is created"){
      val sourceDF:DataFrame = jsonToDF(
        List(
          """{"id":1628717018247143424
            |, "timestamp":"2023-02-23T11:22:50Z"
            |, "attributes":{"deviceId":"1440702360799186944"}
            |,"can":{"vehicle":{"pedals":{"throttle":{"level":20}}}}
            |,"gnss":{"type":"Gps","coordinate":{"lat":18.444129,"lng":-69.255797} }
            |,"gsm":{"rssi":65463}
            |,"ignition":{"status":true}
            |}""".stripMargin
        )
        ,sourceSchema
      )

      val actualDF:DataFrame = EventsHelper.createExcessiveThrottleEvent()(sourceDF)

      val expectedDF:DataFrame = jsonToDF(
        List(
        """{"id":1, "device_id":1440702360799186944, "created":"2023-02-23 11:22:50", "type_id":1, "location_address":"", "location_latitude":"", "location_longitude":"", "value":"20%" }"""
        )
        ,event_schema
      )

      //The ids are set on the fly so we can not compare them
      assertSmallDataFrameEquality(actualDF.drop("id"), expectedDF.drop("id"))
    }

    it("The throttle level is over 20% threshold. The event is created") {
      val sourceDF: DataFrame = jsonToDF(
        List(
          """{"id":1628717018247143424
            |, "timestamp":"2023-02-23T11:22:50Z"
            |, "attributes":{"deviceId":"1440702360799186944"}
            |,"can":{"vehicle":{"pedals":{"throttle":{"level":21}}}}
            |,"gnss":{"type":"Gps","coordinate":{"lat":18.444129,"lng":-69.255797}}
            |,"gsm":{"rssi":65463}
            |,"ignition":{"status":true}
            |}""".stripMargin
        )
        , sourceSchema
      )

      val actualDF: DataFrame = EventsHelper.createExcessiveThrottleEvent()(sourceDF)

      val expectedDF: DataFrame = jsonToDF(
        List(
          """{"id":1, "device_id":1440702360799186944, "created":"2023-02-23 11:22:50", "type_id":1, "location_address":"", "location_latitude":"", "location_longitude":"", "value":"21%" }"""
        )
        , event_schema
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)
    }

    it("The throttle level is under 20% threshold. The event is created") {
      val sourceDF: DataFrame = jsonToDF(
        List(
          """{"id":1628717018247143424
            |, "timestamp":"2023-02-23T11:22:50Z"
            |, "attributes":{"deviceId":"1440702360799186944"}
            |,"can":{"vehicle":{"pedals":{"throttle":{"level":19}}}}
            |,"gnss":{"type":"Gps","coordinate":{"lat":18.444129,"lng":-69.255797}}
            |,"gsm":{"rssi":65463}
            |,"ignition":{"status":true}
            |}""".stripMargin
        )
        , sourceSchema
      )

      val actualDF: DataFrame = EventsHelper.createExcessiveThrottleEvent()(sourceDF)

      assert(actualDF === 0)
    }
  }
}
