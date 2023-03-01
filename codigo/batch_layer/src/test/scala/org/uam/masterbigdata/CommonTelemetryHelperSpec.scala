package org.uam.masterbigdata

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.scalatest.funspec.AnyFunSpec

class CommonTelemetryHelperSpec extends AnyFunSpec
  with DataFrameComparer
  with SparkSessionTestWrapper
  with DataFrameTestHelper {
  describe("flatMainFields") {
    val source_schema: StructType = StructType(
      Array(
        StructField("timestamp", StringType, nullable = false)
        ,StructField("gnss", StructType(
          Array(
            StructField("type", StringType, nullable = false)
            , StructField("coordinate", StructType(
              Array(
                StructField("lat", DoubleType, nullable = false)
                , StructField("lng", DoubleType, nullable = false)
              )
            ))
            , StructField("altitude", DoubleType)
            , StructField("speed", IntegerType)
            , StructField("course", IntegerType)
            , StructField("address", StringType)
            , StructField("satellites", IntegerType)
          )
        ))
      )
    )

    val expected_schema: StructType = StructType(
      Array(
        StructField("timestamp", TimestampType, nullable = false)
        , StructField("gnss", StructType(
          Array(
            StructField("type", StringType, nullable = false)
            , StructField("altitude", DoubleType)
            , StructField("speed", IntegerType)
            , StructField("course", IntegerType)
            , StructField("satellites", IntegerType)
          )
        ))
        , StructField("location_address", StringType, nullable = false)
        , StructField("location_latitude", DoubleType, nullable = false)
        , StructField("location_longitude", DoubleType, nullable = false)
      )
    )

    it("Flats the main fields for the journeys analisis") {
      val sourceDF = jsonToDF(
        List(
          """{"timestamp":"2023-02-05T10:58:27Z"
            |,"gnss":{
            |          "type":"Gps"
            |          ,"coordinate":{
            |                          "lat":40.605956
            |                          ,"lng":-3.711923
            |                        }
            |          ,"altitude":722.0
            |          ,"speed":0
            |          ,"course":212
            |          ,"address":"Avenida de la Vega, Tres Cantos, Comunidad de Madrid, 28760, Espa�a"
            |          ,"satellites":13
            |}}""".stripMargin
        )
        , source_schema)

      val actualDF = CommonTelemetryHelper.flatLocationsFields()(sourceDF)

      val expectedDF = jsonToDF(
        List(
          """{
            |"timestamp":"2023-02-05 10:58:27"
            |,"gnss":{
            |          "type":"Gps"
            |          ,"altitude":722.0
            |          ,"speed":0
            |          ,"course":212
            |          ,"satellites":13
            |}
            |,"location_address":"Avenida de la Vega, Tres Cantos, Comunidad de Madrid, 28760, Espa�a"
            |,"location_latitude":40.605956
            |,"location_longitude":-3.711923
            |}""".stripMargin
        )
        , expected_schema)

      assertSmallDataFrameEquality(actualDF, expectedDF)
    }
  }

}
