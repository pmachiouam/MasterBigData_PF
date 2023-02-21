package org.uam.masterbigdata

import org.apache.spark.sql.types.{StructType, StructField, LongType, StringType, BooleanType, DoubleType, IntegerType, TimestampType}

trait Schemas {

  val telemetry_schema: StructType = StructType(
    Array(
      StructField("id", LongType, nullable = false)
      , StructField("version", StringType, nullable = false)
      , StructField("timestamp", StringType, nullable = false)
      , StructField("server", StructType(
        Array(
          StructField("timestamp", StringType, nullable = false)
        )
      )
      )
      , StructField("attributes", StructType(
        Array(
          StructField("tenantId", StringType, nullable = false)
          , StructField("deviceId", StringType, nullable = false)
          , StructField("manufacturer", StringType, nullable = false)
          , StructField("model", StringType, nullable = false)
          , StructField("identifier", StringType, nullable = false)
        )
      ), nullable = false)
      , StructField("device", StructType(
        Array(
          StructField("battery", StructType(
            Array(
              StructField("voltage", LongType)
              , StructField("level", LongType)
            )
          ))
          , StructField("mileage", StructType(
            Array(
              StructField("distance", LongType)
            )
          ))
        )
      ))
      , StructField("can", StructType(
        Array(
          StructField("vehicle", StructType(
            Array(
              StructField("mileage", StructType(
                Array(
                  StructField("mileage", LongType)
                )
              ))
              , StructField("cruise", StructType(
                Array(
                  StructField("status", BooleanType)
                )
              ))
              , StructField("handBrake", StructType(
                Array(
                  StructField("status", BooleanType)
                )
              ))
              , StructField("doors", StructType(
                Array(
                  StructField("indicator", StructType(
                    Array(
                      StructField("status", BooleanType)
                    )
                  ))
                )
              ))
              , StructField("lights", StructType(
                Array(
                  StructField("hazard", StructType(
                    Array(
                      StructField("status", BooleanType)
                    )
                  ))
                  , StructField("fog", StructType(
                    Array(
                      StructField("status", BooleanType)
                    )
                  ))
                )
              ))
              , StructField("seatBelts", StructType(
                Array(
                  StructField("indicator", StructType(
                    Array(
                      StructField("status", BooleanType)
                    )
                  ))
                )
              ))
              , StructField("beams", StructType(
                Array(
                  StructField("high", StructType(
                    Array(
                      StructField("status", BooleanType)
                    )
                  ))
                )
              ))
              , StructField("lock", StructType(
                Array(
                  StructField("central", StructType(
                    Array(
                      StructField("status", BooleanType)
                    )
                  ))
                )
              ))
              , StructField("gear", StructType(
                Array(
                  StructField("reverse", StructType(
                    Array(
                      StructField("status", BooleanType)
                    )
                  ))
                )
              ))
              , StructField("airConditioning", StructType(
                Array(
                  StructField("status", BooleanType)
                )
              ))
            )
          )
          )
          , StructField("engine", StructType(
            Array(
              StructField("time", StructType(
                Array(
                  StructField("duration", LongType)
                )
              ))
            )
          ))
          , StructField("battery", StructType(
            Array(
              StructField("charging", StructType(
                Array(
                  StructField("status", BooleanType)
                )
              ))
            )
          ))
        )
      ))
      , StructField("gnss", StructType(
        Array(
          StructField("type", StringType, nullable = false)
          , StructField("coordinate", StructType(
            Array(
              StructField("lat", DoubleType, nullable = false)
              , StructField("lng", DoubleType, nullable = false)
            )
          )
          )
          , StructField("altitude", IntegerType)
          , StructField("speed", IntegerType)
          , StructField("course", IntegerType)
          , StructField("address", StringType)
          , StructField("satellites", IntegerType)
        )
      ))
      , StructField("ignition", StructType(
        Array(
          StructField("status", BooleanType)
        )
      )
      )
    )
  )


  val journey_schema:StructType = StructType(
    Array(
      StructField("id", StringType, nullable = false)
      ,StructField("deviceId", LongType, nullable = false)
      ,StructField("start_timestamp", TimestampType, nullable = false)
      ,StructField("start_location_address", StringType, nullable = false)
      ,StructField("start_location_latitude", DoubleType, nullable = false)
      ,StructField("start_location_longitude", DoubleType, nullable = false)
      ,StructField("end_timestamp", TimestampType, nullable = false)
      ,StructField("end_location_address", StringType, nullable = false)
      , StructField("end_location_latitude", DoubleType, nullable = false)
      , StructField("end_location_longitude", DoubleType, nullable = false)
      , StructField("distance", IntegerType, nullable = false)
      , StructField("label", StringType, nullable = false)
    )
  )
}