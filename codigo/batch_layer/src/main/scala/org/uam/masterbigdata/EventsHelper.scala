package org.uam.masterbigdata

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, expr, lit}
object EventsHelper {
  /** Creates a event based on the abusive throttle use (more than 20%) */
  def createExcessiveThrottleEvent()(df: DataFrame): DataFrame = {
    df.transform(CommonTelemetryHelper.flatBasicFields())
      .where(col("can").getField("vehicle").getField("pedals").getField("throttle").getField("level") >= 20)
      .withColumn("value", expr("concat( cast( can.vehicle.pedals.throttle.level as string ), '%' )" ))
      .select(
        expr("uuid()").as("id")
        , col("device_id")
        , col("timestamp").as("created")
        , lit(1L).as("type_id")
        , col("location_address")
        , col("location_latitude")
        , col("location_longitude")
        , col("value")
      )
  }

  def createFuelStealingEvent()(df:DataFrame):DataFrame = {
    ???
  }

}
