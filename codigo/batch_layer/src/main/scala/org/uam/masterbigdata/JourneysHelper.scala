package org.uam.masterbigdata

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{coalesce, col, first, lag, last, least, sum, when}

object JourneysHelper {

  def calculateJourneys()(df: DataFrame): DataFrame = {
    df.transform(setIgnitionStateChange())
      .transform(setGroupOfStateChangesToFrames())
      .transform(setInitialStateChangeValues())
      .transform(setFinalStateChangeValues())
      .where(col("ignition") =!= false)
    //agrupar y agregar
    //.groupBy(col("state_changed_group"))
    //añadir identificador creado por UUID
  }

  private val window_partition_by_deviceId_order_by_timestamp = Window
    .partitionBy(col("attributes").getField("deviceId"))
    .orderBy(col("timestamp"))
  private val window_partition_by_deviceId_and_by_stateChangedGroup_order_by_timestamp_unbound = Window
    .partitionBy(col("attributes").getField("deviceId"), col("state_changed_group"))
    .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    .orderBy(col("timestamp"))
  private val window_partition_by_deviceId_and_by_stateChangedGroup_order_by_timestamp = Window
    .partitionBy(col("attributes").getField("deviceId"), col("state_changed_group"))
    .orderBy(col("timestamp"))


  /** Creates a new column, state_changed, which marks the state change of column ignition compare with the previous one.
   * If there is a change the value is 1 if not the value is 0 */
  def setIgnitionStateChange()(df: DataFrame): DataFrame = {
    df.withColumn("state_changed"
      , when(col("ignition") === coalesce(lag(col("ignition"), 1).over(window_partition_by_deviceId_order_by_timestamp), col("ignition")), 0)
        .when(col("ignition") =!= coalesce(lag(col("ignition"), 1).over(window_partition_by_deviceId_order_by_timestamp), col("ignition")), 1)
    )
  }

  /** Creates a new column, state_changed_group, with a incremental identifier base on column state_changed.
   * It sum the value of column state_changed, so when there is a state change the identifier is increased by 1 */
  def setGroupOfStateChangesToFrames()(df: DataFrame): DataFrame = {
    df.withColumn("state_changed_group"
      , sum(col("state_changed")).over(window_partition_by_deviceId_order_by_timestamp)
    )
  }

  /**
   * Sets all the initial values, like the timestamp, latitude, longitude, address
   * */
  def setInitialStateChangeValues()(df: DataFrame): DataFrame = {
    df.withColumn("start_timestamp"
      , first(col("timestamp")).over(window_partition_by_deviceId_and_by_stateChangedGroup_order_by_timestamp_unbound)
    )
      .withColumn("start_location_address"
        , first(col("location_address")).over(window_partition_by_deviceId_and_by_stateChangedGroup_order_by_timestamp_unbound)
      )
      .withColumn("start_location_latitude"
        , first(col("location_latitude")).over(window_partition_by_deviceId_and_by_stateChangedGroup_order_by_timestamp_unbound)
      )
      .withColumn("start_location_longitude"
        , first(col("location_longitude")).over(window_partition_by_deviceId_and_by_stateChangedGroup_order_by_timestamp_unbound)
      )
  }

  /**
   * Sets all the final values, like the timestamp, latitude, longitude, address
   * */
  def setFinalStateChangeValues()(df: DataFrame): DataFrame = {
    df.withColumn("end_timestamp"
      , last(col("timestamp"), true).over(window_partition_by_deviceId_and_by_stateChangedGroup_order_by_timestamp_unbound)
    )
      .withColumn("end_location_address"
        , last(col("location_address"), true).over(window_partition_by_deviceId_and_by_stateChangedGroup_order_by_timestamp_unbound)
      )
      .withColumn("end_location_latitude"
        , last(col("location_latitude"), true).over(window_partition_by_deviceId_and_by_stateChangedGroup_order_by_timestamp_unbound)
      )
      .withColumn("end_location_longitude"
        , last(col("location_longitude"), true ).over(window_partition_by_deviceId_and_by_stateChangedGroup_order_by_timestamp_unbound)
      )
  }

  /**
   * */
  def setCountersValues()(df:DataFrame):DataFrame = {
    val canbusDistanceCol: Column = col("can").getField("vehicle").getField("mileage").getField("distance")
    val canbusConsumptionCol: Column = col("can").getField("fuel").getField("consumed").getField("volume")

    df.withColumn("distance"
      , canbusDistanceCol- coalesce(lag(canbusDistanceCol, 1).over(window_partition_by_deviceId_and_by_stateChangedGroup_order_by_timestamp), canbusDistanceCol)
    ).withColumn("consumption"
      , canbusConsumptionCol - coalesce(lag(canbusConsumptionCol, 1).over(window_partition_by_deviceId_and_by_stateChangedGroup_order_by_timestamp), canbusConsumptionCol)
    )
  }

  /**
   * Aggregates the initial and final values per StateChange and:
   * * Consumption
   * * Distance
   * */
  def aggregateStateChangeValues()(df:DataFrame):DataFrame = {
    ???
  }
}
