package org.uam.masterbigdata

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object CommonTelemetryHelper {
  def flatLocationsFields()(df: DataFrame): DataFrame = {
    df.transform(DateHelper.convertToDate("timestamp", "timestamp"))
      .withColumn("location_address", col("gnss").getField("address"))
      .withColumn("location_latitude", col("gnss").getField("coordinate").getField("lat"))
      .withColumn("location_longitude", col("gnss").getField("coordinate").getField("lng"))
      .withColumn("gnss", col("gnss").dropFields("address", "coordinate"))
  }
}
