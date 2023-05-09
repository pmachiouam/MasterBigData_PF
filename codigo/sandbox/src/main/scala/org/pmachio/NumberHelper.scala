package org.pmachio

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object NumberHelper {
  def isEven(inputColName: String, outputColName: String)(df: DataFrame): DataFrame = {
    df.withColumn(outputColName, col(inputColName) % 2 === lit(0))
  }
}
