package org.uam.masterbigdata

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, to_date, to_timestamp}

object DateHelper {
  def convertToDate(inputColName: String, outputColName: String)(df: DataFrame) =
    df.withColumn(outputColName, to_timestamp(col(inputColName), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
}
