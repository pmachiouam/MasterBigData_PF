package org.pmachio

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
object StringHelper {
  def withStartsWithA(colName: String)(df: DataFrame): DataFrame = {
         df.withColumn("starts_with_a", col(colName).startsWith("a"))
    }

}
