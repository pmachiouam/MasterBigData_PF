package org.pmachio

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.scalatest.funspec.AnyFunSpec
import org.apache.spark.sql.types._
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._

class StringHelperSpec extends AnyFunSpec with SparkSessionTestWrapper with DataFrameComparer {
describe ("withStartsWithA"){
  it("checks if a string starts with the letter a"){
    //createDF Viene de SparkSessionExt
    val sourceDF = spark.createDF(List(("apache"), ("animation"), ("bill")),List(("words", StringType, true)) )
    val actualDF = sourceDF.transform(StringHelper.withStartsWithA("words"))

    val expectedDF = spark.createDF(List(("apache", true), ("animation", true), ("bill", false)),
      List(("words", StringType, true), ("starts_with_a", BooleanType, true)))

    assertSmallDatasetEquality(actualDF, expectedDF)
  }
}
}
