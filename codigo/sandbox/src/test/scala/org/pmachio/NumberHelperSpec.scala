package org.pmachio

import com.github.mrpowers.spark.fast.tests.ColumnComparer
import org.scalatest.funspec.AnyFunSpec

class NumberHelperSpec extends AnyFunSpec with SparkSessionTestWrapper with ColumnComparer{

  import spark.implicits._
  describe("isEven"){
    it("returns true for even numbers"){
      val df = Seq((2, true), (3, false), (6, true), (5, false)).toDF("nums", "expected")
      val rest = df.transform(NumberHelper.isEven("nums", "is_even"))// para depurar.show()
      assertColumnEquality(rest, "expected", "is_even")
    }
  }
}
