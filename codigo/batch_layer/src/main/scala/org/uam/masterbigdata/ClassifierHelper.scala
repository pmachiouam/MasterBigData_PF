package org.uam.masterbigdata


import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.tuning.CrossValidator
import org.apache.spark.ml.tuning.ParamGridBuilder

object ClassifierHelper extends Schemas{
  /**Generamos datos a partir de unos trayectos originales con una sensibilidad de unos 111m para la latitud y longitud
   * http://wiki.gis.com/wiki/index.php/Decimal_degrees */
  def generateData(originalDataFrame:DataFrame): DataFrame = {
    originalDataFrame.withColumn("new_col", explode(array((1 until 200).map(lit): _*)))
      .drop("new_col")
      .withColumn("start_location_latitude",
        when(rand() > 0.5, col("start_location_latitude") + lit(rand() * 0.001))
          .otherwise(col("start_location_latitude") - lit(rand() * 0.001))
      )
      .withColumn("start_location_longitude"
        , when(rand() > 0.5, col("start_location_longitude") + lit(rand() * 0.001))
          .otherwise(col("start_location_longitude") - lit(rand() * 0.001))
      )
      .withColumn("end_location_latitude"
        , when(rand() > 0.5, col("end_location_latitude") + lit(rand() * 0.001))
          .otherwise(col("end_location_latitude") - lit(rand() * 0.001))
      )
      .withColumn("end_location_longitude"
        , when(rand() > 0.5, col("end_location_longitude") + lit(rand() * 0.001))
          .otherwise(col("end_location_longitude") - lit(rand() * 0.001))
      )
  }

  //Crear el pipeline y guardarlo.
  def journeysClassification_LogReg(journeysToLearnFrom:DataFrame) = {
    val stringIndexer = new StringIndexer()
    stringIndexer.setInputCol("label")
    stringIndexer.setOutputCol("label_ind")
    //val journeysToLearnFrom_ind_tr = stringIndexer.fit(journeysToLearnFrom)
    //val journeysToLearnFrom_ind = journeysToLearnFrom_ind_tr.transform(journeysToLearnFrom)

     val vectorAssembler:VectorAssembler = new VectorAssembler()
    vectorAssembler.setInputCols(Array("start_location_latitude", "start_location_longitude", "end_location_latitude", "end_location_longitude"))
    vectorAssembler.setOutputCol("features")
    //val dataML = vectorAssembler.transform(journeysToLearnFrom_ind)

    //Separación de los datos para entrenar
    val dataML_split = journeysToLearnFrom.randomSplit(Array(0.7, 0.3))

    val logisticRegression = new LogisticRegression()
    logisticRegression.setFeaturesCol("features")
    logisticRegression.setLabelCol("label_ind")
    logisticRegression.setRegParam(0.01)


    val pipeline = new Pipeline()
    pipeline.setStages(Array(stringIndexer, vectorAssembler, logisticRegression))
    val pipeline_tr = pipeline.fit(dataML_split(0))
    val pred_pipeline = pipeline_tr.transform(dataML_split(1))

    //val logisticRegression_tr:LogisticRegressionModel = logisticRegression.fit(dataML_split(0))

    //println(s"Accuracy ${logisticRegression_tr.summary.accuracy}")

    //val pred_logisticRegression:DataFrame = logisticRegression_tr.transform(dataML_split(1))

    //pred_logisticRegression.show()

    val evaluator = new MulticlassClassificationEvaluator()
    evaluator.setLabelCol("label_ind")
    println(s"Test - F1 ${evaluator.evaluate(pred_pipeline)}")
    evaluator.setMetricName("weightedPrecision")
    println(s"Test - Precision ${evaluator.evaluate(pred_pipeline)}")
    evaluator.setMetricName("weightedRecall")
    println(s"Test - Recall ${evaluator.evaluate(pred_pipeline)}")
    evaluator.setMetricName("accuracy")
    println(s"Test - accuracy ${evaluator.evaluate(pred_pipeline)}")

    //logisticRegression_tr
    pred_pipeline
  }

  //recuperar el pipeline
}
