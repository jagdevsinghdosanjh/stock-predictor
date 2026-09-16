package model

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame

object LinearRegressionModel {

  def train(df: DataFrame): (LinearRegression, DataFrame) = {

    val assembler = new VectorAssembler()
      .setInputCols(Array("Open", "High", "Low", "Volume", "DayOfWeek"))
      .setOutputCol("features")

    val finalDF = assembler.transform(df).withColumnRenamed("Close", "label")

    val lr = new LinearRegression()
      .setMaxIter(50)
      .setRegParam(0.01)

    val model = lr.fit(finalDF)

    val predictions = model.transform(finalDF)

    (lr, predictions)
  }
}
