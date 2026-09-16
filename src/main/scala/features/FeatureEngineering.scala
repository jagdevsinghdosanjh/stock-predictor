package features

import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions._
import analytics.StockAnalytics

object FeatureEngineering {

  // ---------------------------------------------------------
  // CORE ML FEATURES
  // ---------------------------------------------------------
  def addFeatures(df: DataFrame): DataFrame = {
    val base = df
      .withColumn("Return", (col("Close") - col("Open")) / col("Open"))
      .withColumn("Volatility", (col("High") - col("Low")) / col("Open"))
      .withColumn("PriceRange", col("High") - col("Low"))

    // ---------------------------------------------------------
    // INTEGRATE ANALYTICS MODULE
    // ---------------------------------------------------------
    val withMA = StockAnalytics.addMovingAverages(base)

    val trend = StockAnalytics.detectTrend(base)
    val spikes = StockAnalytics.detectSpikes(base)
    val vol = StockAnalytics.detectVolatility(base)

    // Add analytics outputs as columns (Spark-friendly)
    val enriched = withMA
      .withColumn("TrendDirection", lit(trend))
      .withColumn("SpikeDays", lit(spikes.mkString(",")))
      .withColumn("CloseVolatility", lit(vol))

    enriched
  }

  // ---------------------------------------------------------
  // FULL ANALYTICS SUMMARY (OPTIONAL)
  // ---------------------------------------------------------
  def analyticsSummary(df: DataFrame, ticker: String): Map[String, Any] = {
    StockAnalytics.generateJsonSummary(df, ticker)
  }

  def tierBLesson(df: DataFrame, ticker: String): String = {
    StockAnalytics.generateTierBLesson(df, ticker)
  }

  def tierCCaseStudy(df: DataFrame, ticker: String): String = {
    StockAnalytics.generateTierCCaseStudy(df, ticker)
  }
}
