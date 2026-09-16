package analytics

import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object StockAnalytics {

  // ---------------------------------------------------------
  // MOVING AVERAGES
  // ---------------------------------------------------------
  def addMovingAverages(df: DataFrame): DataFrame = {
    val wShort = Window.orderBy(col("date")).rowsBetween(-19, 0) // 20-day MA
    val wLong  = Window.orderBy(col("date")).rowsBetween(-49, 0) // 50-day MA

    df.withColumn("MA_Short", avg(col("Close")).over(wShort))
      .withColumn("MA_Long",  avg(col("Close")).over(wLong))
  }

  // ---------------------------------------------------------
  // DATE FILTERING (placeholder)
  // ---------------------------------------------------------
  def filterByDate(df: DataFrame): DataFrame = df

  // ---------------------------------------------------------
  // KEY STATISTICS
  // ---------------------------------------------------------
  case class KeyStats(
    latestClose: Double,
    avgVolume: Double,
    volatility: Double
  )

  def computeKeyStatistics(df: DataFrame): KeyStats = {
    val latestClose = df.orderBy(col("date").desc).select("Close").first().getDouble(0)
    val avgVolume   = df.select(avg(col("Volume"))).first().getDouble(0)
    val volatility  = df.select(stddev(col("Close"))).first().getDouble(0)

    KeyStats(
      latestClose = BigDecimal(latestClose).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble,
      avgVolume   = BigDecimal(avgVolume).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble,
      volatility  = BigDecimal(volatility).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
    )
  }

  // ---------------------------------------------------------
  // TREND DETECTION
  // ---------------------------------------------------------
  def detectTrend(df: DataFrame): String = {
    val first = df.orderBy(col("date").asc).select("Close").first().getDouble(0)
    val last  = df.orderBy(col("date").desc).select("Close").first().getDouble(0)
    if (last > first) "Uptrend" else "Downtrend"
  }

  // ---------------------------------------------------------
  // SPIKE DETECTION
  // ---------------------------------------------------------
  def detectSpikes(df: DataFrame): Seq[String] = {
    val avgVol    = df.select(avg(col("Volume"))).first().getDouble(0)
    val threshold = avgVol * 2.0

    df.filter(col("Volume") > threshold)
      .select(col("date").cast("string"))
      .collect()
      .map(_.getString(0))
      .toSeq
  }

  // ---------------------------------------------------------
  // VOLATILITY DETECTION
  // ---------------------------------------------------------
  def detectVolatility(df: DataFrame): Double = {
    df.select(stddev(col("Close"))).first().getDouble(0)
  }

  // ---------------------------------------------------------
  // JSON SUMMARY (Scala Map)
  // ---------------------------------------------------------
  def generateJsonSummary(df: DataFrame, ticker: String): Map[String, Any] = {
    val stats       = computeKeyStatistics(df)
    val trend       = detectTrend(df)
    val spikeDays   = detectSpikes(df)

    Map(
      "ticker"          -> ticker,
      "latest_close"    -> stats.latestClose,
      "avg_volume"      -> stats.avgVolume,
      "volatility"      -> stats.volatility,
      "trend_direction" -> trend,
      "spike_days"      -> spikeDays
    )
  }

  // ---------------------------------------------------------
  // TIER‑B LESSON
  // ---------------------------------------------------------
  def generateTierBLesson(df: DataFrame, ticker: String): String = {
    s"""
### Understanding $ticker

This chart shows how the price of $ticker moves over time.

- **Open, High, Low, Close** show daily price movement.
- **Volume spikes** indicate strong buying or selling.
- **Moving averages** help identify long-term trends.
- **Uptrends** mean buyers dominate.
- **Downtrends** mean sellers dominate.

This helps students understand real market behaviour.
""".stripMargin
  }

  // ---------------------------------------------------------
  // TIER‑C CASE STUDY
  // ---------------------------------------------------------
  def generateTierCCaseStudy(df: DataFrame, ticker: String): String = {
    val firstClose = df.orderBy(col("date").asc).select("Close").first().getDouble(0)
    val lastClose  = df.orderBy(col("date").desc).select("Close").first().getDouble(0)
    val slope      = BigDecimal(lastClose - firstClose).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
    val vol        = BigDecimal(detectVolatility(df)).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
    val spikes     = detectSpikes(df).length

    s"""
## Premium Case Study: $ticker

### 1. Market Cycle Analysis
We identify phases like accumulation, uptrend, distribution, correction, and base formation.

### 2. Statistical Interpretation
- Trend slope: $slope
- Volatility: $vol
- Volume anomalies: $spikes spike days

### 3. Economic Interpretation
We connect price behaviour with macro factors, sector cycles, and company fundamentals.

### 4. Q&A Prompts
- Why did volume spike on certain dates?
- What does a flattening moving average indicate?
- How do we identify a distribution zone?
- What signals a capitulation bottom?
""".stripMargin
  }
}
