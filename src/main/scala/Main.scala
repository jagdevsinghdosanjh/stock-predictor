import org.apache.spark.sql.SparkSession
import data.CSVLoader
import features.FeatureEngineering
import model.LinearRegressionModel
import ollama.OllamaClient
import analytics.StockAnalytics
import utils.CSVDownloader

object Main {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("StockPredictor")
      .master("local[*]")
      .getOrCreate()

    try {
      // ---------------------------------------------------------
      // STEP 1 — Download CSV manually (avoids Streamlit redirect)
      // ---------------------------------------------------------
      val remoteUrl = "https://your-static-host/AMD_data.csv"   // CHANGE THIS
      val localPath = "data/AMD_data.csv"

      CSVDownloader.download(remoteUrl, localPath)

      // ---------------------------------------------------------
      // STEP 2 — Load CSV from local file (Spark-friendly)
      // ---------------------------------------------------------
      val df = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(localPath)

      // Validate schema before continuing
      val requiredCols = Seq("Open", "High", "Low", "Close", "Volume", "date")
      val missing = requiredCols.filterNot(df.columns.contains)

      if (missing.nonEmpty) {
        println(s"❌ Missing columns: ${missing.mkString(", ")}")
        println("❌ The downloaded file is NOT a valid CSV.")
        spark.stop()
        return
      }

      // ---------------------------------------------------------
      // STEP 3 — Feature Engineering (ML + Analytics)
      // ---------------------------------------------------------
      val feDF = FeatureEngineering.addFeatures(df)

      // ---------------------------------------------------------
      // STEP 4 — ML Model
      // ---------------------------------------------------------
      val (_, predictions) = LinearRegressionModel.train(feDF)
      predictions.show(10, false)

      // ---------------------------------------------------------
      // STEP 5 — Analytics (Trend, Spikes, Volatility, JSON)
      // ---------------------------------------------------------
      val jsonSummary = FeatureEngineering.analyticsSummary(df, "AMD")
      val tierB      = FeatureEngineering.tierBLesson(df, "AMD")
      val tierC      = FeatureEngineering.tierCCaseStudy(df, "AMD")

      println("\n=== JSON SUMMARY ===")
      println(jsonSummary)

      println("\n=== TIER‑B LESSON ===")
      println(tierB)

      println("\n=== TIER‑C CASE STUDY ===")
      println(tierC)

      // ---------------------------------------------------------
      // STEP 6 — Ollama Analysis
      // ---------------------------------------------------------
      val ollamaSummary = OllamaClient.ask(
        s"Analyze this stock prediction summary: ${predictions.take(5).mkString}"
      )

      println("\n=== OLLAMA ANALYSIS ===")
      println(ollamaSummary)

    } catch {
      case e: Exception =>
        println("\n❌ Pipeline failed:")
        println(e.getMessage)
    } finally {
      spark.stop()
    }
  }
}
