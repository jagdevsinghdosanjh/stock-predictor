package data

import org.apache.spark.sql.{DataFrame, SparkSession}
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.net.URI
import java.nio.file.{Files, Paths}

object CSVLoader {

  def loadCSV(spark: SparkSession, url: String): DataFrame = {

    val client = HttpClient.newBuilder()
      .followRedirects(HttpClient.Redirect.NORMAL)
      .build()

    val request = HttpRequest.newBuilder()
      .uri(URI.create(url))
      .GET()
      .build()

    val response = client.send(request, HttpResponse.BodyHandlers.ofByteArray())

    val tempPath = Files.createTempFile("stock-data", ".csv")
    Files.write(tempPath, response.body())

    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(tempPath.toString)
  }
}
