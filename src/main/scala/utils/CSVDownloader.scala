package utils

import java.net.URL
import java.nio.file.{Files, Paths}

object CSVDownloader {

  def download(url: String, outputPath: String): Unit = {
    println(s"Downloading CSV from: $url")

    val in = new URL(url).openStream()
    val outPath = Paths.get(outputPath)

    Files.copy(in, outPath, java.nio.file.StandardCopyOption.REPLACE_EXISTING)
    in.close()

    println(s"Saved CSV to: $outputPath")
  }
}
