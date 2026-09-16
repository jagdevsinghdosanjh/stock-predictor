package ollama

import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.net.URI
import ujson._

object OllamaClient {

  def ask(prompt: String): String = {

    val client = HttpClient.newHttpClient()

    val jsonBody =
      s"""{"model":"llama3","prompt":"$prompt"}"""

    val request = HttpRequest.newBuilder()
      .uri(URI.create("http://localhost:11434/api/generate"))
      .header("Content-Type", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
      .build()

    val response = client.send(request, HttpResponse.BodyHandlers.ofString())

    val parsed = ujson.read(response.body())
    parsed("response").str
  }
}
