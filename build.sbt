name := "stock-predictor"
version := "1.0"
scalaVersion := "2.13.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "4.2.0",
  "org.apache.spark" %% "spark-sql" % "4.2.0",
  "org.apache.spark" %% "spark-mllib" % "4.2.0",
  "com.lihaoyi" %% "ujson" % "3.2.0",
  "com.lihaoyi" %% "upickle" % "3.2.0",
  "com.softwaremill.sttp.client3" %% "core" % "3.9.0"
)


fork := true

javaOptions ++= Seq(
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
)
