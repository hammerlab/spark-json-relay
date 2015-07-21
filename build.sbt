name := "spark-json-relay"

version := "1.0.0"

scalaVersion := "2.11.4"

val json4s = "org.json4s" %% "json4s-jackson" % "3.2.10"

val spark = "org.apache.spark" %% "spark-core" % "1.4.1" % "provided"

libraryDependencies ++= Seq(json4s, spark)
