name := "spark-json-relay"

version := "2.0.1"

scalaVersion := "2.10.4"

val json4s = "org.json4s" %% "json4s-jackson" % "3.2.10"
val scalatest = "org.scalatest" %% "scalatest" % "2.2.4" % "test"

libraryDependencies += json4s
libraryDependencies += scalatest

spName := "hammerlab/spark-json-relay"

sparkVersion := "1.5.0"

// check deprecation without manual restart
scalacOptions in ThisBuild ++= Seq("-unchecked", "-deprecation", "-feature")

// display full-length stacktraces from ScalaTest
testOptions in Test += Tests.Argument("-oF")

coverageHighlighting := {
  if (scalaBinaryVersion.value == "2.10") false
  else true
}
coverageMinimum := 80
coverageFailOnMinimum := true
