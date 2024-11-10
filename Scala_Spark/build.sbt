import scala.collection.Seq

version in ThisBuild := "0.1.0-SNAPSHOT"

scalaVersion in ThisBuild := "2.11.12"

lazy val root = (project in file("."))
  .settings(
    name := "Scala_Spark",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "2.4.4",
      "org.apache.spark" %% "spark-sql" % "2.4.4",
      "org.slf4j" % "slf4j-api" % "1.7.32",
      "org.slf4j" % "slf4j-simple" % "1.7.32",
      "com.fasterxml.jackson.core" % "jackson-core" % "2.6.7",
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7.3",
      "com.fasterxml.jackson.core" % "jackson-annotations" % "2.6.7"
    )
  )
