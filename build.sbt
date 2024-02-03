ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "untitled_Hotel"
  )

ThisBuild / libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.2",
  "org.apache.spark" %% "spark-sql" % "3.3.2",
  "org.apache.spark" %% "spark-mllib" % "3.3.2",
  "org.apache.spark" %% "spark-hive" % "3.3.2" ,
  "org.apache.logging.log4j" %% "log4j-api-scala" % "12.0",
  "org.apache.logging.log4j" % "log4j-core" % "2.20.0",
  "com.typesafe" % "config" % "1.4.2",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.13.0",
  "com.sparkjava" % "spark-core" % "2.9.4")