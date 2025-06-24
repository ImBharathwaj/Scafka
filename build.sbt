ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.3.6"

lazy val root = (project in file("."))
  .settings(
    name := "Scafka",
    idePackagePrefix := Some("org.tamaai.com")
  )

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "3.4.0",
  "ch.qos.logback" % "logback-classic" % "1.2.11",
  "org.slf4j" % "slf4j-api" % "2.0.17",
  "org.slf4j" % "slf4j-simple" % "2.0.17"
)