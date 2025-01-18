ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "InternetShopStreamingFlink"
  )

val flinkVersion = "1.17.0"

val logbackVersion = "1.2.12"

val flinkDependencies = Seq(
  "org.apache.flink" % "flink-streaming-java" % flinkVersion,
  "org.apache.flink" % "flink-clients" % flinkVersion,
  "org.apache.flink" % "flink-test-utils" % flinkVersion,
  "org.apache.flink" % "flink-statebackend-rocksdb" % flinkVersion,
  "org.apache.flink" % "flink-csv" % flinkVersion,
  "org.apache.flink" % "flink-connector-files" % flinkVersion,
  "org.apache.flink" % "flink-connector-kafka" % flinkVersion,
  "org.apache.flink" % "flink-json" % flinkVersion,
  "org.apache.flink" % "flink-connector-elasticsearch7" % "3.0.1-1.17"
)

val logbackDependencies = Seq(
  "ch.qos.logback" % "logback-core" % logbackVersion,
  "ch.qos.logback" % "logback-classic" % logbackVersion
)

libraryDependencies ++= flinkDependencies ++ logbackDependencies