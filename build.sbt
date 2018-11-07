name := "ParquetMonitor"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= {
  val sparkVersion = "2.2.0"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.apache.spark" %% "spark-streaming" % sparkVersion,
    "org.apache.hadoop" % "hadoop-client" % "2.6.0-cdh5.12.1",
    "com.paulgoldbaum" %% "scala-influxdb-client" % "0.6.1",
    "com.typesafe" % "config" % "1.0.2",
    "org.apache.logging.log4j" % "log4j-api" % "2.11.1",
    "org.apache.logging.log4j" % "log4j-core" % "2.11.1"
  )
}