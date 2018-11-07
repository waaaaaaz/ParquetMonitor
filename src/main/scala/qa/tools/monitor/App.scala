package qa.tools.monitor

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger


object App {

  def main(args: Array[String]): Unit ={

    val log = Logger.getLogger(classOf[App])

    val spark = SparkSession
      .builder
      .master("local")
      .appName("ParquetReader")
      .getOrCreate()

    spark.sparkContext.setLogLevel("INFO")

    val hdfs = new HDFSDriver()
    val influxDB = InfluxDBDriver.getInstance
    val scenarios = Configuration.scenarios
    val sparkDriver = new SparkDriver(spark)

    log.info("start to execute batch process ")
    scenarios.foreach(s=> App.taskRun(hdfs, influxDB, sparkDriver, s._2))
    log.info("end batch process ")

  }

  def taskRun(hdfs: HDFSDriver, influxdb: InfluxDBDriver, sparkDriver: SparkDriver, scenario: Scenario): Unit ={
    val monitorTask = new MonitorTask(hdfs, influxdb, sparkDriver, scenario)
    monitorTask.taskExecution()
  }

}