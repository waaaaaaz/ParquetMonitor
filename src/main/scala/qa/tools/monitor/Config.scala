package qa.tools.monitor

import com.typesafe.config.ConfigFactory


object Configuration {

  private val config = ConfigFactory.load()

  val influx_host: String = config.getString("parquet-monitor.influxdb.host")
  val influx_port: Int = config.getInt("parquet-monitor.influxdb.port")
  val influx_dbname_batch: String = config.getString("parquet-monitor.influxdb.db.batch")
  val influx_dbname_steam: String = config.getString("parquet-monitor.influxdb.db.stream")
  val influx_measurement_schema: String = config.getString("parquet-monitor.influxdb.measurement.schema")
  val influx_measurement_exapmle_value: String = config.getString("parquet-monitor.influxdb.measurement.exapmle_value")
  val influx_measurement_execution_result: String = config.getString("parquet-monitor.influxdb.measurement.execution_result")
  val influx_measurement_alter_name: String = config.getString("parquet-monitor.influxdb.measurement.alter_name")
  val influx_measurement_alter_type: String = config.getString("parquet-monitor.influxdb.measurement.alter_type")
  val influx_measurement_alter_value: String = config.getString("parquet-monitor.influxdb.measurement.alter_value")

  val baseHDFSAddress: String = config.getString("parquet-monitor.hdfs.baseAddress")

  private val scenario_dev2_events = new Scenario("dev2_events",
    config.getString("parquet-monitor.scenario.dev2_events.env"),
    config.getString("parquet-monitor.scenario.dev2_events.monitor_type"),
    config.getString("parquet-monitor.scenario.dev2_events.path"))


  private val scenario_dev2_entities = new Scenario("dev2_entities",
    config.getString("parquet-monitor.scenario.dev2_entities.env"),
    config.getString("parquet-monitor.scenario.dev2_entities.monitor_type"),
    config.getString("parquet-monitor.scenario.dev2_entities.path"))

  private val scenario_test2_events = new Scenario("test2_events",
    config.getString("parquet-monitor.scenario.test2_events.env"),
    config.getString("parquet-monitor.scenario.test2_events.monitor_type"),
    config.getString("parquet-monitor.scenario.test2_events.path"))

  private val scenario_test2_entities = new Scenario("test2_entities",
    config.getString("parquet-monitor.scenario.test2_entities.env"),
    config.getString("parquet-monitor.scenario.test2_entities.monitor_type"),
    config.getString("parquet-monitor.scenario.test2_entities.path"))

  val scenarios = Map("dev2_events" -> scenario_dev2_events,
    "dev2_entities" -> scenario_dev2_entities,
    "test2_events" -> scenario_test2_events,
    "test2_entities" -> scenario_test2_entities)

}

case class Scenario(name: String, env: String, monitorType: String, path: String) {
}
