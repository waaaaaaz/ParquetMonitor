package qa.tools.monitor

import org.apache.spark.sql.DataFrame

class MonitorTask(hdfs: HDFSDriver, influxdb: InfluxDBDriver, sparkDriver: SparkDriver, scenario: Scenario) {
  var hasLatestExeTime = true
  var hasLatestSchema = true
  var hasLatestValueExample = true
  val currentExeTime: Long = System.currentTimeMillis
  val latestExeTime: Any = influxdb.latestExeTime()
  val latestSchema: Any = influxdb.latestSchema()
  val latestValueExample: Any = influxdb.latestExampleValue()
  taskSetUp

  private def taskSetUp: Unit = {
    if (latestExeTime == None) hasLatestExeTime = false
    if (latestSchema == None) hasLatestSchema = false
    if (latestValueExample == None) hasLatestValueExample = false
  }

  private def parquetDF(path: String): ParquetDataFrame = {
    sparkDriver.parquetDF(path)
  }

  def taskExecution() = {
    val files: Array[String] = targetFiles()
    files.foreach(p => taskFlow(p))
  }

  private def taskFlow(path: String) = {

    val currentDF = parquetDF(path)

    if (!hasLatestSchema) {
      influxdb.insertSchema(scenario, currentDF.dfSchemaMap.toSeq)
    }
    else {
      schemaTask(currentDF.dfSchemaMap, latestSchema.asInstanceOf[Map[String, Any]], scenario)
    }
    if (!hasLatestValueExample) {
      influxdb.insertExample(scenario, currentDF.dfSchemaMap.toSeq)
    }
    else {
      valueExampleTask(valueExample(currentDF.getSortedDF, currentDF.getColumns), latestValueExample.asInstanceOf[Map[String, Any]])
    }
    valueLegalTask(valueExample(currentDF.getSortedDF, currentDF.getColumns), scenario)

  }

  private def valueExample(pSortedFD: DataFrame, pColumns: Array[String]): Map[String, String] = {
    sparkDriver.dsToListMap(pSortedFD, pColumns)
  }

  private def targetFiles(): Array[String] = {
    if (hasLatestExeTime) {
      hdfs.targetParquetFilesSortedList(scenario.path, latestExeTime.asInstanceOf[Number].longValue)
    }
    else {
      hdfs.parquetFilesSortedList(scenario.path)
    }
  }


  private def schemaTask(currentSchema: Map[String, Any], lastSchema: Map[String, Any]) = {
    val addedColumnsName = currentSchema.keySet.diff(lastSchema.keySet)
    val removedColumnsName = lastSchema.keySet.diff(currentSchema.keySet)
    val intersectedCurrentSchema = currentSchema.filterKeys(addedColumnsName.contains(_) == false)
    val intersectedLastSchema = lastSchema.filterKeys(removedColumnsName.contains(_) == false)
    val updatedType = intersectedCurrentSchema.toSet.diff(intersectedLastSchema.toSet)
    if (addedColumnsName.nonEmpty) {
      val addedNamePoint = InfluxPoint(Configuration.influx_measurement_alter_name,
        scenario.env,
        scenario.monitorType,
        addedColumnsName.map(x => ("added_name", x)).toList)
      influxdb.insertPoint(addedNamePoint)
    }
    if (removedColumnsName.nonEmpty) {
      val removedNamePoint = InfluxPoint(Configuration.influx_measurement_alter_name,
        scenario.env,
        scenario.monitorType,
        addedColumnsName.map(x => ("removed_name", x)).toList)
      influxdb.insertPoint(removedNamePoint)
    }
    if (updatedType.nonEmpty) {
      val updatedTypePoint = InfluxPoint(Configuration.influx_measurement_alter_type,
        scenario.env,
        scenario.monitorType,
        addedColumnsName.map(x => ("updated_type", "Field " + x + " : from " + intersectedLastSchema(x) + " to " + intersectedCurrentSchema(x))).toList)
      influxdb.insertPoint(updatedTypePoint)
    }
    if (addedColumnsName.nonEmpty && removedColumnsName.nonEmpty && updatedType.nonEmpty) {
      influxdb.insertSchema(scenario, currentSchema.toList)
    }
  }

  private def valueExampleTask(currentValueExample: Map[String, Any], latestValueExample: Map[String, Any]): Map[String, Any] = {
    val updated = latestValueExample.map(x => if (currentValueExample.contains(x._1) && currentValueExample(x._1) != "null") x._1 -> currentValueExample(x._1) else x._1 -> x._2)
    val x = currentValueExample.filterKeys(currentValueExample.keySet.diff(updated.keySet))
    updated ++ currentValueExample.filterKeys(currentValueExample.keySet.diff(updated.keySet))
  }

  private def valueLegalTask(values: Map[String, String]) = {
    val alter = values.filter(x => !(digitsRule(x._1, x._2) || lettersRule(x._1, x._2)))
    if (alter.nonEmpty) {
      influxdb.insertValueAlter(scenario, alter.map(x => ("illegal_value", x._1 + " : " + x._2)).toList)
    }
  }

  private def digitsRule(x: String, y: String): Boolean = {
    val scope = Set("creation_time", "update_time", "blood_group")
    if (scope.contains(x)) {
      y.forall(_.isDigit)
    }
    else true
  }

  private def lettersRule(x: String, y: String): Boolean = {
    val scope = Set("language", "language2", "dialect", "dialect2", "from_gender", "to_gender", "from_language", "to_language")
    if (scope.contains(x)) {
      y.forall(_.isLetter)
    }
    else true
  }

}