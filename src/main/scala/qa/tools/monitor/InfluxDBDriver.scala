package qa.tools.monitor

import com.paulgoldbaum.influxdbclient.Parameter.Precision
import com.paulgoldbaum.influxdbclient.Parameter.Precision.Precision
import com.paulgoldbaum.influxdbclient._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}


case class InfluxPoint(measurement: String,
                       env: String,
                       monitorSource: String,
                       fields: Seq[(String, Any)],
                       timeStamp: Long = System.currentTimeMillis) {

  def getMeasurement: String = measurement

  def getEnvTags: String = env

  def getMonitorSourceTag: String = monitorSource

  def getFields: Seq[(String, Any)] = fields

  def getTimeStamp: Long = timeStamp
}


object InfluxDBDriver {

  val getInstance: InfluxDBDriver = new InfluxDBDriver(Configuration.influx_host, Configuration.influx_port, Configuration.influx_dbname_batch)

  def buildPoint(iPoint: InfluxPoint): Point = {

    val point = Point(iPoint.getMeasurement, iPoint.getTimeStamp).addTag("env", iPoint.getEnvTags).addTag("monitorSource", iPoint.getMonitorSourceTag)
    val fields = iPoint.getFields
    fields.foldLeft(point)((p: Point, fp: (String, Any)) =>
      fp._2 match {
        case s: String => p.addField(fp._1, s)
        case d: Double => p.addField(fp._1, d)
        case l: Long => p.addField(fp._1, l)
        case b: Boolean => p.addField(fp._1, b)
        case _ => throw new RuntimeException("Wrong type for field")
      })
  }

}

class InfluxDBDriver(host: String, port: Int, dbname: String) {

  private val dbConn = InfluxDB.connect(host, port)
  private var db = dbConn.selectDatabase(dbname)

  val db_future: Future[Database] = db.exists().map(x =>
    if (x)
      Future.successful(db)
    else {
      db.create().map(_ => db)
    })
    .flatMap(identity)

//  def setDB(dbname: String):InfluxDBDriver = {
//    db = dbConn.selectDatabase(dbname)
//    this
//  }

  def insertPoint(iPoint: InfluxPoint): Boolean = {
    val point = InfluxDBDriver.buildPoint(iPoint)
    try {
      val future = db.write(point, precision = Precision.MILLISECONDS)
      val result = Await.result(future, 60.seconds)
      result
    }
    catch {
      case e: Exception => {
        println(e)
        throw e
        false
      }
    }
  }

  def insertPoints(iPoints: Seq[InfluxPoint]): Boolean = {
    if (iPoints.length == 1)
      insertPoint(iPoints.head)
    else {
      val points: Seq[Point] = for(p <- iPoints) yield InfluxDBDriver.buildPoint(p)
      try {
        val future = db.bulkWrite(points, precision = Precision.MILLISECONDS)
        val result = Await.result(future, 60.seconds)
        result
      }
      catch {
        case e: Exception => {
          println(e)
          throw e
          false
        }
      }
    }
  }



  def query(q: String, precision: Precision = Precision.MILLISECONDS): List[Map[String, Any]] = {
    val future = db.query(q, precision)
    val result = Await.result(future, 60.seconds)
//    println(result.series)
    asListMap(result)
  }

  def latestSchema() = {
    val measurement = Configuration.influx_measurement_schema
//    val measurement = "schema001"
    val queryString = f"SELECT * FROM $measurement%s ORDER BY time DESC LIMIT 1"
    val queryResult = query(queryString)
    if (queryResult.nonEmpty)
    {queryResult.head.filterKeys{Set("env", "monitorSource", "time").contains(_) == false}}
    else
      None
  }

  def latestExeTime() = {
    val measurement = Configuration.influx_measurement_execution_result
    val queryString = f"SELECT * FROM $measurement%s ORDER BY time DESC LIMIT 1"
    val queryResult = query(queryString)
    if (queryResult.nonEmpty)
    {queryResult.head("time")}
    else
      None
  }

  def latestExampleValue() = {
    val measurement = Configuration.influx_measurement_exapmle_value
    val queryString = f"SELECT * FROM $measurement%s ORDER BY time DESC LIMIT 1"
    val queryResult = query(queryString)
    if (queryResult.nonEmpty)
    {queryResult.head.filterKeys{Set("env", "monitorSource", "time").contains(_) == false}}
    else
      None
  }


  def insertSchema(scenario: Scenario,
                   fields: Seq[(String, Any)],
                   timeStamp: Long = System.currentTimeMillis): Boolean = {
    val iPoint = InfluxPoint(Configuration.influx_measurement_schema, scenario.env, scenario.monitorType, fields, timeStamp)
    insertPoint(iPoint)
  }

  def insertExample(scenario: Scenario,
                         fields: Seq[(String, Any)],
                         timeStamp: Long = System.currentTimeMillis): Boolean = {
    val iPoint = InfluxPoint(Configuration.influx_measurement_exapmle_value, scenario.env, scenario.monitorType, fields, timeStamp)
    insertPoint(iPoint)
  }

  def insertNameAlter(scenario: Scenario,
                    fields: Seq[(String, Any)],
                    timeStamp: Long = System.currentTimeMillis): Boolean = {
    val iPoint = InfluxPoint(Configuration.influx_measurement_alter_name, scenario.env, scenario.monitorType, fields, timeStamp)
    insertPoint(iPoint)
  }

  def insertTypeAlter(scenario: Scenario,
                      fields: Seq[(String, Any)],
                      timeStamp: Long = System.currentTimeMillis): Boolean = {
    val iPoint = InfluxPoint(Configuration.influx_measurement_alter_type, scenario.env, scenario.monitorType, fields, timeStamp)
    insertPoint(iPoint)
  }

  def insertValueAlter(scenario: Scenario,
                      fields: Seq[(String, Any)],
                      timeStamp: Long = System.currentTimeMillis): Boolean = {
    val iPoint = InfluxPoint(Configuration.influx_measurement_alter_value, scenario.env, scenario.monitorType, fields, timeStamp)
    insertPoint(iPoint)
  }

  def asListMap(queryResult: QueryResult): List[Map[String, Any]] = {
    if (queryResult.series.nonEmpty) {
      val columns = queryResult.series.head.columns
      val records = queryResult.series.head.records
      val toListMap = for (x <- records) yield columns.zip(x.allValues).toMap
      toListMap
    }
    else
      List()
  }

  def close(): Unit = {
    dbConn.close()
  }
}



