package qa.tools.monitor

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

case class ParquetDataFrame(spark: SparkSession, parquetPath: String) {
  def getDF: DataFrame = spark.read.parquet(parquetPath)

  def getColumns: Array[String] = getDF.columns

  def getSchema: StructType = getDF.schema

  def dfSchemaMap: Map[String, Any] = parquetSchemaToMap(getSchema)

  def getSortedDF: DataFrame = {
    getDF.createOrReplaceTempView("view")
    spark.sql("SELECT * FROM view ORDER BY flush_time DESC")
  }

  private def parquetSchemaToMap(dfSchema: StructType): Map[String, Any] = {
    dfSchema.map(_.name).zip(dfSchema.map(_.dataType.simpleString)).toMap
  }
}



class SparkDriver(spark: SparkSession) {

  def parquetDF(path: String) = ParquetDataFrame(spark, path)
  def dsCollect(df: DataFrame)={
    df.collect()
  }


  def dsToListMap(df: DataFrame, columns:Array[String]):Map[String, String] = {
    val localDF = dsCollect(df)
    val dfArray = for ( x <- columns.indices) yield {if (localDF.map(_(x)).exists(_!=null)){localDF.map(_(x)).filter(_ != null)(0)} else null}
    columns.zip(dfArray).toMap.map(x=> x._1 -> x._2.toString)
  }
}

object Run2 {
  def main(args: Array[String]): Unit = {
    print(11)
  }
}

