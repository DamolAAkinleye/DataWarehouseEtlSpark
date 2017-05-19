package cn.whaley.datawarehouse.util

import cn.whaley.datawarehouse.fact.constant.LogPath
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by Tony on 17/4/15.
  */
object DataExtractUtils {

  def readFromJdbc(sqlContext: SQLContext, sourceDb: Map[String, String]): DataFrame = {
    sqlContext.read.format("jdbc").options(sourceDb).load()
  }

  def readFromParquet(sqlContext: SQLContext, sourceParquetPath: String, startDate: String): DataFrame = {
    val filePath = sourceParquetPath.replace(LogPath.DATE_ESCAPE, startDate)
    val sourceDf = sqlContext.read.parquet(filePath)
    sourceDf
  }
  def readFromParquet(sqlContext: SQLContext, sourceParquetPath: String): DataFrame = {
    val sourceDf = sqlContext.read.parquet(sourceParquetPath)
    sourceDf
  }

}
