package cn.whaley.datawarehouse.fact

import cn.whaley.datawarehouse.BaseClass
import cn.whaley.datawarehouse.fact.common.{DimensionColumn, UserDefinedColumn}
import cn.whaley.datawarehouse.fact.constant.LogPath
import cn.whaley.datawarehouse.global.Globals._
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.{DataFrameUtil, HdfsUtil, Params}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.JavaConversions._
import scala.reflect.io.File


/**
  * Created by Tony on 17/4/5.
  */
abstract class FactEtlBase extends BaseClass {

  private val INDEX_NAME = "source_index"

  var columnsFromSource: List[(String, String)] = _

  var topicName: String = _

  var parquetPath: String = _

  var addColumns: List[UserDefinedColumn] = _

  var dimensionColumns: List[DimensionColumn] = _

  //  override def execute(params: Params): Unit = {
  //    val result = doExecute(params)
  //
  //    HdfsUtil.deleteHDFSFileOrPath(MEDUSA_FACT_HDFS_BASE_PATH + File.separator + topicName + File.separator + params.startDate)
  //    result.write.parquet(MEDUSA_FACT_HDFS_BASE_PATH + File.separator + topicName + File.separator + params.startDate)
  //  }

  override def extract(params: Params): DataFrame = {
    readSource(params.startDate)
  }

  def readSource(startDate: String): DataFrame = {
    if (readSourceType == null || readSourceType == parquet) {
      readFromParquet(parquetPath, startDate)
    } else {
      null
    }
  }

  def readFromParquet(path: String, startDate: String): DataFrame = {
    val filePath = path.replace(LogPath.DATE_ESCAPE, startDate)
    val sourceDf = sqlContext.read.parquet(filePath)
    sourceDf
    //    sourceDf.selectExpr(columnsFromSourceMap.map(s => s._2 + " as " + s._1).toArray: _*)
  }

  override def transform(params: Params, sourceDf: DataFrame): DataFrame = {

    val filteredSourceDf = filterRows(sourceDf)

    val completeSourceDf = addNewColumns(filteredSourceDf)

    val dimensionDf = parseDimension(completeSourceDf)

    completeSourceDf.as("a").join(dimensionDf.as("b"), List(INDEX_NAME), "leftouter").selectExpr(
      columnsFromSource.map(c => c._2 + " as " + c._1)
        ++ dimensionDf.schema.fields.filter(_.name != INDEX_NAME).map("b." + _.name)
        : _*
    )
  }

  private def filterRows(sourceDf: DataFrame): DataFrame = {
    sourceDf
  }

  private def addNewColumns(sourceDf: DataFrame): DataFrame = {
    var result = DataFrameUtil.dfZipWithIndex(sourceDf, INDEX_NAME)
    if (addColumns != null) {
      addColumns.foreach(column =>
        result = result.withColumn(column.name, column.udf(column.inputColumns.map(col): _*))
      )
    }
    result
  }

  private def parseDimension(sourceDf: DataFrame): DataFrame = {
    var dimensionColumnDf: DataFrame = null
    if (dimensionColumns != null) {
      dimensionColumns.foreach(c => {
        val dimensionDf = sqlContext.read.parquet(DIMENSION_HDFS_BASE_PATH + File.separator + c.dimensionName)
        val df = sourceDf.as("a").join(dimensionDf.as("b"),
          c.joinColumnList.head.map(s => sourceDf(s._1) === dimensionDf(s._2)).reduceLeft(_ && _),
          "leftouter").selectExpr("a." + INDEX_NAME, "b." + c.dimensionSkName)
        if (dimensionColumnDf == null) {
          dimensionColumnDf = df
        } else {
          dimensionColumnDf = dimensionColumnDf.join(df, INDEX_NAME)
        }
      })
    }
    if (dimensionColumnDf != null) {
      dimensionColumnDf
    } else {
      sqlContext.createDataFrame(List[Row](), StructType(Array[StructField]()))
    }
  }

  override def load(params: Params, df: DataFrame): Unit = {
    HdfsUtil.deleteHDFSFileOrPath(MEDUSA_FACT_HDFS_BASE_PATH + File.separator + topicName + File.separator + params.startDate)
    df.write.parquet(MEDUSA_FACT_HDFS_BASE_PATH + File.separator + topicName + File.separator + params.startDate)
  }


}
