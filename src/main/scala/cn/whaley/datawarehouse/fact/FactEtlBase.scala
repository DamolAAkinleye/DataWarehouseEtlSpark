package cn.whaley.datawarehouse.fact

import cn.whaley.datawarehouse.BaseClass
import cn.whaley.datawarehouse.common.{DimensionColumn, UserDefinedColumn}
import cn.whaley.datawarehouse.fact.constant.LogPath
import cn.whaley.datawarehouse.global.Globals._
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.{DataFrameUtil, HdfsUtil, Params}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

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

  /**
    * 事实发生的时间，格式yyyy-MM-dd HH:mm:ss
    */
  var factTime: String = "concat(dim_date, '', dim_time)"

  //  override def execute(params: Params): Unit = {
  //    val result = doExecute(params)
  //
  //    HdfsUtil.deleteHDFSFileOrPath(MEDUSA_FACT_HDFS_BASE_PATH + File.separator + topicName + File.separator + params.startDate)
  //    result.write.parquet(MEDUSA_FACT_HDFS_BASE_PATH + File.separator + topicName + File.separator + params.startDate)
  //  }

  /**
    * 源数据读取函数, ETL中的Extract
    * 如需自定义，可以在子类中重载实现
    *
    * @return
    */
  override def extract(params: Params): DataFrame = {
    params.paramMap.get("date") match {
      case Some(d) => {
        println("数据时间：" + d)
        readSource(d.toString)
      }
      case None =>
        throw new RuntimeException("未设置时间参数！")
    }
  }

  def readSource(sourceDate: String): DataFrame = {
    if (sourceDate == null) {
      null
    } else if (readSourceType == null || readSourceType == parquet) {
      readFromParquet(parquetPath, sourceDate)
    } else {
      null
    }
  }

  def readFromParquet(path: String, sourceDate: String): DataFrame = {
    val filePath = path.replace(LogPath.DATE_ESCAPE, sourceDate)
    val sourceDf = sqlContext.read.parquet(filePath)
    sourceDf
  }

  /**
    * 数据转换函数，ETL中的Transform
    *
    * @return
    */
  override def transform(params: Params, sourceDf: DataFrame): DataFrame = {

    val filteredSourceDf = filterRows(sourceDf)
//    filteredSourceDf.dropDuplicates()

    val completeSourceDf = addNewColumns(filteredSourceDf)
    completeSourceDf.persist()

    if(debug) {
      println("完整事实表行数：" + completeSourceDf.count())
      completeSourceDf.show()
    }


    val dimensionJoinDf = parseDimension(completeSourceDf, dimensionColumns, INDEX_NAME, factTime)
    if(debug) {
      dimensionJoinDf.persist()
      println("维度关联表行数：" + dimensionJoinDf.count())
      dimensionJoinDf.show()
    }

    var df = completeSourceDf.join(dimensionJoinDf, List(INDEX_NAME), "leftouter").as("source")
    if (dimensionColumns != null) {
      //关联所有的维度  TODO 判断只关联用到的维度
      dimensionColumns.foreach(c => {
        val dimensionDf = sqlContext.read.parquet(DIMENSION_HDFS_BASE_PATH + File.separator + c.dimensionName)
        df = df.join(dimensionDf.as(c.dimensionName),
          expr("source." + c.dimensionColumnName + " = " + c.dimensionName + "." + c.dimensionSkName),
          "leftouter")
      })
    }
    val result = df.selectExpr(
      columnsFromSource.map(
        c => if (c._2.contains(" ") || c._2.contains("."))
          c._2 + " as " + c._1
        else
          "source." + c._2 + " as " + c._1)
        ++ dimensionJoinDf.schema.fields.filter(_.name != INDEX_NAME).map("source." + _.name)
        : _*
    )

    if(debug) {
      println("最终结果行数：" + result.count())
    }

    result
  }

  def filterRows(sourceDf: DataFrame): DataFrame = {
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

  /* private def parseDimension(sourceDf: DataFrame): DataFrame = {
     var dimensionColumnDf: DataFrame = null
     if (dimensionColumns != null) {
       //对每个维度表
       dimensionColumns.foreach(c => {
         val dimensionDfBase = sqlContext.read.parquet(DIMENSION_HDFS_BASE_PATH + File.separator + c.dimensionName)
         var df: DataFrame = null
         //对每组关联条件
         c.joinColumnList.foreach(jc => {
           var dimensionDf = dimensionDfBase
           //维度表过滤
           if (jc.whereClause != null && !jc.whereClause.isEmpty) {
             dimensionDf = dimensionDf.where(jc.whereClause)
           }
           //维度表排序
           if (jc.orderBy != null && jc.orderBy.nonEmpty) {
             dimensionDf.orderBy(jc.orderBy.map(s => if (s._2) col(s._1).desc else col(s._1).asc): _*)
           }
           //维度表去重
           dimensionDf = dimensionDf.dropDuplicates(jc.columnPairs.values.toArray)
           //实时表源数据过滤
           val sourceFilterDf =
             if (jc.sourceWhereClause != null && !jc.sourceWhereClause.isEmpty)
               sourceDf.where(jc.sourceWhereClause)
             else
               sourceDf
           //源表与维度表join
           if (df == null) {
             df = sourceFilterDf.as("a").join(dimensionDf.as("b"),
               jc.columnPairs.map(s => sourceFilterDf(s._1) === dimensionDf(s._2)).reduceLeft(_ && _),
               "inner").selectExpr("a." + INDEX_NAME, "b." + c.dimensionSkName)
           } else {
             df = sourceFilterDf.as("a").join(df.as("dim"), sourceFilterDf(INDEX_NAME) === df(INDEX_NAME), "leftouter").join(
               dimensionDf.as("b"),
               jc.columnPairs.map(s => sourceFilterDf(s._1) === dimensionDf(s._2)).reduceLeft(_ && _)
                 && isnull(df(c.dimensionSkName)),
               "inner").selectExpr("a." + INDEX_NAME, "b." + c.dimensionSkName).unionAll(df)
           }
         })
         df = sourceDf.as("a").join(df.as("b"), sourceDf(INDEX_NAME) === df(INDEX_NAME), "leftouter").selectExpr(
           "a." + INDEX_NAME, "b." + c.dimensionSkName + " as " + c.dimensionColumnName)
         //        println(df.count())
         //多个维度合成一个dataframe
         if (dimensionColumnDf == null) {
           dimensionColumnDf = df
         } else {
           dimensionColumnDf = dimensionColumnDf.join(df, INDEX_NAME)
         }
       }
       )
     }

     if (dimensionColumnDf != null) {
       dimensionColumnDf
     } else {
       sqlContext.createDataFrame(List[Row](), StructType(Array[StructField]()))
     }
   }*/

  override def load(params: Params, df: DataFrame): Unit = {
    HdfsUtil.deleteHDFSFileOrPath(FACT_HDFS_BASE_PATH + File.separator + topicName + File.separator + params.paramMap("date") + File.separator + "00")
    df.write.parquet(FACT_HDFS_BASE_PATH + File.separator + topicName + File.separator + params.paramMap("date") + File.separator + "00")
  }


}
