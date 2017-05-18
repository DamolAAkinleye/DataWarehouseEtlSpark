package cn.whaley.datawarehouse

import cn.whaley.datawarehouse.common.DimensionColumn
import cn.whaley.datawarehouse.global.Constants._
import cn.whaley.datawarehouse.global.Globals._
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.{DateFormatUtils, Params, ParamsParseUtil}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.io.File


/**
  * Created by Tony on 16/12/21.
  */
trait BaseClass {
  val config = new SparkConf()
  /**
    * define some parameters
    */
  var sc: SparkContext = null
  implicit var sqlContext: SQLContext = null
  var hiveContext: HiveContext = null

  var readSourceType: Value = _

  var debug = false

  /**
    * 程序入口
    *
    * @param args
    */
  def main(args: Array[String]) {
    System.out.println("init start ....")
    init()
    System.out.println("init success ....")

    beforeExecute()
    println("execute start ....")
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        if (p.startDate != null) {
          var date = p.startDate
          p.paramMap.put("date", date)
          execute(p)
          while (p.endDate != null && date < p.endDate) {
            date = DateFormatUtils.enDateAdd(date, 1)
            p.paramMap.put("date", date)
            execute(p)
          }
        } else {
          execute(p)
        }
      }
      case None => {
        throw new RuntimeException("parameters wrong")
      }
    }
    println("execute end ....")
    destroy()

  }

  /**
    * 全局变量初始化
    */
  def init(): Unit = {
    sc = new SparkContext(config)
    sqlContext = SQLContext.getOrCreate(sc)

    //    hiveContext = new HiveContext(sc)
    //    DataIO.init("hdfs://hans/test/config.json")
  }

  def beforeExecute(): Unit = {

  }

  /**
    * ETL过程执行程序
    */
  def execute(params: Params): Unit = {

    if (params.debug) debug = true

    val df = extract(params)

    val result = transform(params, df)

    load(params, result)

  }

  /**
    * release resource
    */
  def destroy(): Unit = {
    if (sc != null) {
      sqlContext.clearCache()
      sc.stop()
    }
  }

  /**
    * 源数据读取函数, ETL中的Extract
    * 如需自定义，可以在子类中重载实现
    *
    * @return
    */
  def extract(params: Params): DataFrame

  /**
    * 数据转换函数，ETL中的Transform
    *
    * @return
    */
  def transform(params: Params, df: DataFrame): DataFrame

  /**
    * 数据存储函数，ETL中的Load
    */
  def load(params: Params, df: DataFrame)

  /**
    * 维度解析方法
    *
    * @param sourceDf         目标表
    * @param dimensionColumns 解析用的join参数
    * @param uniqueKeyName    目标表的唯一键列
    * @param sourceTimeColumn 源数据时间列获取sql(或者只是个列名)
    * @return 输出包含uniqueKeyName列和所以维度表的代理键列，不包含目标表中的数据，失败返回null
    */
  def parseDimension(sourceDf: DataFrame,
                     dimensionColumns: List[DimensionColumn],
                     uniqueKeyName: String,
                     sourceTimeColumn: String = null): DataFrame = {
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
          //          dimensionDf = dimensionDf.dropDuplicates(jc.columnPairs.values.toArray)
          //实时表源数据过滤
          var sourceFilterDf =
          if (jc.sourceWhereClause != null && !jc.sourceWhereClause.isEmpty)
            sourceDf.where(jc.sourceWhereClause)
          else
            sourceDf
          sourceFilterDf =
            if (sourceTimeColumn == null || sourceTimeColumn.isEmpty) {
              sourceFilterDf.withColumn(COLUMN_NAME_FOR_SOURCE_TIME, expr("null"))
            } else {
              sourceFilterDf.withColumn(COLUMN_NAME_FOR_SOURCE_TIME, expr(sourceTimeColumn))
            }

          //源数据中未关联上的行
          val notJoinDf =
            if (df != null) {
              sourceFilterDf.as("a").join(
                df.as("dim"), sourceFilterDf(uniqueKeyName) === df(uniqueKeyName), "leftouter"
              ).where(c.dimensionColumnName.map("dim." + _._1 + " is null").mkString(" and ")).selectExpr("a.*")
            } else {
              sourceFilterDf
            }

          //源表与维度表join
          val afterJoinDf = notJoinDf.as("a").join(
            dimensionDf.as("b"),
            jc.columnPairs.map(s => notJoinDf(s._1) === dimensionDf(s._2)).reduceLeft(_ && _)
              && (expr(s"a.$COLUMN_NAME_FOR_SOURCE_TIME is null and b.dim_invalid_time is null") ||
              expr(s"a.$COLUMN_NAME_FOR_SOURCE_TIME >= b.dim_valid_time and " +
                s"(a.$COLUMN_NAME_FOR_SOURCE_TIME < b.dim_invalid_time or b.dim_invalid_time is null)")),
            "inner"
          ).selectExpr("a." + uniqueKeyName :: c.dimensionColumnName.map("b." + _._1): _*
          ).dropDuplicates(List(uniqueKeyName))

          if (df != null) {
            df = afterJoinDf.unionAll(df)
          } else {
            df = afterJoinDf
          }
        })
        df = sourceDf.as("a").join(
          df.as("b"), sourceDf(uniqueKeyName) === df(uniqueKeyName), "leftouter"
        ).selectExpr(
          "a." + uniqueKeyName :: c.dimensionColumnName.map(c => "b." + c._1 + " as " + c._2): _*
        )
        //多个维度合成一个DataFrame
        if (dimensionColumnDf == null) {
          dimensionColumnDf = df
        } else {
          dimensionColumnDf = dimensionColumnDf.join(df, uniqueKeyName)
        }
      }
      )
    }
    dimensionColumnDf
  }
}
