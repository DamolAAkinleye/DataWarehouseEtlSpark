package cn.whaley.datawarehouse

import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.{Params, ParamsParseUtil}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}


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

  /**
    * 程序入口
    *
    * @param args
    */
  def main(args: Array[String]) {
    System.out.println("init start ....")
    init()
    System.out.println("init success ....")

    println("execute start ....")
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        execute(p)
      }
      case None => {
        throw new RuntimeException("parameters wrong")
      }
    }
    println("execute end ....")

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


  /**
    * ETL过程执行程序
    */
  def execute(params: Params): Unit = {

    val df = extract(params)

    val result = transform(params, df)

    load(params, result)

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
    * release resource
    */
  def destroy(): Unit = {
    if (sc != null) {
      sqlContext.clearCache()
      sc.stop()
    }
  }
}
