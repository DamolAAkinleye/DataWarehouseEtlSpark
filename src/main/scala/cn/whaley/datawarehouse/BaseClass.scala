package cn.whaley.datawarehouse

import cn.whaley.datawarehouse.util.{Params, ParamsParseUtil}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
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
    * initialize global parameters
    */
  def init(): Unit = {
    sc = new SparkContext(config)
    sqlContext = SQLContext.getOrCreate(sc)

    //    hiveContext = new HiveContext(sc)
    //    DataIO.init("hdfs://hans/test/config.json")
  }


  /**
    * this method do not complete.Sub class that extends BaseClass complete this method
    */
  def execute(params: Params)

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
