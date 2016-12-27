package cn.whaley.datawarehouse

import cn.whaley.sdk.dataexchangeio.DataIO
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Tony on 16/12/21.
  */
trait BaseClass {

  /**
    * define some parameters
    */
  var sc: SparkContext = null
  implicit var sqlContext: SQLContext = null
  val config = new SparkConf()
//    .set("spark.executor.memory", "4g")
//    .set("spark.executor.cores", "3")
    .set("spark.scheduler.mode", "FAIR")
    .set("spark.eventLog.enabled", "true")
    .set("spark.eventLog.dir", "hdfs://hans/spark-log/spark-events")
//    .set("spark.cores.max", "72")
    .set("spark.driver.maxResultSize", "2g")
//    .setAppName(this.getClass.getSimpleName)


  def main(args: Array[String]) {
    System.out.println("init start ....")
    init()
    System.out.println("init success ....")
    execute(args)
  }

  /**
    * initialize global parameters
    */
  def init() = {
    sc = new SparkContext(config)
    sqlContext = SQLContext.getOrCreate(sc)
//    DataIO.init("hdfs://hans/test/config.json")
  }

  /**
    * this method do not complete.Sub class that extends BaseClass complete this method
    */
  def execute(args: Array[String])

  /**
    * release resource
    */
  def destroy() = {
    if (sc != null) {
      sqlContext.clearCache()
      sc.stop()
    }
  }
}
