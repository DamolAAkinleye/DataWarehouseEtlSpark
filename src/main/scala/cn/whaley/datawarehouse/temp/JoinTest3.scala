package cn.whaley.datawarehouse.temp

import cn.whaley.datawarehouse.BaseClass
import cn.whaley.datawarehouse.global.Globals
import cn.whaley.datawarehouse.util.{HdfsUtil, Params}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Created by Tony on 17/5/11.
  */
object JoinTest3 extends BaseClass {


  /**
    * 源数据读取函数, ETL中的Extract
    * 如需自定义，可以在子类中重载实现
    *
    * @return
    */
  override def extract(params: Params): DataFrame = {
    val fact = sqlContext.read.parquet(Globals.FACT_HDFS_BASE_PATH + "/fact_medusa_user_login/20170509/00")
//    val userDf = sqlContext.read.parquet(Globals.DIMENSION_HDFS_BASE_PATH + "/dim_medusa_terminal_user")
//    val userLoginDf = sqlContext.read.parquet(Globals.DIMENSION_HDFS_BASE_PATH + "/dim_medusa_terminal_user_login")
//    val productDf = sqlContext.read.parquet(Globals.DIMENSION_HDFS_BASE_PATH + "/dim_medusa_product_model")
//    val promotionDf = sqlContext.read.parquet(Globals.DIMENSION_HDFS_BASE_PATH + "/dim_medusa_promotion")
//    val appDf = sqlContext.read.parquet(Globals.DIMENSION_HDFS_BASE_PATH + "/dim_app_version")
//    val locDf = sqlContext.read.parquet(Globals.DIMENSION_HDFS_BASE_PATH + "/dim_web_location")

    val filterFact = fact.where("ip > '100'")
    var a = filterFact
    (0 to 10).foreach(s => {
      val userDf = sqlContext.read.parquet(Globals.DIMENSION_HDFS_BASE_PATH + "/dim_medusa_terminal_user")
      a = a.as("a").join(userDf, a("user_sk") === userDf("user_sk"), "leftouter").selectExpr("a.*")
    })

    a
//    a.selectExpr("a.product_serial",
//      "a.sys_ver",
//      "a.wifi_mac",
//      "a.app_name",
//      "a.ip",
//      "a.mac",
//      "a.product_model",
//      "a.product_version",
//      "a.promotion_channel",
//      "a.sn",
//      "a.log_timestamp",
//      "a.user_id",
//      "a.user_type",
//      "a.version",
//      "a.dim_date",
//      "a.dim_time")

  }

  /**
    * 数据转换函数，ETL中的Transform
    *
    * @return
    */
  override def transform(params: Params, df: DataFrame): DataFrame = {
    df
  }

  /**
    * 数据存储函数，ETL中的Load
    */
  override def load(params: Params, df: DataFrame): Unit = {
    HdfsUtil.deleteHDFSFileOrPath("/tmp/jointest3")
    df.write.parquet("/tmp/jointest3")
  }
}
