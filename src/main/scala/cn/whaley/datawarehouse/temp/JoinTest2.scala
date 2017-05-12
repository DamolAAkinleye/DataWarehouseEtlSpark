package cn.whaley.datawarehouse.temp

import cn.whaley.datawarehouse.BaseClass
import cn.whaley.datawarehouse.global.Globals
import cn.whaley.datawarehouse.util.{HdfsUtil, Params}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Created by Tony on 17/5/11.
  */
object JoinTest2 extends BaseClass {


  /**
    * 源数据读取函数, ETL中的Extract
    * 如需自定义，可以在子类中重载实现
    *
    * @return
    */
  override def extract(params: Params): DataFrame = {
    val fact = sqlContext.read.parquet(Globals.FACT_HDFS_BASE_PATH + "/fact_medusa_user_login/20170509/00")
    val userDf = sqlContext.read.parquet(Globals.DIMENSION_HDFS_BASE_PATH + "/dim_medusa_terminal_user")
    val userLoginDf = sqlContext.read.parquet(Globals.DIMENSION_HDFS_BASE_PATH + "/dim_medusa_terminal_user_login")
    val productDf = sqlContext.read.parquet(Globals.DIMENSION_HDFS_BASE_PATH + "/dim_medusa_product_model")
    val promotionDf = sqlContext.read.parquet(Globals.DIMENSION_HDFS_BASE_PATH + "/dim_medusa_promotion")
    val appDf = sqlContext.read.parquet(Globals.DIMENSION_HDFS_BASE_PATH + "/dim_app_version")
    val locDf = sqlContext.read.parquet(Globals.DIMENSION_HDFS_BASE_PATH + "/dim_web_location")
    fact.registerTempTable("a")
    userDf.registerTempTable("b")
    userLoginDf.registerTempTable("c")
    productDf.registerTempTable("d")
    promotionDf.registerTempTable("e")
    appDf.registerTempTable("f")
    locDf.registerTempTable("g")

    sqlContext.sql("select a.product_serial,a.sys_ver,a.wifi_mac,a.app_name,a.ip,a.mac,a.product_model," +
      "a.product_version,a.promotion_channel,a.sn,a.log_timestamp,a.user_id," +
      "a.user_type,a.version,a.dim_date,a.dim_time " +
      "from a left join b on a.user_sk = b.user_sk " +
      "left join c on a.user_login_sk = c.user_login_sk " +
      "left join d on a.product_model_sk = d.product_model_sk " +
      "left join e on a.promotion_sk = e.promotion_sk " +
      "left join f on a.app_version_sk = f.app_version_sk " +
      "left join g on a.web_location_sk = g.web_location_sk " )
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
    HdfsUtil.deleteHDFSFileOrPath("/tmp/jointest2")
    df.write.parquet("/tmp/jointest2")
  }
}
