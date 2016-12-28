package cn.whaley.datawarehouse.dimentions

import cn.whaley.datawarehouse.BaseClass
import cn.whaley.datawarehouse.util.{HdfsUtil, MysqlDB}

/**
  * Created by Tony on 16/12/23.
  */
object AppVersion extends BaseClass {
  override def execute(args: Array[String]): Unit = {
    val jdbcDF = sqlContext.read.format("jdbc").options(MysqlDB.medusaAppVersion).load()
    jdbcDF.registerTempTable("moretv_app_version")

    val df = sqlContext.sql("SELECT cast((id + 10000) as int) as app_version_sk, app_name, app_en_name, " +
      "'' as app_id, app_series, version, build_time,  " +
      "'微鲸' as company, '' as product" +
      " from moretv_app_version")

    HdfsUtil.deleteHDFSFileOrPath("/data_warehouse/dimensions/app_version")
    df.write.parquet("/data_warehouse/dimensions/app_version")
  }
}
