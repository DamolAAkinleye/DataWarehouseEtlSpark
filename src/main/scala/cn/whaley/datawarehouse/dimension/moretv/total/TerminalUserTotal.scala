package cn.whaley.datawarehouse.dimension.moretv.total

import cn.whaley.datawarehouse.BaseClass
import cn.whaley.datawarehouse.util.{HdfsUtil, MysqlDB}

/**
  * Created by Tony on 16/12/22.
  *
  * 电视猫终端用户信息全量更新
  */
object TerminalUserTotal extends BaseClass {
  override def execute(args: Array[String]): Unit = {

    val jdbcDF = sqlContext.read.format("jdbc").options(MysqlDB.medusaTvServiceAccount).load()
    jdbcDF.registerTempTable("mtv_account")

    val df = sqlContext.sql("SELECT cast(id as long) as terminal_sk, user_id, openTime as open_time, mac, wifi_mac, " +
      "product_model, product_serial, promotion_channel, lastLoginTime as last_login_time " +
      " from mtv_account " +
      " where user_id is not null and user_id <> ''")

    HdfsUtil.deleteHDFSFileOrPath("/data_warehouse/dw_dimensions/dim_medusa_terminal_user")
    df.write.parquet("/data_warehouse/dw_dimensions/dim_medusa_terminal_user")
  }
}
