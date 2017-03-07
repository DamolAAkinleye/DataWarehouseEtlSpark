package cn.whaley.datawarehouse.dimentions.medusa

import cn.whaley.datawarehouse.BaseClass
import cn.whaley.datawarehouse.util.{MysqlDB}
import org.apache.spark.sql.DataFrame

/**
  *  数据备份参考
  *  参数 --deleteOld true
  */
object AccountTotalExample extends BaseClass {
  private val tableName = "bbs_ucenter_members"
  private val dimensionType = "dim_medusa_account"

  override def execute(args: Array[String]): (DataFrame,String) = {
    val jdbcDF = sqlContext.read.format("jdbc").options(MysqlDB.medusaUCenterMember).load()
    jdbcDF.registerTempTable(tableName)
    val df = sqlContext.sql("SELECT cast(moretvid as long) as account_sk, moretvid as account_id, " +
      "username as user_name, email, mobile, " +
      "cast(regdate as timestamp) as reg_time, " +
      "registerfrom as register_from " +
      s" from $tableName " +
      " where account_id is not null and account_id <> ''")
    (df,dimensionType)
  }
}
