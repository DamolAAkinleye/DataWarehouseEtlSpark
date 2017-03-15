package cn.whaley.datawarehouse.util

/**
  * Created by Tony on 16/12/26.
  */
object MysqlDB {

  def medusaUCenterMember = {
    Map("url" -> "jdbc:mysql://bigdata-extsvr-db_moretv_ucenter:3306/ucenter?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
      "dbtable" -> "bbs_ucenter_members",
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> "bislave",
      "password" -> "slave4bi@whaley",
      "partitionColumn" -> "uid",
      "lowerBound" -> "1",
      "upperBound" -> "4619253",
      "numPartitions" -> "10")
  }

  def medusaTvServiceAccount = {
    Map("url" -> "jdbc:mysql://bigdata-appsvr-130-3:3306/tvservice?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
      "dbtable" -> "mtv_account",
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> "bislave",
      "password" -> "slave4bi@whaley",
      "partitionColumn" -> "id",
      "lowerBound" -> "1",
      "upperBound" -> "800000000",
      "numPartitions" -> "300")
  }

  def medusaCms(table: String, partitionColumn: String, lowerBound: Long, upperBound: Long, numPartitions:Int ) = {
    Map("url" -> "jdbc:mysql://bigdata-appsvr-130-2:3306/mtv_cms?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
      "dbtable" -> table,
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> "bislave",
      "password" -> "slave4bi@whaley",
      "partitionColumn" -> partitionColumn,
      "lowerBound" -> lowerBound.toString,
      "upperBound" -> upperBound.toString,
      "numPartitions" -> numPartitions.toString)
  }

  def dwDimensionDb(table: String) = {
    Map("url" -> "jdbc:mysql://bigdata-extsvr-db_bi2:3306/dw_dimension?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
      "dbtable" -> table, //"moretv_app_version",
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> "bi",
      "password" -> "mlw321@moretv")
  }

  def whaleyCms(table: String, partitionColumn: String, lowerBound: Long, upperBound: Long, numPartitions:Int ) = {
    Map("url" -> "jdbc:mysql://bigdata-appsvr-130-1:3306/mtv_cms?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
      "dbtable" -> table,
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> "bislave",
      "password" -> "slave4bi@whaley",
      "partitionColumn" -> partitionColumn,
      "lowerBound" -> lowerBound.toString,
      "upperBound" -> upperBound.toString,
      "numPartitions" -> numPartitions.toString)
  }

  def whaleyDolphin(table: String, partitionColumn: String, lowerBound: Long, upperBound: Long, numPartitions:Int ) = {
    Map("url" -> "jdbc:mysql://bigdata-extsvr-db_whaley_dlph_tmnl:3306/dolphin_terminal?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
      "dbtable" -> table,
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> "bislave",
      "password" -> "slave4bi@whaley",
      "partitionColumn" -> partitionColumn,
      "lowerBound" -> lowerBound.toString,
      "upperBound" -> upperBound.toString,
      "numPartitions" -> numPartitions.toString)
  }

//  def whaleyDimension(table: String, partitionColumn: String, lowerBound: Long, upperBound: Long, numPartitions:Int ) = {
//    Map("url" -> "jdbc:mysql://10.10.2.16:3306/dw_dimension?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
//      "dbtable" -> table,
//      "driver" -> "com.mysql.jdbc.Driver",
//      "user" -> "dw_user",
//      "password" -> "dw_user@whaley",
//      "partitionColumn" -> partitionColumn,
//      "lowerBound" -> lowerBound.toString,
//      "upperBound" -> upperBound.toString,
//      "numPartitions" -> numPartitions.toString)
//  }

  def whaleyAvccount(table: String, partitionColumn: String, lowerBound: Long, upperBound: Long, numPartitions:Int ) = {
    Map("url" -> "jdbc:mysql://bigdata-extsvr-db_whaley_ucenter:3306/ucenter?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
      "dbtable" -> table,
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> "bislave",
      "password" -> "slave4bi@whaley",
      "partitionColumn" -> partitionColumn,
      "lowerBound" -> lowerBound.toString,
      "upperBound" -> upperBound.toString,
      "numPartitions" -> numPartitions.toString)
  }


  def whaleyTerminalMember = {
    Map("url" -> "jdbc:mysql://bigdata-extsvr-db_whaley_tmnl_upg:3306/terminal_upgrade?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
      "dbtable" -> "mtv_terminal",
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> "bislave",
      "password" -> "slave4bi@whaley",
      "partitionColumn" -> "serial_number",
      "lowerBound" -> "1",
      "upperBound" -> "500000",
      "numPartitions" -> "10")
  }



  def mergerActivity =  {
    Map("url" -> "jdbc:mysql://bigdata-extsvr-db_bi1:3306/eagletv?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
      "dbtable" -> "mtv_activity",
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> "bi",
      "password" -> "mlw321@moretv",
      "partitionColumn" -> "sid",
      "lowerBound" -> "1",
      "upperBound" -> "50",
      "numPartitions" -> "2"
    )
  }

  def whaleyApp(table: String, partitionColumn: String, lowerBound: Long, upperBound: Long, numPartitions:Int ) = {
    Map("url" -> "jdbc:mysql://10.10.72.124:3306/app_cms?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
      "dbtable" -> table,
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> "biread",
      "password" -> "bigdataTV@608_810",
      "partitionColumn" -> partitionColumn,
      "lowerBound" -> lowerBound.toString,
      "upperBound" -> upperBound.toString,
      "numPartitions" -> numPartitions.toString)
  }

}
