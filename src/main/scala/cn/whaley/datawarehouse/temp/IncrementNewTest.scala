package cn.whaley.datawarehouse.temp

import cn.whaley.datawarehouse.dimension.DimensionBase

/**
  * Created by Tony on 17/3/8.
  */
object IncrementNewTest extends DimensionBase{
  columns.primaryKeys = List("id")
  columns.trackingColumns = List("title")
  columns.otherColumns = List("code", "type")
  columns.skName = "sk"

  sourceDb = {
    Map("url" -> "jdbc:mysql://10.10.2.16:3306/dw_dimension?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
      "dbtable" -> "test",
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> "dw_user",
      "password" -> "dw_user@wha1ey")
  }

  dimensionName = "test"
}
