package cn.whaley.datawarehouse.util

/**
  * Created by baozhiwang on 2017/3/6.
  */

case class Params(
                   isOnline: Boolean = false, //参数决定维度数据是否上线
                   isBackup: Boolean = true, //参数决定是否备份线上数据，第一次备份数据才可以作为回滚数据。当涉及到多次执行,设置isBackup=false
                   startDate: String = "yyyyMMdd",
                   paramMap: Map[String, Any] = Map[String,Any]() //其他非命令行参数
                 )

