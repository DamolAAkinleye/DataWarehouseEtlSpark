package cn.whaley.datawarehouse.fact.common

import org.apache.spark.sql.UserDefinedFunction

/**
  * Created by Tony on 17/4/6.
  */
case class UserDefinedColumn(name: String,
                             udf: UserDefinedFunction,
                             inputColumns: List[String]) {

}
