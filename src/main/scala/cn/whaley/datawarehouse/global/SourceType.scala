package cn.whaley.datawarehouse.global

/**
  * Created by Tony on 17/3/8.
  */
object SourceType extends Enumeration{
  type SourceType = Value
  val jdbc = Value
  val parquet = Value
  val custom = Value  //自定义获取源数据
}
