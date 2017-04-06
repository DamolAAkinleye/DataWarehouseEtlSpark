package cn.whaley.datawarehouse.fact.constant

/**
  * Created by Tony on 17/3/8.
  */
object SourceType extends Enumeration{
  type SourceType = Value
  val parquet = Value
  val custom = Value  //自定义源数据
}
