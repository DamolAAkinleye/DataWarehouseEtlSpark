package cn.whaley.datawarehouse.dimension.constant

/**
  * Created by Tony on 17/3/8.
  */
object SourceType extends Enumeration{
  type SourceType = Value
  val jdbc = Value
  val custom = Value  //自定义去除源数据
}
