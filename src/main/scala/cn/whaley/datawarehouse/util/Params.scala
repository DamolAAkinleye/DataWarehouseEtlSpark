package cn.whaley.datawarehouse.util

/**
  * Created by baozhiwang on 2017/3/6.
  */

case class Params (
                   deleteOld:Boolean=false, //是否删除旧数据
                   dimensionType:String = "", //维度类型
                   isOnline:Boolean=false, //是否替换线上维度
                   paramMap: Map[String,String] = Map[String,String]() //参数集合
                  ){

  def get[T](key:String) = paramMap(key).asInstanceOf[T]

  def getOrElse[T](key:String,default: => T) =
    paramMap.get(key) match {
      case Some(value) => value.asInstanceOf[T]
      case None => default
    }

  def getString = get[String] _
  def getInt = get[Int] _
  def getLong = get[Long] _
  def getDouble = get[Double] _
  def getBoolean = get[Boolean] _
  def contains = paramMap.contains _

}
