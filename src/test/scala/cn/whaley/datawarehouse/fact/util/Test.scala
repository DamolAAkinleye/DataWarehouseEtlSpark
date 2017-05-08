package cn.whaley.datawarehouse.fact.util

/**
  * Created by baozhiwang on 2017/5/8.
  */
object Test extends App{
  val location_code="a"


  location_code match {
    case "a" =>  println("1")
    case "a" => println("2")
    case "b" => println("3")
    case _ =>
  }

}
