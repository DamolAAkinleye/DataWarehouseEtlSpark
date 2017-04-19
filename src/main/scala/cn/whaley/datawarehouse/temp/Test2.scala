package cn.whaley.datawarehouse.temp

/**
  * 创建人：郭浩
  * 创建时间：2017/4/13
  * 程序作用：
  * 数据输入：
  * 数据输出：
  */
object Test2 {
  def main(args: Array[String]): Unit = {
    val str = "a10 120 2"
    val arr = str.split(" ")
    val partNum = arr(0)
    val price = arr(1)
    val num = str.split(" ")(2).toInt
    (1 to num).foreach(i=>{
      println(partNum,price,i)
    })
  }

}
