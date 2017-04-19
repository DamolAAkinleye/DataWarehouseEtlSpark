package cn.whaley.datawarehouse.fact.whaley

import java.util.Calendar

import cn.whaley.datawarehouse.util.DateFormatUtils

/**
  * 创建人：郭浩
  * 创建时间：2017/4/18
  * 程序作用：
  * 数据输入：
  * 数据输出：
  */
object Test {
  def main(args: Array[String]): Unit = {

    val cal = Calendar.getInstance()
    cal.setTime(DateFormatUtils.readFormat.parse("20170418"))
    val time = DateFormatUtils.cnFormat.format(cal.getTime)
    println(time)
  }

}
