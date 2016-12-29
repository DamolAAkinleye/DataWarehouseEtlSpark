package cn.whaley.datawarehouse.temp

import cn.whaley.datawarehouse.BaseClass
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Created by Tony on 16/12/28.
  */
object Test extends BaseClass {

  override def execute(args: Array[String]): Unit = {
    val df = sqlContext.read.load("/data_warehouse/dw_dimensions/dim_app_version")
    val window = Window.orderBy(df("app_version_sk").desc)
    df.select(df("app_version_sk"),row_number().over(window).as("r")).show()

    df.registerTempTable("log")
    sqlContext.sql("select Row_Number() OVER (ORDER BY app_version_sk desc), app_version_sk from log limit 20").show(20)
  }

}
