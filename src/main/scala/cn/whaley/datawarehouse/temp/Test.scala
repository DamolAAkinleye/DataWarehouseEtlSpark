package cn.whaley.datawarehouse.temp

import cn.whaley.datawarehouse.BaseClass
import cn.whaley.datawarehouse.util.Params
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Created by Tony on 16/12/28.
  */
object Test extends BaseClass {

  override def execute(params: Params): Unit = {

    System.out.println(hiveContext)

    val listOfEmployees = List(Employee(1, "iteblog"), Employee(2, "Jason"), Employee(3, "Abhi"))
    val empFrame = sqlContext.createDataFrame(listOfEmployees)
    empFrame.registerTempTable("t")
    sqlContext.sql("select Row_Number() OVER (ORDER BY id), id, name from t limit 20").show(20)

//    val window = Window.orderBy(empFrame("id").desc)
//    empFrame.select(empFrame("id"), empFrame("name"), row_number().over(window).as("r")).show()
  }

  case class Employee(id: Int, name: String)
}
