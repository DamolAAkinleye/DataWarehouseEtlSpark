package cn.whaley.datawarehouse.dimension.moretv

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{DataType, StringType}

/**
  * Created by Tony on 17/3/31.
  */
object PageEntrance extends DimensionBase {

  columns.skName = "page_entrance_sk"
  columns.primaryKeys = List("page_entrance_id")
  columns.trackingColumns = List()
  columns.allColumns = List(
    "page_entrance_id",
    "page_code",
    "page_name",
    "area_code",
    "area_name",
    "location_code",
    "location_name")

  readSourceType = jdbc

  //维度表的字段对应源数据的获取方式
  sourceColumnMap = Map(
    "page_entrance_id" -> "id"
  )


  sourceDb = MysqlDB.dwDimensionDb("moretv_page_entrance")

  //TODO 读取电视猫cms中的表
  sourceDbFromCms = MysqlDB.medusaCms("mtv_layoutPosition","id", 1, 3000, 20)

  dimensionName = "dim_medusa_page_entrance"

  fullUpdate = true

  val SourceType = jdbc

  override def readSource(SourceType: Value): Dataset[Row] = {

    //TODO 读取mysql中moretv_page_entrance
    val result_mysql: DataFrame = super.readSource(readSourceType)

    //TODO 读另外一个表, 并且跟df对齐
    val df_cms: DataFrame = sqlContext.read.format("jdbc").options(sourceDbFromCms).load()

    //TODO 取出所需列生成新的df
    val result_origin: DataFrame = df_cms.select("id","code","title").filter("content_type='game' and code like '%game%' and length(title) <= 4").toDF()

    //TODO 添加新列
    val fields = List(
      ("page_code", "game", StringType),
      ("page_name", "游戏首页", StringType),
      ("location_code", "", StringType),
      ("location_name", "", StringType),
      ("content_type", "game", StringType),
      ("update_time", "0000-00-00 00:00:00", StringType)
    )
    val  result_df_1 = addColumn(result_origin, fields)


    //TODO 列重新命名
    val result_df_2 = result_df_1.withColumnRenamed("code","area_code")
    val result_df = result_df_2.withColumnRenamed("title","area_name")

    //TODO 保持列对齐
    val result_cms = result_df.select("id","page_code","page_name","area_code","area_name","location_code","location_name","content_type","update_time")

    //TODO 两个表union，输出

    result_mysql.union(result_cms)
  }

  def addColumn(df: DataFrame, fields: List[(String, Any, DataType)]): DataFrame = {
    var dataFrame: DataFrame = df
    fields.foreach(tuple => {
      val field = tuple._1
      val value = tuple._2
      val dataType = tuple._3
      val flag = dataFrame.schema.fieldNames.contains(field)
      if (!flag) {
        dataFrame = dataFrame.withColumn(field, lit(value).cast(dataType))
      }
    })
    dataFrame
  }

}
