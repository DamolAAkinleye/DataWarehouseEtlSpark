package cn.whaley.datawarehouse.dimension.moretv

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}


/**
  * Created by Tony
  * 搜索维度表
  */
object Search extends DimensionBase {

  dimensionName = "dim_medusa_search"

  columns.skName = "search_sk"

  columns.primaryKeys = List("search_from", "search_tab", "search_from_hot_word", "search_result_index")

  columns.trackingColumns = List()

  columns.allColumns = List(
    "search_from",
    "search_tab",
    "search_from_hot_word",
    "search_result_index")


  readSourceType = jdbc

  sourceDb = MysqlDB.dwDimensionDb("moretv_search")


  override def filterSource(sourceDf: DataFrame): DataFrame = {
    val searchFromDf = sourceDf.where("content = 'search_from'").select("value")
      .withColumnRenamed("value", "search_from").withColumn("fake_id", lit(1))
    val searchTabDf = sourceDf.where("content = 'search_tab'").select("value")
      .withColumnRenamed("value", "search_tab").withColumn("fake_id", lit(1))

//    val searchFromHotWordDf = sqlContext.createDataFrame(
//      (0 to 1).map(s => Row.fromTuple(Seq(s))),
//      StructType(Array(StructField("search_from_hot_word", IntegerType)))
//    ).withColumn("fake_id", lit(1))

        searchFromDf.join(searchTabDf, List("fake_id"), "leftouter")
      .withColumn("search_from_hot_word", lit(0))
      .withColumn("search_result_index", lit("未知"))
  }
}
