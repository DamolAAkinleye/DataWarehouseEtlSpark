//package cn.whaley.datawarehouse.normalized.tag
//
//import cn.whaley.datawarehouse.normalized.NormalizedEtlBase
//import cn.whaley.datawarehouse.util.{DataExtractUtils, MysqlDB, Params}
//import org.apache.spark.sql.DataFrame
//import org.apache.spark.sql.functions._
//
///**
//  * Created by huanghu on 17/8/18.
//  */
//object StkInStockInfomation extends NormalizedEtlBase{
//
//  tableName = "stk_in_stock_information"
//
//  override def extract(params: Params): DataFrame = {
//    val sourceDb = MysqlDB.whaleyWms("stk_instock", "order_no", 1, 500000, 100)
//    DataExtractUtils.readFromJdbc(sqlContext,sourceDb)
//
//    val sourceDb2 = MysqlDB.whaleyWms("stk_instock_detail", "order_no", 1, 500000, 100)
//    val inStockDetail = DataExtractUtils.readFromJdbc(sqlContext, sourceDb2)
//
//    val sourceDb3 = MysqlDB.whaleyWms("whare_house", "wharehouse_code", 1, 1000, 10)
//    val whareHouse = DataExtractUtils.readFromJdbc(sqlContext, sourceDb3)
//
//
//  }
//
//
//  override def transform(params: Params, df: DataFrame): DataFrame = {
//  df
//  }
//}
