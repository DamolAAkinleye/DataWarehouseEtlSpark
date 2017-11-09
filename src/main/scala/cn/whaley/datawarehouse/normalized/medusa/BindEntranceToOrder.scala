package cn.whaley.datawarehouse.normalized.medusa

import java.util.{Calendar, Date}
import cn.whaley.datawarehouse.BaseClass
import cn.whaley.datawarehouse.fact.constant.LogPath
import cn.whaley.datawarehouse.util.{DataExtractUtils, DateFormatUtils, Params}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import scala.collection.mutable.ListBuffer

/**
  * Created by xiajun on 2017/11/8.
  * 关联订单号与产生该订单的入口信息
  */
object BindEntranceToOrder extends BaseClass{

  override def extract(params: Params) = {
    params.paramMap.get("date") match {
      case Some(p) => {
        //数据库快照路径中的日期需要减1天
        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.cnFormat.parse(p.toString))
        calendar.add(Calendar.DAY_OF_MONTH, -1)
        val snapShotDate = DateFormatUtils.readFormat.format(calendar.getTime)
        val accountUidDF = DataExtractUtils.readFromParquet(sqlContext, LogPath.MEDUSA_ACCOUNT_UID_MAP)
        val businessOrderDF = DataExtractUtils.readFromParquet(sqlContext, LogPath.MEDUSA_BUSINESS_ORDER, snapShotDate).
          filter("total_price != 0")
        /** 根据订单中的account_id信息，绑定相应的user_id信息 */
        val orderDF = businessOrderDF.join(accountUidDF,businessOrderDF("account_id") === accountUidDF("account_id")).
          select(businessOrderDF("order_code"), businessOrderDF("account_id"), accountUidDF("user_id"), businessOrderDF("create_time")).
          withColumn("create_date",col("create_time").substr(0,10))

        params.mode match {
          case "all" => {
            val startDate = DateFormatUtils.readFormat.parse("20170901")
            val endDate = DateFormatUtils.readFormat.parse(p.toString)
            val pathDate = getPathDate(startDate, endDate)
            val entranceDF = DataExtractUtils.readFromParquet(sqlContext, LogPath.MEDUSA_PURCHASE_ENTRANCE, pathDate)
            bindEntrance(orderDF, entranceDF)
          }
          case "increment" => {
            val entranceDF = DataExtractUtils.readFromParquet(sqlContext, LogPath.MEDUSA_PURCHASE_ENTRANCE, p.toString)
            bindEntrance(orderDF, entranceDF)
          }
        }
      }
      case None => throw new RuntimeException("未设置时间参数！")
    }
  }

  override def transform(params: Params, df: DataFrame) = {
    df
  }

  override def load(params: Params, df: DataFrame) = {}

  /**
    * 绑定购买入口信息
    * @param orderDF
    * @param entranceDF
    */
  def bindEntrance(orderDF:DataFrame, entranceDF:DataFrame):DataFrame = {


    // Step 1: 根据account_id进行关联
    val allEntranceByAccountIdDF = orderDF.join(entranceDF,
      orderDF("account_id") === entranceDF("accountId") && orderDF("create_date") === entranceDF("date"))

    val orderEntranceByAccountDF = allEntranceByAccountIdDF.filter(col("create_time") > col("datetime")).
      groupBy("order_code","account_id","create_date").agg("datetime" -> "max")

    val entranceByAccountDF = orderEntranceByAccountDF.join(entranceDF,
      orderEntranceByAccountDF("account_id") === entranceDF("accountId") &&
        orderEntranceByAccountDF("max(datetime)") === entranceDF("datetime")).
      select("order_code","account_id", "userId","entrance","videoSid").withColumnRenamed("userId", "user_id")

    // Step 2: 根据uid进行关联
    val allEntranceByUidDF = orderDF.join(entranceDF,
      orderDF("user_id") === entranceDF("userId") && orderDF("create_date") === entranceDF("date"))

    val orderEntranceByUidDF = allEntranceByUidDF.filter(col("create_time") > col("datetime")).
      groupBy("order_code", "user_id","create_date").agg("datetime" -> "max")

    val entranceByUidDF = orderEntranceByUidDF.join(entranceDF,
      orderEntranceByUidDF("user_id") === entranceDF("userId") &&
        orderEntranceByUidDF("max(datetime)") === entranceDF("datetime")).
      select("order_code", "accountId","user_id","entrance","videoSid").withColumnRenamed("accountId", "account_id")

    entranceByAccountDF.union(entranceByUidDF)
  }

  /**
    * 提取日志路径中的日期信息
    * @param startDate
    * @param endDate
    * @return
    */
  def getPathDate(startDate:Date, endDate:Date):String = {
    var dateArr = ListBuffer[String]()
    val dateDiffs = (endDate.getTime - startDate.getTime) / (1000*3600*24)
    val calendar = Calendar.getInstance()
    calendar.setTime(startDate)
    (0 to dateDiffs.toInt).foreach(i => {
      dateArr.+=(DateFormatUtils.readFormat.format(calendar.getTime))
      calendar.add(Calendar.DAY_OF_MONTH, 1)
    })
    "{" + dateArr.mkString(",") + "}"
  }


}
