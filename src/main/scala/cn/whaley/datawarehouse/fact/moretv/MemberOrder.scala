package cn.whaley.datawarehouse.fact.moretv

import java.io.File
import java.util.{Calendar, Date}

import cn.whaley.datawarehouse.common.{DimensionColumn, DimensionJoinCondition, UserDefinedColumn}
import cn.whaley.datawarehouse.fact.FactEtlBase
import cn.whaley.datawarehouse.fact.constant.Constants.FACT_HDFS_BASE_PATH_TMP
import cn.whaley.datawarehouse.fact.constant.LogPath
import cn.whaley.datawarehouse.global.FilterType
import cn.whaley.datawarehouse.util._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, min, udf}

import scala.collection.mutable.ListBuffer

/**
  * Created by xia.jun on 2017/11/9.
  */
object MemberOrder extends FactEtlBase{

  topicName = "fact_medusa_member_order"

  columnsFromSource = List(
    ("order_code","order_code"),
    ("order_desc","order_desc"),
    ("vip_pay_time","vip_pay_time"),
    ("vip_start_time","vip_start_time"),
    ("vip_end_time","vip_end_time"),
    ("activity_id","activity_id"),
    ("total_price","total_price"),
    ("real_price", "real_price"),
    ("payment_amount","payment_amount"),
    ("business_type","business_type"),
    ("pay_channel","pay_channel"),
    ("order_channel","order_channel"),
    ("order_type","order_type"),
    ("valid_status","valid_status"),
    ("trade_status","trade_status"),
    ("pay_status","pay_status"),
    ("duration","duration"),
    ("cid", "cid"),
    ("create_time","create_time"),
    ("update_time","update_time"),
    ("dim_date","dim_date"),
    ("dim_time","dim_time")
  )

  override def readSource(sourceDate: String, sourceHour: String): DataFrame = {
    val sourceDB = MysqlDB.medusaMemberDB("business_order")
    val businessOrderDF = DataExtractUtils.readFromJdbc(sqlContext,sourceDB)
    var df = businessOrderDF.filter(s"substring(create_time,0,10) = '${DateFormatUtils.cnFormat.format(DateFormatUtils.readFormat.parse(sourceDate))}'")
    if(sourceHour != null) {
      df = df.filter(s"substring(create_time,12,2) = '$sourceHour'")
    }
    df
  }

  override def load(params: Params, df: DataFrame) = {
    val goodsDF = DataExtractUtils.readFromParquet(sqlContext,LogPath.DIM_MEDUSA_MEMBER_GOOD).filter("dim_invalid_time is null and is_valid = 1").select("good_sk","good_name").withColumnRenamed("good_sk", "dim_good_sk")
    val finalDf = addContMonOrderFlag(params,df,goodsDF)
    super.load(params,finalDf)
  }

  def addContMonOrderFlag(params: Params,df:DataFrame,goodsDF:DataFrame):DataFrame = {
    params.paramMap.get("date") match {
      case Some(p) => {
        if(!p.toString.equals("20170811")) {

          val startDate = DateFormatUtils.readFormat.parse("20170811")
          val endDate = DateFormatUtils.readFormat.parse(p.toString)
          val pathDate = getPathDate(startDate, endDate)
          val previousOrderDF = DataExtractUtils.readFromParquet(sqlContext, LogPath.FACT_MEDUSA_ORDER, pathDate)

          val previousConMonOrderDF = previousOrderDF.join(goodsDF,previousOrderDF("good_sk")===goodsDF("dim_good_sk")).
            filter(s"good_name = '${FilterType.CONSECUTIVE_MONTH_ORDER}'").select("account_sk","vip_end_time")
          val mergerDF = df.join(goodsDF,df("good_sk") === goodsDF("dim_good_sk"))

          /** Case One: 非连续包月订单*/
          val todayNonConMonOrderDF = mergerDF.filter(s"good_name != '${FilterType.CONSECUTIVE_MONTH_ORDER}'").
            drop("dim_good_sk").drop("good_name").withColumn("is_first_cont_mon_order", lit(0))

          /** Case Two: 连续包月订单*/
          val todayConMonOrderDF = mergerDF.filter(s"good_name = '${FilterType.CONSECUTIVE_MONTH_ORDER}'")
            .drop("dim_good_sk").drop("good_name")

          // 获取当日订单中vip_start_time最早的一笔订单（这种情况是用来处理一天内出现多笔连续包月的订单情况）
          val todayAccountFirstVipStartDateDF  = todayConMonOrderDF.groupBy("account_sk").agg(min(("vip_start_time"))).withColumnRenamed("account_sk","account_sk_first")
          val todayFirstVipConMonOrderDF = todayConMonOrderDF.join(todayAccountFirstVipStartDateDF,
            todayConMonOrderDF("account_sk") === todayAccountFirstVipStartDateDF("account_sk_first") &&
              todayConMonOrderDF("vip_start_time") === todayAccountFirstVipStartDateDF("min(vip_start_time)")).
            drop(todayAccountFirstVipStartDateDF("account_sk_first")).drop(todayAccountFirstVipStartDateDF("min(vip_start_time)"))

          val todayNonFirstVipConMonOrderDF = todayConMonOrderDF.except(todayFirstVipConMonOrderDF).
            withColumn("is_first_cont_mon_order", lit(0))

          // 处理当日内的订单是否是属于之前连续包月订单中的一部分
          val nonFirstConMonOrderDF = todayFirstVipConMonOrderDF.join(previousConMonOrderDF,
            todayFirstVipConMonOrderDF("account_sk") === previousConMonOrderDF("account_sk") &&
              getDate(todayFirstVipConMonOrderDF("vip_start_time"))=== getDate(previousConMonOrderDF("vip_end_time"))).
            select(todayFirstVipConMonOrderDF("order_code"))
          val nonFirstConMonDF = todayFirstVipConMonOrderDF.join(nonFirstConMonOrderDF,Seq("order_code"))

          val firstConMonDF = todayFirstVipConMonOrderDF.except(nonFirstConMonDF).withColumn("is_first_cont_mon_order", lit(1))

          todayNonConMonOrderDF.union(todayNonFirstVipConMonOrderDF).union(nonFirstConMonDF.withColumn("is_first_cont_mon_order",lit(0))).union(firstConMonDF)

        } else {
          df.withColumn("is_first_cont_mon_order",lit(0))
        }
      }
      case None => {
        throw new RuntimeException("未设置时间参数！")
      }
    }

  }

  addColumns = List(
    UserDefinedColumn("dim_date", udf(getDimDate: String => String), List("create_time")),
    UserDefinedColumn("dim_time", udf(getDimTime: String => String), List("create_time"))
  )


  dimensionColumns = List(

    /** 基于订单中的real_price获取对应的商品维度good_sk */
    new DimensionColumn("dim_medusa_member_goods",
      List(DimensionJoinCondition(Map("payment_amount" -> "good_price", "member_code" -> "member_code"))),
      "good_sk","good_sk"),

    /** 基于订单中的account_id获取账号表中的账号维度account_sk */
    new DimensionColumn("dim_medusa_account",
      List(DimensionJoinCondition(Map("account_id" -> "account_id"))),
      "account_sk","account_sk")
  )

  val getDate = udf((time: String) => {
    time.substring(0,10)
  })



  def getDimDate(dateTime: String): String = {
    try {
      val dateTimeInfo = dateTime.split(" ")
      if (dateTimeInfo.length >= 2) {
        dateTimeInfo(0)
      } else ""
    } catch {
      case ex: Exception => ""
    }
  }

  def getDimTime(dateTime: String): String = {
    try {
      val dateTimeInfo = dateTime.split(" ")
      if (dateTimeInfo.length >= 2) {
        dateTimeInfo(1)
      } else ""
    } catch {
      case ex: Exception => ""
    }
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
