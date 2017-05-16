package cn.whaley.datawarehouse.fact.moretv

import java.io.File
import java.util.Calendar

import cn.whaley.datawarehouse.common.{DimensionColumn, DimensionJoinCondition, UserDefinedColumn}
import cn.whaley.datawarehouse.fact.FactEtlBase
import cn.whaley.datawarehouse.fact.moretv.Play._
import cn.whaley.datawarehouse.fact.util._
import cn.whaley.datawarehouse.global.{Constants, Globals, LogConfig, LogTypes}
import cn.whaley.datawarehouse.util._
import cn.whaley.sdk.dataexchangeio.DataIO
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


/**
  * Created by michel on 17/4/24.
  */
object PlayTest extends FactEtlBase with  LogConfig{
  /** log type name */
  topicName = "fact_medusa_play"
  partition = 500
  /**
    * step 1, get data source
    * */
  override def readSource(startDate: String): DataFrame = {
    println("------- before readSource "+Calendar.getInstance().getTime)
    val medusa_input_dir = DataIO.getDataFrameOps.getPath(MEDUSA, LogTypes.PLAY, startDate)
    val moretv_input_dir = DataIO.getDataFrameOps.getPath(MORETV, LogTypes.PLAYVIEW, startDate)
    val medusaFlag = HdfsUtil.IsInputGenerateSuccess(medusa_input_dir)
    val moretvFlag = HdfsUtil.IsInputGenerateSuccess(moretv_input_dir)
    if (medusaFlag && moretvFlag) {
      val medusaDf = DataIO.getDataFrameOps.getDF(sqlContext, Map[String,String](), MEDUSA, LogTypes.PLAY, startDate).withColumn("flag",lit(MEDUSA))
      val moretvDf = DataIO.getDataFrameOps.getDF(sqlContext, Map[String,String](), MORETV, LogTypes.PLAYVIEW, startDate).withColumn("flag",lit(MORETV))
      val medusaRDD=medusaDf.toJSON
      val moretvRDD=moretvDf.toJSON
      val mergerRDD=medusaRDD.union(moretvRDD)
      val mergerDataFrame = sqlContext.read.json(mergerRDD).toDF()
      mergerDataFrame.randomSplit(Array(0.2,0.8)).head
    }else{
      throw new RuntimeException("medusaFlag or moretvFlag is false")
    }
  }

  /**
    * step 2, filter data source record
    * */

  /**
    * step 3, generate new columns
    * */
  addColumns = List(
   UserDefinedColumn("recommendSourceType", udf(RecommendUtils.getRecommendSourceType: (String,String,String) => String), List("pathSub", "path", "flag")),
    UserDefinedColumn("recommendLogType", udf(RecommendUtils.getRecommendLogType: (String,String,String) => String), List("pathSub", "path", "flag")),
    UserDefinedColumn("previousSid", udf(RecommendUtils.getPreviousSid: (String) => String), List("pathSub")),
    UserDefinedColumn("previousContentType", udf(RecommendUtils.getPreviousContentType: (String) => String), List("pathSub")),
    UserDefinedColumn("recommendSlotIndex", udf(RecommendUtils.getRecommendSlotIndex: (String) => String), List("pathMain"))
   )

  /**
    * step 4, left join dimension table,get sk
    * */
  dimensionColumns = List(
    /** 获得推荐来源sk */
    RecommendUtils.getRecommendPositionSK()
  )

  /**
    * step 5,保留哪些列，以及别名声明
    * */
  columnsFromSource = List(
    ("recommendSourceType", "recommendSourceType"),
    ("previousSid", "previousSid"),
    ("previousContentType", "previousContentType"),
    ("recommendSlotIndex", "recommendSlotIndex"),
    ("recommendType", "recommendType"),
    ("recommendLogType", "recommendLogType"),
    ("pathMain", "pathMain"),
    ("path", "path"),
    ("pathSpecial", "pathSpecial"),
    ("pathSub", "pathSub")
  )

  factTime = null

 //will use common util function class instead
  def getIpKey(ip: String): Long = {
    try {
      val ipInfo = ip.split("\\.")
      if (ipInfo.length >= 3) {
        (((ipInfo(0).toLong * 256) + ipInfo(1).toLong) * 256 + ipInfo(2).toLong) * 256
      } else 0
    } catch {
      case ex: Exception => 0
    }
  }

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

  def getAppSeries(seriesAndVersion: String): String = {
    try {
      val index = seriesAndVersion.lastIndexOf("_")
      if (index > 0) {
        seriesAndVersion.substring(0, index)
      } else ""
    } catch {
      case ex: Exception => ""
    }
  }

  def getAppVersion(seriesAndVersion: String): String = {
    try {
      val index = seriesAndVersion.lastIndexOf("_")
      if (index > 0) {
        seriesAndVersion.substring(index + 1, seriesAndVersion.length)
      } else ""
    } catch {
      case ex: Exception => ""
    }
  }

}
