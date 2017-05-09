package cn.whaley.datawarehouse.fact.moretv

import cn.whaley.datawarehouse.common.{DimensionColumn, DimensionJoinCondition, UserDefinedColumn}
import cn.whaley.datawarehouse.fact.util._
import cn.whaley.datawarehouse.fact.FactEtlBase
import cn.whaley.datawarehouse.global.{LogConfig, LogTypes}
import cn.whaley.datawarehouse.util._
import cn.whaley.sdk.dataexchangeio.DataIO
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._



/**
  * Created by michel on 17/4/24.
  */
object Play extends FactEtlBase with  LogConfig{

  /** log type name */
  topicName = "fact_medusa_play"

  /**
    * step 1, get data source
    * */
  override def readSource(startDate: String): DataFrame = {
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
      mergerDataFrame
    }else{
      throw new RuntimeException("medusaFlag or moretvFlag is false")
    }
  }

  /**
    * step 2, filter data source record
    * */
  override def filterRows(sourceDf: DataFrame): DataFrame = {
    /** 用于过滤单个用户播放单个视频量过大的情况 */
    val playNumLimit=5000
    sourceDf.registerTempTable("source_log")
    var sqlStr =
      s"""
         |select concat(userId,videoSid) as filterColumn
         |from source_log
         |group by concat(userId,videoSid)
         |having count(1)>=$playNumLimit
                     """.stripMargin
    sqlContext.sql(sqlStr).registerTempTable("table_filter")
    sqlStr =
      s"""
         |select a.*
         |from source_log         a
         |     left join
         |     table_filter       b
         |     on concat(a.userId,a.videoSid)=b.filterColumn
         |where b.filterColumn is null
                     """.stripMargin
    val resultDF = sqlContext.sql(sqlStr)
    resultDF
  }

  /**
    * step 3, generate new columns
    * */
  addColumns = List(
    UserDefinedColumn("ipKey", udf(getIpKey: String => Long), List("realIP")),
    UserDefinedColumn("dim_date", udf(getDimDate: String => String), List("datetime")),
    UserDefinedColumn("dim_time", udf(getDimTime: String => String), List("datetime")),
    UserDefinedColumn("app_series", udf(getAppSeries: String => String), List("version")),
    UserDefinedColumn("app_version", udf(getAppVersion: String => String), List("version")),
    UserDefinedColumn("subjectCode", udf(SubjectUtils.getSubjectCodeByPathETL: (String, String,String) => String), List("pathSpecial", "path", "flag")),
    UserDefinedColumn("subjectName", udf(SubjectUtils.getSubjectNameByPathETL: (String) => String), List("pathSpecial")),
    UserDefinedColumn("mainCategory", udf(ListCategoryUtils.getListMainCategory: (String,String,String) => String), List("pathMain", "path", "flag")),
    UserDefinedColumn("secondCategory",udf(ListCategoryUtils.getListSecondCategory: (String,String,String) => String), List("pathMain", "path", "flag")),
    UserDefinedColumn("thirdCategory", udf(ListCategoryUtils.getListThirdCategory: (String,String,String) => String), List("pathMain", "path", "flag")),
    UserDefinedColumn("launcherAreaCode", udf(EntranceTypeUtils.getEntranceAreaCode: (String, String,String) => String), List("pathMain", "path", "flag")),
    UserDefinedColumn("launcherLocationCode", udf(EntranceTypeUtils.getEntranceLocationCode: (String, String,String) => String), List("pathMain", "path", "flag")),
    UserDefinedColumn("filterCategoryFirst", udf(FilterCategoryUtils.getFilterCategoryFirst: (String,String,String) => String), List("pathMain", "path", "flag")),
    UserDefinedColumn("filterCategorySecond", udf(FilterCategoryUtils.getFilterCategorySecond: (String,String,String) => String), List("pathMain", "path", "flag")),
    UserDefinedColumn("filterCategoryThird", udf(FilterCategoryUtils.getFilterCategoryThird: (String,String,String) => String), List("pathMain", "path", "flag")),
    UserDefinedColumn("filterCategoryFourth", udf(FilterCategoryUtils.getFilterCategoryFourth: (String,String,String) => String), List("pathMain", "path", "flag")),
    UserDefinedColumn("recommendSourceType", udf(RecommendUtils.getRecommendSourceType: (String,String,String) => String), List("pathSub", "path", "flag")),
    UserDefinedColumn("previousSid", udf(RecommendUtils.getPreviousSid: (String) => String), List("pathSub")),
    UserDefinedColumn("previousContentType", udf(RecommendUtils.getPreviousContentType: (String) => String), List("pathSub")),
    UserDefinedColumn("recommendSlotIndex", udf(RecommendUtils.getRecommendSlotIndex: (String) => String), List("pathMain")),
    UserDefinedColumn("searchFrom", udf(SearchUtils.getSearchFrom: (String,String,String) => String),List("pathMain", "path", "flag")),
    UserDefinedColumn("searchKeyword", udf(SearchUtils.getSearchKeyword: (String,String,String) => String),List("pathMain", "path", "flag")),
    UserDefinedColumn("pageEntrancePageCode", udf(PageEntrancePathParseUtils.getPageEntrancePageCode: (String, String,String) => String), List("pathMain", "path", "flag")),
    UserDefinedColumn("pageEntranceAreaCode", udf(PageEntrancePathParseUtils.getPageEntranceAreaCode: (String, String,String) => String), List("pathMain", "path", "flag")),
    UserDefinedColumn("pageEntranceLocationCode", udf(PageEntrancePathParseUtils.getPageEntranceLocationCode: (String, String,String) => String), List("pathMain", "path", "flag"))
  )

  /**
    * step 4, left join dimension table,get sk
    * */
  dimensionColumns = List(
    /** 获得列表页sk source_site_sk */
    ListCategoryUtils.getSourceSiteSK(),

    /** 获得专题 subject_sk */
    SubjectUtils.getSubjectSK(),

    /** 获得筛选sk */

    /** 获得推荐来源sk */

    /** 获得搜索来源sk */

    /** 获得频道主页来源维度sk（只有少儿，音乐，体育有频道主页来源维度）*/
    PageEntrancePathParseUtils.getPageEntranceSK(),

    /** 获得首页入口 launcher_entrance_sk */
    EntranceTypeUtils.getLauncherEntranceSK(),

    /** 获得用户ip对应的地域维度user_web_location_sk */

    /** 获得访问ip对应的地域维度user_web_location_sk */
    new DimensionColumn("dim_web_location",
      List(DimensionJoinCondition(Map("ipKey" -> "web_location_key"))),
      "web_location_sk","user_web_location_sk"),

    /** 获得用户维度user_sk */
    new DimensionColumn("dim_medusa_terminal_user",
      List(DimensionJoinCondition(Map("userId" -> "user_id"))),
      "user_sk"),

    /** 获得设备型号维度product_model_sk */
    new DimensionColumn("dim_medusa_product_model",
      List(DimensionJoinCondition(Map("productModel" -> "product_model"))),
      "product_model_sk"),

   /** 获得推广渠道维度promotion_sk */
    new DimensionColumn("dim_medusa_promotion",
      List(DimensionJoinCondition(Map("promotionChannel" -> "promotion_code"))),
      "promotion_sk"),

    /**获得用户登录维度user_login_sk */
    new DimensionColumn("dim_medusa_terminal_user_login",
      List(DimensionJoinCondition(Map("userId" -> "user_id"))),
      "user_login_sk"),

    /**获得app版本维度app_version_sk */
    new DimensionColumn("dim_app_version",
      List(DimensionJoinCondition(Map("app_series" -> "app_series", "app_version" -> "version"), null, List(("build_time", false)))),
      "app_version_sk"),

    /**获得节目维度program_sk */
    new DimensionColumn("dim_medusa_program",
      List(DimensionJoinCondition(Map("videoSid" -> "sid"))),
      "program_sk"),

    /**获得剧集节目维度episode_program_sk */

    /**获得账号维度account_sk*/
    new DimensionColumn("dim_medusa_account",
      List(DimensionJoinCondition(Map("accountId" -> "account_id"))),
      "account_sk"),

    /**获得音乐精选集维度mv_topic_sk*/
    new DimensionColumn("dim_medusa_mv_topic",
      List(DimensionJoinCondition(Map("omnibusSid" -> "mv_topic_sid"))),
      "mv_topic_sk"),

    /**获得歌手维度singer_sk*/
    new DimensionColumn("dim_medusa_singer",
      List(DimensionJoinCondition(Map("singerSid" -> "singer_id"))),
      "singer_sk"),

    /**获得电台维度mv_radio_sk*/

    /**获得音乐榜单维度mv_hot_sk*/
    new DimensionColumn("dim_medusa_mv_hot_list",
      List(DimensionJoinCondition(Map("topRankSid" -> "mv_hot_id"))),
      "mv_hot_sk")
  )


  /**
    * step 5,保留哪些列，以及别名声明
    * */
  columnsFromSource = List(
    ("subject_name", "subjectName"),
    ("subject_code", "subjectCode"),
    ("main_category", "mainCategory"),
    ("second_category", "secondCategory"),
    ("third_category", "thirdCategory"),
    ("launcher_area_code", "launcherAreaCode"),
    ("launcher_location_code", "launcherLocationCode"),
    ("account_id", "accountId"),
    ("user_id", "userId"),
    ("path_main", "pathMain"),
    ("path", "path"),
    ("pathSpecial", "pathSpecial"),
    ("pathSub", "pathSub"),
    ("filterCategoryFirst", "filterCategoryFirst"),
    ("filterCategorySecond", "filterCategorySecond"),
    ("filterCategoryThird", "filterCategoryThird"),
    ("filterCategoryFourth", "filterCategoryFourth"),
    ("recommendSourceType", "recommendSourceType"),
    ("previousSid", "previousSid"),
    ("previousContentType", "previousContentType"),
    ("ipKey", "ipKey"),
    ("searchFrom", "searchFrom"),
    ("searchKeyword", "searchKeyword"),
    ("pageEntranceAreaCode", "pageEntranceAreaCode"),
    ("pageEntranceLocationCode", "pageEntranceLocationCode"),
    ("pageEntrancePageCode", "pageEntrancePageCode"),
    ("dim_date", "dim_date"),
    ("dim_time", "dim_time")
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
