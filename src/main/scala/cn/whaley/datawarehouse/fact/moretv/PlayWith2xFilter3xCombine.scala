package cn.whaley.datawarehouse.fact.moretv

import java.util.Calendar

import cn.whaley.datawarehouse.common.{DimensionColumn, DimensionJoinCondition, UserDefinedColumn}
import cn.whaley.datawarehouse.fact.FactEtlBase
import cn.whaley.datawarehouse.fact.moretv.util._
import cn.whaley.datawarehouse.global.{LogConfig, LogTypes}
import cn.whaley.datawarehouse.util._
import cn.whaley.sdk.dataexchangeio.DataIO
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


/**
  * Created by michel on 17/4/24.
  */
object PlayWith2xFilter3xCombine extends FactEtlBase with  LogConfig{
  /** log type name */
  topicName = "fact_medusa_play"
  partition = 2000


  /**
    * step 1, get data source
    * */
  override def readSource(startDate: String): DataFrame = {
    println("------- before readSource "+Calendar.getInstance().getTime)
    val medusa_input_dir = DataIO.getDataFrameOps.getPath(MERGER, LogTypes.MEDUSA_PLAY_3X_COMBINE_RESULT, startDate)
    val moretv_input_dir = DataIO.getDataFrameOps.getPath(MERGER, LogTypes.MEDUSA_PLAY_2X_FILTER_RESULT, startDate)
    val medusaFlag = HdfsUtil.IsInputGenerateSuccess(medusa_input_dir)
    val moretvFlag = HdfsUtil.IsInputGenerateSuccess(moretv_input_dir)
    if (medusaFlag && moretvFlag) {
      val medusaDf = DataIO.getDataFrameOps.getDF(sqlContext, Map[String,String](), MERGER, LogTypes.MEDUSA_PLAY_3X_COMBINE_RESULT, startDate).withColumn("flag",lit(MEDUSA))
      val moretvDf = DataIO.getDataFrameOps.getDF(sqlContext, Map[String,String](), MERGER, LogTypes.MEDUSA_PLAY_2X_FILTER_RESULT, startDate).withColumn("flag",lit(MORETV))
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
    println("------- before filterRows "+Calendar.getInstance().getTime)
    /** 用于过滤单个用户播放单个视频量过大的情况 */
    val playNumLimit=5000
    println("sourceDf.count():"+sourceDf.count())
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
    println("------- after filterRows "+Calendar.getInstance().getTime)
    println("filterRows resultDF.count():"+resultDF.count())
    resultDF
  }

  /**
    * step 3, generate new columns
    * */
  addColumns = List(
    UserDefinedColumn("ipKey", udf(getIpKey: String => Long), List("realIP")),
    UserDefinedColumn("dim_date", udf(getDimDate: String => String), List("fDatetime")),
    UserDefinedColumn("dim_time", udf(getDimTime: String => String), List("fDatetime")),
    UserDefinedColumn("app_series", udf(getAppSeries: String => String), List("version")),
    UserDefinedColumn("app_version", udf(getAppVersion: String => String), List("version")),
    UserDefinedColumn("subjectCode", udf(SubjectUtils.getSubjectCodeByPathETL: (String, String,String) => String), List("pathSpecial", "path", "flag")),
    UserDefinedColumn("subjectName", udf(SubjectUtils.getSubjectNameByPathETL: (String) => String), List("pathSpecial")),
    UserDefinedColumn("mainCategory", udf(ListCategoryUtils.getListMainCategory: (String,String,String) => String), List("pathMain", "path", "flag")),
    UserDefinedColumn("secondCategory",udf(ListCategoryUtils.getListSecondCategory: (String,String,String) => String), List("pathMain", "path", "flag")),
    UserDefinedColumn("thirdCategory", udf(ListCategoryUtils.getListThirdCategory: (String,String,String) => String), List("pathMain", "path", "flag")),
    UserDefinedColumn("launcherAreaCode", udf(EntranceTypeUtils.getEntranceAreaCode: (String, String,String) => String), List("pathMain", "path", "flag")),
    UserDefinedColumn("launcherLocationCode", udf(EntranceTypeUtils.getEntranceLocationCode: (String, String,String) => String), List("pathMain", "path", "flag")),
    UserDefinedColumn("filterContentType", udf(FilterCategoryUtils.getFilterCategoryContentType: (String,String,String) => String), List("pathMain", "path", "flag")),
    UserDefinedColumn("filterCategoryFirst", udf(FilterCategoryUtils.getFilterCategoryFirst: (String,String,String) => String), List("pathMain", "path", "flag")),
    UserDefinedColumn("filterCategorySecond", udf(FilterCategoryUtils.getFilterCategorySecond: (String,String,String) => String), List("pathMain", "path", "flag")),
    UserDefinedColumn("filterCategoryThird", udf(FilterCategoryUtils.getFilterCategoryThird: (String,String,String) => String), List("pathMain", "path", "flag")),
    UserDefinedColumn("filterCategoryFourth", udf(FilterCategoryUtils.getFilterCategoryFourth: (String,String,String) => String), List("pathMain", "path", "flag")),
    UserDefinedColumn("recommendSourceType", udf(RecommendUtils.getRecommendSourceType: (String,String,String) => String), List("pathSub", "path", "flag")),
    UserDefinedColumn("recommendLogType", udf(RecommendUtils.getRecommendLogType: (String,String,String) => String), List("pathSub", "path", "flag")),
    UserDefinedColumn("previousSid", udf(RecommendUtils.getPreviousSid: (String) => String), List("pathSub")),
    UserDefinedColumn("previousContentType", udf(RecommendUtils.getPreviousContentType: (String) => String), List("pathSub")),
    UserDefinedColumn("recommendSlotIndex", udf(RecommendUtils.getRecommendSlotIndex: (String) => String), List("pathMain")),
    UserDefinedColumn("searchFrom", udf(SearchUtils.getSearchFrom: (String,String,String) => String),List("pathMain", "path", "flag")),
    UserDefinedColumn("searchKeyword", udf(SearchUtils.getSearchKeyword: (String,String,String) => String),List("pathMain", "path", "flag")),
    UserDefinedColumn("searchFromHotWord", udf(SearchUtils.isSearchFromHotWord: String => Int),List("extraPath")),
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
    FilterCategoryUtils.getRetrievalSK(),

    /** 获得推荐来源sk */
    RecommendUtils.getRecommendPositionSK(),

    /** 获得搜索来源sk */
    SearchUtils.getSearchSK(),

    /** 获得频道主页来源维度sk（只有少儿，音乐，体育有频道主页来源维度）*/
    PageEntrancePathParseUtils.getPageEntranceSK(),

    /** 获得首页入口 launcher_entrance_sk */
    EntranceTypeUtils.getLauncherEntranceSK(),


    /** 获得访问ip对应的地域维度user_web_location_sk */
    new DimensionColumn("dim_web_location",
      List(DimensionJoinCondition(Map("ipKey" -> "web_location_key"))),
      "web_location_sk","access_web_location_sk"),

    /** 获得用户维度user_sk */
    new DimensionColumn("dim_medusa_terminal_user",
      List(DimensionJoinCondition(Map("userId" -> "user_id"))),
      List(("user_sk","user_sk"), ("web_location_sk", "user_web_location_sk"))),

    /** 获得用户登录维度user_login_sk */
    new DimensionColumn("dim_medusa_terminal_user_login",
      List(DimensionJoinCondition(Map("userId" -> "user_id"))),
      "user_login_sk"),

    /** 获得设备型号维度product_model_sk */
    new DimensionColumn("dim_medusa_product_model",
      List(DimensionJoinCondition(Map("productModel" -> "product_model"))),
      "product_model_sk"),

   /** 获得推广渠道维度promotion_sk */
    new DimensionColumn("dim_medusa_promotion",
      List(DimensionJoinCondition(Map("promotionChannel" -> "promotion_code"))),
      "promotion_sk"),

    /** 获得app版本维度app_version_sk */
    new DimensionColumn("dim_app_version",
      List(DimensionJoinCondition(
        Map("app_series" -> "app_series", "app_version" -> "version"))
      ),
      "app_version_sk"),

    /** 获得节目维度program_sk */
    new DimensionColumn("dim_medusa_program",
      List(DimensionJoinCondition(Map("videoSid" -> "sid"))),
      "program_sk"),

    /** 获得剧集节目维度episode_program_sk*/
    new DimensionColumn("dim_medusa_program", "dim_medusa_program_episode",
      List(DimensionJoinCondition(Map("episodeSid" -> "sid"))),
      "program_sk","episode_program_sk"),

    /** 获得账号维度account_sk*/
    new DimensionColumn("dim_medusa_account",
      List(DimensionJoinCondition(Map("accountId" -> "account_id"))),
      "account_sk"),

    /** 获得音乐精选集维度mv_topic_sk*/
    new DimensionColumn("dim_medusa_mv_topic",
      List(DimensionJoinCondition(Map("omnibusSid" -> "mv_topic_sid"))),
      "mv_topic_sk"),

    /** 获得歌手维度singer_sk*/
    new DimensionColumn("dim_medusa_singer",
      List(DimensionJoinCondition(Map("singerSid" -> "singer_id"))),
      "singer_sk"),

    /** 获得电台维度mv_radio_sk*/
    new DimensionColumn("dim_medusa_mv_radio",
      List(DimensionJoinCondition(Map("station" -> "mv_radio_title"))),
      "mv_radio_sk"),

    /** 获得音乐榜单维度mv_hot_sk */
    new DimensionColumn("dim_medusa_mv_hot_list",
      List(DimensionJoinCondition(Map("topRankSid" -> "mv_hot_rank_id"))),
      "mv_hot_sk")
  )


  /**
    * step 5,保留哪些列，以及别名声明
    * */

  dimensionsNeedInFact = List("dim_medusa_subject", "dim_medusa_program")

  columnsFromSource = List(
    //作为测试字段,验证维度解析是否正确，上线后删除
    ("subjectName", "subjectName"),
    ("subjectCode", "subjectCode"),
    ("mainCategory", "mainCategory"),
    ("secondCategory", "secondCategory"),
    ("thirdCategory", "thirdCategory"),
    ("launcherAreaCode", "launcherAreaCode"),
    ("launcherLocationCode", "launcherLocationCode"),
    ("filterContentType", "filterContentType"),
    ("filterCategoryFirst", "filterCategoryFirst"),
    ("filterCategorySecond", "filterCategorySecond"),
    ("filterCategoryThird", "filterCategoryThird"),
    ("filterCategoryFourth", "filterCategoryFourth"),
    ("recommendSourceType", "recommendSourceType"),
    ("previousSid", "previousSid"),
    ("previousContentType", "previousContentType"),
    ("recommendSlotIndex", "recommendSlotIndex"),
    ("recommendType", "recommendType"),
    ("recommendLogType", "recommendLogType"),
    ("pageEntranceAreaCode", "pageEntranceAreaCode"),
    ("pageEntranceLocationCode", "pageEntranceLocationCode"),
    ("pageEntrancePageCode", "pageEntrancePageCode"),
    ("ipKey", "ipKey"),
    ("account_id", "accountId"),
    ("pathMain", "pathMain"),
    ("path", "path"),
    ("pathSpecial", "pathSpecial"),
    ("pathSub", "pathSub"),
    ("searchFrom", "searchFrom"),
    ("resultIndex", "resultIndex"),
    ("tabName", "tabName"),
    ("searchFromHotWord", "searchFromHotWord"),


//--------在fact_medusa_play表中展示的字段---------
    ("duration", "FDuration"),
    ("program_duration", "programDuration"),//programDuration
    //("mid_post_duration", ""),//for now,not online filed
    ("user_id", "userId"),
    ("mac", "mac"),
    ("end_event", "end_event"),//no end_event,need to merge play
    //("start_time", ""),//for now,not online filed
    //("end_time", ""),//for now,not online filed
    ("program_sid", "videoSid"),
    ("content_type", "contentType"),
    ("play_content_type",
      "case when dim_medusa_subject.subject_content_type is not null then dim_medusa_subject.subject_content_type " +
        "when dim_medusa_program.content_type is not null then dim_medusa_program.content_type " +
        "when trim(contentType) = '' then null else contentType end"),
    ("is_reservation", "case when trim(contentType) = 'reservation' then 'true' else 'false' end"),
    ("search_keyword", "searchKeyword"),
    ("product_model", "productModel"),
    ("auto_clarity", "tencentAutoClarity"),
    ("contain_ad", "containAd"),
    ("app_enter_way", "appEnterWay"),
    //("session_id", "sessionId"),//for now,not online filed
    //("device_id", "deviceId"),//for now,not online filed
    //("display_id", "displayId"),//for now,not online filed
    //("player_type", "playerType"),//for now,not online filed
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
