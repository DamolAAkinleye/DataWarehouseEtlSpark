package cn.whaley.datawarehouse.fact.moretv

import cn.whaley.datawarehouse.common.UserDefinedColumn
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
    UserDefinedColumn("subjectCode", udf(SubjectUtils.getSubjectCodeByPathETL: (String, String,String) => String), List("pathSpecial", "path", "flag")),
    UserDefinedColumn("subjectName", udf(SubjectUtils.getSubjectNameByPathETL: (String) => String), List("pathSpecial")),
    UserDefinedColumn("mainCategory", udf(ListCategoryUtils.getListMainCategory: (String,String,String) => String), List("pathMain", "path", "flag")),
    UserDefinedColumn("secondCategory",udf(ListCategoryUtils.getListSecondCategory: (String,String,String) => String), List("pathMain", "path", "flag")),
    UserDefinedColumn("thirdCategory", udf(ListCategoryUtils.getListThirdCategory: (String,String,String) => String), List("pathMain", "path", "flag")),
    UserDefinedColumn("ipKey", udf(getIpKey: String => Long), List("realIP")),
    UserDefinedColumn("launcherAreaCode", udf(EntranceTypeUtils.getEntranceAreaCode: (String, String,String) => String), List("pathMain", "path", "flag")),
    UserDefinedColumn("launcherLocationCode", udf(EntranceTypeUtils.getEntranceLocationCode: (String, String,String) => String), List("pathMain", "path", "flag")),
    UserDefinedColumn("filterCategoryFirst", udf(FilterCategoryUtils.getFilterCategoryFirst: (String,String,String) => String), List("pathMain", "path", "flag")),
    UserDefinedColumn("filterCategorySecond", udf(FilterCategoryUtils.getFilterCategorySecond: (String,String,String) => String), List("pathMain", "path", "flag")),
    UserDefinedColumn("filterCategoryThird", udf(FilterCategoryUtils.getFilterCategoryThird: (String,String,String) => String), List("pathMain", "path", "flag")),
    UserDefinedColumn("filterCategoryFourth", udf(FilterCategoryUtils.getFilterCategoryFourth: (String,String,String) => String), List("pathMain", "path", "flag")),
    UserDefinedColumn("recommendSourceType", udf(RecommendUtils.getRecommendSourceType: (String,String,String) => String), List("pathSub", "path", "flag")),
    UserDefinedColumn("previousSid", udf(RecommendUtils.getPreviousSid: (String) => String), List("pathSub")),
    UserDefinedColumn("previousContentType", udf(RecommendUtils.getPreviousContentType: (String) => String), List("pathSub")),
    UserDefinedColumn("searchFrom", udf(SearchUtils.getSearchFrom: (String,String,String) => String),List("pathMain", "path", "flag")),
    UserDefinedColumn("searchKeyword", udf(SearchUtils.getSearchKeyword: (String,String,String) => String),List("pathMain", "path", "flag"))
  )

  /**
    * step 4, left join dimension table,get sk
    * */
  dimensionColumns = List(
    /** 获得列表页sk source_site_sk */
    ListCategoryUtils.getSourceSiteSK,

    /** 获得专题 subject_sk */
    SubjectUtils.getSubjectSK,

    /** 获得筛选sk */

    /** 获得推荐来源sk */

    /** 获得频道主页来源维度sk（只有少儿，音乐，体育有频道主页来源维度）*/


    /** 获得首页入口 launcher_entrance_sk */
    EntranceTypeUtils.getLauncherEntranceSK
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
    ("filterCategoryFirst", "filterCategoryFirst"),
    ("filterCategorySecond", "filterCategorySecond"),
    ("filterCategoryThird", "filterCategoryThird"),
    ("filterCategoryFourth", "filterCategoryFourth"),
    ("recommendSourceType", "recommendSourceType"),
    ("previousSid", "previousSid"),
    ("previousContentType", "previousContentType"),
    ("ipKey", "ipKey"),
    ("searchFrom", "searchFrom"),
    ("searchKeyword", "searchKeyword")
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

}
