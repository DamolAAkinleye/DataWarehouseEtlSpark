package cn.whaley.datawarehouse.fact.moretv

import cn.whaley.datawarehouse.common.UserDefinedColumn
import cn.whaley.datawarehouse.fact.{FactEtlBase, util}
import cn.whaley.datawarehouse.fact.util._
import cn.whaley.datawarehouse.global.{LogConfig, LogTypes}
import cn.whaley.datawarehouse.util._
import cn.whaley.sdk.dataexchangeio.DataIO
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


/**
  * Created by wu.jiulin on 17/4/27.
  */
object PlayTemp extends FactEtlBase with  LogConfig{

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
    * step 2, generate new columns
    * */
  addColumns = List(
    UserDefinedColumn("subjectCode", udf(SubjectUtils.getSubjectCodeByPathETL: (String, String,String) => String), List("pathSpecial", "path", "flag")),
    UserDefinedColumn("subjectName", udf(SubjectUtils.getSubjectNameByPathETL: (String) => String), List("pathSpecial")),
    UserDefinedColumn("launcherAreaCode", udf(EntranceTypeUtils.getEntranceAreaCode: (String, String,String) => String), List("pathMain", "path", "flag")),
    UserDefinedColumn("launcherLocationCode", udf(EntranceTypeUtils.getEntranceLocationCode: (String, String,String) => String), List("pathMain", "path", "flag")),
    UserDefinedColumn("pageEntranceAreaCode", udf(PageEntrancePathParseUtils.getPageEntranceAreaCode: (String, String,String) => String), List("pathMain", "path", "flag")),
    UserDefinedColumn("pageEntranceLocationCode", udf(PageEntrancePathParseUtils.getPageEntranceLocationCode: (String, String,String) => String), List("pathMain", "path", "flag")),
    UserDefinedColumn("pageEntrancePageCode", udf(PageEntrancePathParseUtils.getPageEntrancePageCode: (String, String,String) => String), List("pathMain", "path", "flag")),
    UserDefinedColumn("mainCategory", udf(ListCategoryUtils.getListMainCategory: (String,String,String) => String), List("pathMain", "path", "flag")),
    UserDefinedColumn("secondCategory",udf(ListCategoryUtils.getListSecondCategory: (String,String,String) => String), List("pathMain", "path", "flag")),
    UserDefinedColumn("thirdCategory", udf(ListCategoryUtils.getListThirdCategory: (String,String,String) => String), List("pathMain", "path", "flag")),
    UserDefinedColumn("ipKey", udf(getIpKey: String => Long), List("realIP"))
  )

  /**
    * step 3, left join dimension table,get new column
    * */
  dimensionColumns = List(
    /** 获得列表页sk source_site_sk*/
    ListCategoryUtils.getSourceSiteSK,
    /** 获得专题 subject_sk */
    SubjectUtils.getSubjectSK,
    /** 获得首页入口 launcher_entrance_sk */
    EntranceTypeUtils.getLauncherEntranceSK,
    /** 获得频道主页来源 page_entrance_sk */
    PageEntrancePathParseUtils.getPageEntranceSK
  )


  /**
    * step 4,保留哪些列，以及别名声明
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
    ("area_code", "pageEntranceAreaCode"),
    ("location_code", "pageEntranceLocationCode"),
    ("page_code", "pageEntrancePageCode")
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
