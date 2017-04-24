package cn.whaley.datawarehouse.fact.moretv

import cn.whaley.datawarehouse.fact.util._
import cn.whaley.datawarehouse.fact.{FactEtlBase}
import cn.whaley.datawarehouse.fact.common.{UserDefinedColumn}
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
    * step 2, generate new columns
    * */
  addColumns = List(
    UserDefinedColumn("subjectCode", udf(SubjectUtils.getSubjectCodeByPathETL: (String, String,String) => String), List("pathMain", "path", "flag")),
    UserDefinedColumn("subjectName", udf(SubjectUtils.getSubjectNameByPathETL: (String) => String), List("pathSpecial")),
    UserDefinedColumn("entryType", udf(EntranceTypeUtils.getEntranceTypeByPathETL: (String, String,String) => String), List("pathMain", "path", "flag")),
    UserDefinedColumn("mainCategory", udf(ListCategoryUtils.getListMainCategory: (String,String,String) => String), List("pathMain", "path", "flag")),
    UserDefinedColumn("secondCategory",udf(ListCategoryUtils.getListSecondCategory: (String,String,String) => String), List("pathMain", "path", "flag")),
    UserDefinedColumn("thirdCategory", udf(ListCategoryUtils.getListThirdCategory: (String,String,String) => String), List("pathMain", "path", "flag"))
  )

  /**
    * step 3, left join dimension table,get new column
    * */
  dimensionColumns = List(
    /** list category join*/
    KidsPathParserUtils.kidsListCategoryDimension,
    SportsPathParserUtils.sportListCategoryDimension,
    MvPathParseUtils.mvListCategoryDimension,
    CommonPathParseUtils.commonListCategoryDimension,
    /** 获得专题 subject_sk */
    SubjectUtils.getSubjectSKBySubjectCodeOrSubjectName


  /**  频道首页入口 maybe left join or udf*/

    /**  首页入口 maybe left join or udf*/

    /* example
    new DimensionColumn("dim_medusa_promotion",
      List(DimensionJoinCondition(Map("promotionChannel" -> "promotion_code"))), "promotion_sk"),
    new DimensionColumn("dim_app_version",
      List(
        DimensionJoinCondition(
          Map("app_series" -> "app_series", "app_version" -> "version"),
          null,
          null," main_category is null "
        )
      ),
      "app_version_sk")*/
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
    ("entry_type", "entryType"),
    ("account_id", "accountId"),
    ("user_id", "userId"),
    ("path_main", "pathMain"),
    ("path", "path")
  )




}
