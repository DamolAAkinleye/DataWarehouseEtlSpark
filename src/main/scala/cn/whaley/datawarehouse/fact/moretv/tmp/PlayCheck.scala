package cn.whaley.datawarehouse.fact.moretv.tmp

import java.io.File

import cn.whaley.datawarehouse.BaseClass
import cn.whaley.datawarehouse.fact.constant.Constants._
import cn.whaley.datawarehouse.global.Globals._
import cn.whaley.datawarehouse.global.LogConfig
import cn.whaley.datawarehouse.util.{DataFrameUtil, HdfsUtil, Params}
import org.apache.spark.sql.DataFrame

/**
  * Created by michael on 2017/5/17.
  */
object PlayCheck extends BaseClass with LogConfig{
  val partition = 1000
  val topicName = "fact_medusa_play"
  val INDEX_NAME="fact_index"
  val baseOutputPath = FACT_HDFS_BASE_PATH_CHECK+ File.separator + topicName
  val fact_table_name="log_data"
  /**加载待check数据*/
  override def extract(params: Params): DataFrame = {
    params.paramMap.get("date") match {
      case Some(d) => {
        val inputPath=FACT_HDFS_BASE_PATH + File.separator + topicName + File.separator + params.paramMap("date") + File.separator + "00"
        val isExist= HdfsUtil.pathIsExist(inputPath)
        if(isExist){
         sqlContext.read.parquet(inputPath)
        }else{
          throw new RuntimeException(inputPath+"not exist！")
        }
   }
   case None =>
     throw new RuntimeException("未设置时间参数！")
 }
}
  /**获得校验结果*/
  override def transform(params: Params, factDataFrame: DataFrame): DataFrame = {
    var df:DataFrame=null

    /**创建一个无内容的基础文件夹*/
    val isBaseOutputPathExist = HdfsUtil.IsDirExist(baseOutputPath)
    if (isBaseOutputPathExist) {
      val isDelete=HdfsUtil.deleteHDFSFileOrPath(baseOutputPath)
      println(s"删除 $topicName 的基础目录: $baseOutputPath")
    } else {
      println(s"$baseOutputPath 文件夹不存在，创建此文件夹")
      HdfsUtil.createDir(baseOutputPath)
    }

    val factDFWithIndex=DataFrameUtil.dfZipWithIndex(factDataFrame, INDEX_NAME)
    /**--不要注释掉，否则dfZipWithIndex会出现问题，start*/
    factDFWithIndex.cache()
    val count=factDFWithIndex.count()
    println(s"factDFWithIndex count:$count")
    /**--不要注释掉，否则dfZipWithIndex会出现问题，end*/

    /**-----写入不同的check结果数据-----*/
    /**写入factDFWithIndex数据,作为基础数据*/
    factDFWithIndex.write.parquet(baseOutputPath+File.separator+"factDFWithIndex")
    factDFWithIndex.registerTempTable(fact_table_name)

    /**写入列表页维度check的异常数据*/
    checkListCategoryCase1(factDFWithIndex).write.parquet(baseOutputPath + File.separator + "checkListCategoryCase1DF")
    checkListCategoryCase2(factDFWithIndex).write.parquet(baseOutputPath + File.separator + "checkListCategoryCase2DF")

    /**写入专题维度check的异常数据*/
    checkSubjectCase1(factDFWithIndex).write.parquet(baseOutputPath + File.separator + "checkSubjectCase1")
    checkSubjectCase2(factDFWithIndex).write.parquet(baseOutputPath + File.separator + "checkSubjectCase2")

    /**写入筛选维度check的异常数据*/
    checkRetrievalCase(factDFWithIndex).write.parquet(baseOutputPath + File.separator + "checkRetrievalCase")

    /**写入推荐维度check的异常数据*/
    checkRecommendCase1(factDFWithIndex).write.parquet(baseOutputPath + File.separator + "checkRecommendCase1")
    checkRecommendCase2(factDFWithIndex).write.parquet(baseOutputPath + File.separator + "checkRecommendCase2")

    /**写入搜索维度check的异常数据*/
    checkSearchCase(factDFWithIndex).write.parquet(baseOutputPath + File.separator + "checkSearchCase")

    /**写入频道主页维度check的异常数据*/
    checkPageEntranceCase1(factDFWithIndex).write.parquet(baseOutputPath + File.separator + "checkPageEntranceCase1")
    checkPageEntranceCase2(factDFWithIndex).write.parquet(baseOutputPath + File.separator + "checkPageEntranceCase2")

    /**写入首页入口维度check的异常数据*/
    checkEntranceCase1(factDFWithIndex).write.parquet(baseOutputPath + File.separator + "checkEntranceCase1")
    checkEntranceCase2(factDFWithIndex).write.parquet(baseOutputPath + File.separator + "checkEntranceCase2")

    df
  }

  /**1.mainCategory,secondCategory维度能够解析出来,但是source_site_sk为null*/
  def checkListCategoryCase1(factDFWithIndex:DataFrame):DataFrame = {
    val sqlStr=s"""select $INDEX_NAME,mainCategory,secondCategory,pathMain,path
                     |from $fact_table_name
                     |where mainCategory is not null and
                     |secondCategory is not null and
                     |thirdCategory is null and
                     |source_site_sk is null
       """.stripMargin
    checkBaseCase(factDFWithIndex, sqlStr, "checkListCategoryCase1")
  }

  /** mainCategory,secondCategory and thirdCategory is not null, but source_site_sk is null */
  def checkListCategoryCase2(factDFWithIndex:DataFrame):DataFrame = {
    val sqlStr =
      s"""
         |select $INDEX_NAME, mainCategory, secondCategory, thirdCategory, pathMain, path
         |from $fact_table_name
         |where mainCategory is not null and
         |secondCategory is not null and
         |thirdCategory is not null and
         |source_site_sk is null
      """.stripMargin
    checkBaseCase(factDFWithIndex, sqlStr, "checkListCategoryCase2")
  }

  /** subjectCode is not null, but subject_sk is null */
  def checkSubjectCase1(factDFWithIndex:DataFrame):DataFrame = {
    val sqlStr =
      s"""
        |select $INDEX_NAME, subjectCode ,pathSpecial, path
        |from $fact_table_name
        |where subjectCode is not null and
        |subject_sk is null
      """.stripMargin
    checkBaseCase(factDFWithIndex, sqlStr, "checkSubjectCase1")
  }

  /** subjectName is not null, but subject_sk is null */
  def checkSubjectCase2(factDFWithIndex:DataFrame):DataFrame = {
    val sqlStr =
      s"""
         |select $INDEX_NAME, subjectName ,pathSpecial, path
         |from $fact_table_name
         |where subjectName is not null and
         |subject_sk is null
      """.stripMargin
    checkBaseCase(factDFWithIndex, sqlStr, "checkSubjectCase2")
  }

  /** filterContentType, filterCategoryFirst, filterCategorySecond, filterCategoryThird and filterCategoryFourth is not null, but retrieval_sk is null */
  def checkRetrievalCase(factDFWithIndex:DataFrame):DataFrame = {
    val sqlStr =
      s"""
        |select $INDEX_NAME, filterContentType, filterCategoryFirst, filterCategorySecond, filterCategoryThird, filterCategoryFourth, pathMain, path
        |from $fact_table_name
        |where filterContentType is not null and
        |filterCategoryFirst is not null and
        |filterCategorySecond is not null and
        |filterCategoryThird is not null and
        |filterCategoryFourth is not null and
        |retrieval_sk is null
      """.stripMargin
    checkBaseCase(factDFWithIndex, sqlStr, "checkRetrievalCase")
  }

  /** recommendLogType is home, recommendSlotIndex is not null, but recommend_position_sk is null */
  def checkRecommendCase1(factDFWithIndex:DataFrame):DataFrame = {
    val sqlStr =
      s"""
        |select $INDEX_NAME, recommendSlotIndex, pathMain, path
        |from $fact_table_name
        |where recommendLogType = 'home' and
        |recommendSlotIndex is not null and
        |recommend_position_sk is null
      """.stripMargin
    checkBaseCase(factDFWithIndex, sqlStr, "checkRecommendCase1")
  }

  /** recommendLogType is other, recommendSourceType and previousContentType is not null, but recommend_position_sk is null */
  def checkRecommendCase2(factDFWithIndex:DataFrame):DataFrame = {
    val sqlStr =
      s"""
         |select $INDEX_NAME, recommendSourceType, previousContentType, pathSub, path
         |from $fact_table_name
         |where recommendLogType = 'other' and
         |recommendSourceType is not null and
         |previousContentType is not null and
         |recommend_position_sk is null
      """.stripMargin
    checkBaseCase(factDFWithIndex, sqlStr, "checkRecommendCase2")
  }

  /** searchFrom, resultIndex, tabName and searchFromHotWord is not null, but search_sk is null */
  def checkSearchCase(factDFWithIndex:DataFrame):DataFrame = {
    val sqlStr =
      s"""
        |select $INDEX_NAME, searchFrom, resultIndex, tabName, searchFromHotWord, pathMain, path
        |from $fact_table_name
        |where searchFrom is not null and
        |resultIndex is not null and
        |tabName is not null and
        |searchFromHotWord is not null and
        |search_sk is null
      """.stripMargin
    checkBaseCase(factDFWithIndex, sqlStr, "checkSearchCase")
  }

  /** pageEntrancePageCode, pageEntranceAreaCode and pageEntranceLocationCode is not null, but page_entrance_sk is null */
  def checkPageEntranceCase1(factDFWithIndex:DataFrame):DataFrame = {
    val sqlStr =
      s"""
        |select $INDEX_NAME, pageEntrancePageCode, pageEntranceAreaCode, pageEntranceLocationCode, pathMain, path
        |from $fact_table_name
        |where pageEntrancePageCode is not null and
        |pageEntranceAreaCode is not null and
        |pageEntranceLocationCode is not null and
        |page_entrance_sk is null
      """.stripMargin
    checkBaseCase(factDFWithIndex, sqlStr, "checkPageEntranceCase1")
  }

  /** pageEntrancePageCode and pageEntranceAreaCode is not null, pageEntranceLocationCode is null, page_entrance_sk is null */
  def checkPageEntranceCase2(factDFWithIndex:DataFrame):DataFrame = {
    val sqlStr =
      s"""
         |select $INDEX_NAME, pageEntrancePageCode, pageEntranceAreaCode, pathMain, path
         |from $fact_table_name
         |where pageEntrancePageCode is not null and
         |pageEntranceAreaCode is not null and
         |pageEntranceLocationCode is null and
         |page_entrance_sk is null
      """.stripMargin
    checkBaseCase(factDFWithIndex, sqlStr, "checkPageEntranceCase2")
  }

  /** launcherAreaCode and launcherLocationCode is not null, but launcher_entrance_sk is null */
  def checkEntranceCase1(factDFWithIndex:DataFrame):DataFrame = {
    val sqlStr =
      s"""
        |select $INDEX_NAME, launcherAreaCode, launcherLocationCode, pathMain, path
        |from $fact_table_name
        |where launcherAreaCode is not null and
        |launcherLocationCode is not null and
        |launcher_entrance_sk is null
      """.stripMargin
    checkBaseCase(factDFWithIndex, sqlStr, "checkEntranceCase1")
  }

  /** launcherAreaCode is not null, launcherLocationCode is null, launcher_entrance_sk is null */
  def checkEntranceCase2(factDFWithIndex:DataFrame):DataFrame = {
    val sqlStr =
      s"""
         |select $INDEX_NAME, launcherAreaCode, pathMain, path
         |from $fact_table_name
         |where launcherAreaCode is not null and
         |launcherLocationCode is null and
         |launcher_entrance_sk is null
      """.stripMargin
    checkBaseCase(factDFWithIndex, sqlStr, "checkEntranceCase2")
  }

  def checkBaseCase(factDFWithIndex: DataFrame, sqlStr: String, checkName: String):DataFrame = {
    println(s"$checkName sql: $sqlStr")
    sqlContext.sql(sqlStr)
  }

  /**不做任何事情，将load操作暂时放到transform部分中，因为需要针对不同维度做check，结果落地 */
  override def load(params: Params, df: DataFrame): Unit = {
  }


}
