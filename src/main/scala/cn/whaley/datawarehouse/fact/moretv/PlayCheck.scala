package cn.whaley.datawarehouse.fact.moretv

import java.io.File

import cn.whaley.datawarehouse.BaseClass
import cn.whaley.datawarehouse.fact.constant.Constants._
import cn.whaley.datawarehouse.util.{DataFrameUtil, HdfsUtil, Params}
import org.apache.spark.sql.DataFrame
import cn.whaley.datawarehouse.global.Globals._
/**
  * Created by michael on 2017/5/17.
  */
object PlayCheck extends BaseClass{
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
    val checkListCategoryCase1DF=checkListCategoryCase1(factDFWithIndex)
    checkListCategoryCase1DF.write.parquet(baseOutputPath+File.separator+"checkListCategoryCase1DF")
     //...


    /**写入首页入口维度check的异常数据*/

    //...
    df
  }

  def checkListCategoryCase1(factDFWithIndex:DataFrame):DataFrame={
    /**1.mainCategory,secondCategory维度能够解析出来,但是source_site_sk为null*/
    val sqlStr=s"""select $INDEX_NAME,mainCategory,secondCategory,thirdCategory,pathMain,path
                     |from $fact_table_name
                     |where mainCategory is not null and
                     |secondCategory is not null and
                     |source_site_sk is null
       """.stripMargin
    println(s"checkListCategoryCase1 sql: $sqlStr")
    val df=sqlContext.sql(sqlStr)
    df
  }


  /**不做任何事情，将load操作暂时放到transform部分中，因为需要针对不同维度做check，结果落地 */
  override def load(params: Params, df: DataFrame): Unit = {
  }


}
