package cn.whaley.datawarehouse.fact.moretv

import java.io.File

import cn.whaley.datawarehouse.BaseClass
import cn.whaley.datawarehouse.fact.constant.Constants._
import cn.whaley.datawarehouse.global.{LogConfig, LogTypes}
import cn.whaley.datawarehouse.util.{DataFrameUtil, HdfsUtil, Params}
import cn.whaley.sdk.dataexchangeio.DataIO
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by michael on 2017/5/19.
  *
  * 业务作用：合并medusa 3.x play对一次播放开始和一次播放结束日志为一条记录
  *
  * 基本逻辑：
  * 使用userId,episodeSid,videoSid,pathMain,realIP 作为play session unique key
  * 对相同session unique key进行下面三个字段的判别，剩下的其他字段对于一个play session都一样
  * datetime
  * duration
  * end_event
  *  规则：
  *  1.如果有开始行为又有结束行为，datetime使用开始行为的datetime
  *  2.如果单个startplay自己合成一条记录,end_event类型为noEnd
  */
object Play3xCombine extends BaseClass with LogConfig {
/*  val topicName = "play3xCombineResultV3"
  val baseOutputPath = FACT_HDFS_BASE_PATH_CHECK + File.separator + topicName
  val topicNameCombineTmp = "combineTmpV3"
  val baseOutputPathCombineTmp = FACT_HDFS_BASE_PATH_CHECK + File.separator + topicNameCombineTmp
  val short = "shortV3"
  val baseOutputPathShort = FACT_HDFS_BASE_PATH_CHECK + File.separator + short
  val fact = "factV3"
  val baseOutputPathFact = FACT_HDFS_BASE_PATH_CHECK + File.separator + fact*/

  val fact_table_name = "log_data"
  val INDEX_NAME = "r_index"
  val startPlayEvent = "startplay"
  val userExitEvent = "userexit"
  val selfEndEvent = "selfend"
  val noEnd = "noEnd"
  val shortDataFrameTable = "shortDataFrameTable"
  val combineTmpTable = "combineTmpTable"

  /** 加载3.x play数据 */
  override def extract(params: Params): DataFrame = {
    params.paramMap.get("date") match {
      case Some(d) => {
        val startDate = d.toString
        val medusa_input_dir = DataIO.getDataFrameOps.getPath(MEDUSA, LogTypes.PLAY, startDate)
        val medusaFlag = HdfsUtil.IsInputGenerateSuccess(medusa_input_dir)
        if (medusaFlag) {
          val medusaDf = DataIO.getDataFrameOps.getDF(sqlContext, Map[String, String](), MEDUSA, LogTypes.PLAY, startDate)
          println("medusaDf.count():" + medusaDf.count())
          medusaDf.repartition(1000)
        } else {
          throw new RuntimeException(s" $medusa_input_dir not exist")
        }
      }
      case None =>
        throw new RuntimeException("未设置时间参数！")
    }
  }

  /** 合并操作,没有比较合并记录中，startplay的datetime必须大于等于结束事件的datetime,过时版本
    * */
  override def transform(params: Params, factDataFrame: DataFrame): DataFrame = {
    //进行索引编号
    val factDataFrameWithIndex = DataFrameUtil.dfZipWithIndex(factDataFrame, INDEX_NAME)
    factDataFrameWithIndex.cache()
    println("factDataFrameWithIndex.count():" + factDataFrameWithIndex.count())
    factDataFrameWithIndex.registerTempTable(fact_table_name)
    //writeToHDFS(factDataFrameWithIndex, baseOutputPathFact)

    //获得key
    val sqlString =
      s"""select concat_ws('_',userId,episodeSid,videoSid,pathMain,realIP) as key,duration,datetime,event,${INDEX_NAME}
          |from $fact_table_name
       """.stripMargin
    val shortDataFrame = sqlContext.sql(sqlString)
    //writeToHDFS(shortDataFrame, baseOutputPathShort)
    shortDataFrame.registerTempTable(shortDataFrameTable)
    //(key,list(duration,datetime,event,r_index))
    val rdd1 = shortDataFrame.map(row => (row.getString(0), (row.getLong(1), row.getString(2), row.getString(3), row.getLong(4)))).groupByKey()
    val rddCombineTmp = rdd1.map(x => {
      val ikey = x._1
      val tupleIterable = x._2
      val startList=tupleIterable.toList.filter(_._3==startPlayEvent).sortBy(_._2)
      val endList=tupleIterable.toList.filter(_._3!=null).filter(_._3!=startPlayEvent).sortBy(_._2)
      val startLength=startList.length
      val endLength=endList.length
      val arrayBuffer = ArrayBuffer.empty[Row]

      if(startLength<=endLength){
        var i=0
        while(i<endLength){
          if(i<startLength){
             val row = Row(ikey, endList(i)._1, startList(i)._2, endList(i)._3, endList(i)._4)
            arrayBuffer.+=(row)
          }else{
            val row = Row(ikey, endList(i)._1, endList(i)._2, endList(i)._3, endList(i)._4)
            arrayBuffer.+=(row)
          }
          i=i+1
        }
      }else if(startLength>endLength){
        var i=0
        while(i<startLength){
          if(i<endLength){
            val row = Row(ikey, endList(i)._1, startList(i)._2, endList(i)._3, endList(i)._4)
            arrayBuffer.+=(row)
          }else{
            val row = Row(ikey, startList(i)._1, startList(i)._2, noEnd, startList(i)._4)
            arrayBuffer.+=(row)
          }
          i=i+1
        }
      }
      arrayBuffer.toList
    }).flatMap(x => x)

    println("shortDataFrame.schema.fields:" + shortDataFrame.schema.fields.foreach(e => println(e.name)))
    val combineDFTmp = sqlContext.createDataFrame(rddCombineTmp, StructType(shortDataFrame.schema.fields))
    combineDFTmp.registerTempTable(combineTmpTable)
    //writeToHDFS(combineDFTmp, baseOutputPathCombineTmp)
    val df = sqlContext.sql(
      s"""select a.*,b.event as end_event,b.datetime as fDatetime,b.duration as fDuration
          | from   $fact_table_name         a join
          |        $combineTmpTable         b on a.${INDEX_NAME}=b.${INDEX_NAME}
      """.stripMargin)
    df
  }

  override def load(params: Params, df: DataFrame): Unit = {
    val date = params.paramMap("date").toString
    val baseOutputPath= DataIO.getDataFrameOps.getPath(MERGER,LogTypes.MEDUSA_PLAY_3X_COMBINE_RESULT,date)
    val isBaseOutputPathExist = HdfsUtil.IsDirExist(baseOutputPath)
    if (isBaseOutputPathExist) {
      HdfsUtil.deleteHDFSFileOrPath(baseOutputPath)
      println(s"删除目录: $baseOutputPath")
    }
    println("合并后记录条数:" + df.count())
    println("合并后结果输出目录为：" + baseOutputPath)
    df.write.parquet(baseOutputPath)
  }

  def writeToHDFS(df: DataFrame, path: String): Unit = {
    println(s"write df to $path")
    val isBaseOutputPathExist = HdfsUtil.IsDirExist(path)
    if (isBaseOutputPathExist) {
      HdfsUtil.deleteHDFSFileOrPath(path)
      println(s"删除目录: $path")
    }
    println("记录条数:" + df.count())
    println("输出目录为：" + path)
    df.write.parquet(path)
  }

}
