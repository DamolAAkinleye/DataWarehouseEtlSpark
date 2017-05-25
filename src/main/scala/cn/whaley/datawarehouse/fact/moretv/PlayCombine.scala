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
import scala.util.Sorting

/**
  * Created by michael on 2017/5/19.
  * CREATE EXTERNAL TABLE fact_medusa_play(
  * duration                   INT        COMMENT '播放时长，单位秒',
  * user_id                    STRING     COMMENT '用户id',
  * end_event                  STRING     COMMENT '结束方式, 包括userexit、selfend',
  * start_time                 TIMESTAMP  COMMENT '播放开始时间',
  * end_time                   TIMESTAMP  COMMENT '播放结束时间'，
  * dim_date                   STRING     COMMENT '日期维度',
  * dim_time                   STRING     COMMENT '时间维度'
  * )
  * 其他信息：1.短视频，电影没有剧集id【episodeSid】
  * 2.后期会上线sessionId
  *
  *
  * 如果有playId，用playId关联，
  * 没有playId的话，使用userId,episodeSid,videoSid,pathMain,realIP 作为play session unique key
  * 待确定：
  * 【假设下面行为userId,episodeSid,videoSid,pathMain,realIP ，都相同】
  * 1.如果只有开始行为，end_event默认是null            ok
  * 2.如果只有开始行为，duration默认是0                ok
  * 3.如果有开始行为又有结束行为，datetime使用开始行为的datetime      ok
  * 4.如果有开始行为，有结束行为，但是开始行为和结束行为之间的时间差大于三个小时，是分开作为两条日志，还是合并成一条日志，先做分析？
  * 6.例如load入20170505那一天日志，但是datetime是20170503 23:59:30,是否需要调整datetime? no,load入20170505,输出20170505
  * 7.realIP可能在一个session里开始和结束不同，需要做统计
  *
  */
object PlayCombine extends BaseClass with LogConfig {
  val topicName = "play3xCombineResult"
  val baseOutputPath = FACT_HDFS_BASE_PATH_CHECK + File.separator + topicName
  val topicNameCombineTmp = "combineTmp"
  val baseOutputPathCombineTmp = FACT_HDFS_BASE_PATH_CHECK + File.separator + topicNameCombineTmp
  val short = "short"
  val baseOutputPathShort = FACT_HDFS_BASE_PATH_CHECK + File.separator + short
  val fact = "fact"
  val baseOutputPathFact = FACT_HDFS_BASE_PATH_CHECK + File.separator + fact

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

  def saveToArrayBuffer(row:Row,arrayBuffer: ArrayBuffer[Row],set: scala.collection.mutable.Set[Int],index:Int): Unit ={
    if(!set.contains(index)){
      arrayBuffer.+=(row)
      set.add(index)
    }
  }

  /** 合并操作
    * todo 1.细化，特殊情况，在同一秒有许多开始结束日志，如果根据datetime时间排序,判断是先遇到开始还是结束有随机性，多长时间上传一次日志
    *      2.order by datetime,event
    * */
  override def transform(params: Params, factDataFrame: DataFrame): DataFrame = {
    //进行索引编号
    val factDataFrameWithIndex = DataFrameUtil.dfZipWithIndex(factDataFrame, INDEX_NAME)
    factDataFrameWithIndex.cache()
    println("factDataFrameWithIndex.count():" + factDataFrameWithIndex.count())
    factDataFrameWithIndex.registerTempTable(fact_table_name)
    writeToHDFS(factDataFrameWithIndex, baseOutputPathFact)

    //获得key
    val sqlString =
      s"""select concat_ws('_',userId,episodeSid,videoSid,pathMain,realIP) as key,duration,datetime,event,${INDEX_NAME}
          |from $fact_table_name
       """.stripMargin
    val shortDataFrame = sqlContext.sql(sqlString)
    writeToHDFS(shortDataFrame, baseOutputPathShort)
    shortDataFrame.registerTempTable(shortDataFrameTable)
    //(key,(duration,datetime,event,record_index))
    val rdd1 = shortDataFrame.map(row => (row.getString(0), (row.getLong(1), row.getString(2), row.getString(3), row.getLong(4)))).groupByKey()
    import scala.util.control.Breaks._
    /**
      * (key,Iterable(tuple))
      **/
    val rddCombineTmp = rdd1.map(x => {
      val ikey = x._1
      val tupleIterable = x._2
      //order by datetime
      val list = tupleIterable.toList.sortBy(_._2)
      //val list =Sorting.quickSort(tupleIterable.toArray)(Ordering[(String, String)].on(x => (x._2, x._3)))

      val length = list.length
      //存储合并后的日志记录
      val arrayBuffer = ArrayBuffer.empty[Row]
      //负责标记已经匹配过的记录，防止重复匹配的情况,例如i1与k已经匹配，如果接下来的i2也与k匹配，那么跳过k，寻找新的匹配h
      val keySet= scala.collection.mutable.Set[Int]()
      var i: Int = 0
      while (i < length) {
        val iTuple = list(i)
        breakable {
          val iDuration = iTuple._1
          val iDateTime = iTuple._2
          val iEvent = if (null == iTuple._3) "" else iTuple._3
          val iIndex = iTuple._4

          if(keySet.contains(i)){
            i = i + 1
            break
          }

          //if iEvent is userExit or selfEnd,save record to arrayBuffer directly
          if (iEvent.equalsIgnoreCase(userExitEvent) || iEvent.equalsIgnoreCase(selfEndEvent)) {
            val row = Row(ikey, iDuration, iDateTime, iEvent+"_iFirst"+i, iIndex)
            saveToArrayBuffer(row,arrayBuffer,keySet,i)
            i = i + 1
            break
          }

          val j = i + 1
          if (j < length) {
            val jTuple = list(j)
            val jDuration = jTuple._1
            val jDateTime = jTuple._2
            val jEvent = if (null == jTuple._3) "" else jTuple._3
            val jIndex = jTuple._4

            if (iEvent.equalsIgnoreCase(startPlayEvent)&&(jEvent.equalsIgnoreCase(userExitEvent) || jEvent.equalsIgnoreCase(selfEndEvent))) {
              //发现可以合并的记录，进行合并
              val row = Row(ikey, jDuration, iDateTime, jEvent+"_combine"+i, iIndex)
              saveToArrayBuffer(row,arrayBuffer,keySet,i)
              keySet.add(j)
              i = j + 1
              break
            } else {
              //find kRow where event is userExit or selfEnd
              var k = j + 1

              if (!(k < length)) {
                //处理 i,j key相同，并且event都是startplay的情况
                val rowI = Row(ikey, iDuration, iDateTime, iEvent+"ki"+i, iIndex)
                saveToArrayBuffer(rowI,arrayBuffer,keySet,i)

                //jRow的event肯定也是startplay,否则会和iRow合并，所以也将jRow存入arrayBuffer
                val rowJ = Row(ikey, jDuration, jDateTime, jEvent+"kj"+i, jIndex)
                saveToArrayBuffer(rowJ,arrayBuffer,keySet,j)
                i = k
                break
              }

              while (k < length) {
                val kTuple = list(k)
                val kDuration = kTuple._1
                val kEvent = kTuple._3

                //处理寻找到k的event为结束的情况
                if (kEvent.equalsIgnoreCase(userExitEvent) || kEvent.equalsIgnoreCase(selfEndEvent)) {
                  if(!keySet.contains(k)){
                    val row = Row(ikey, kDuration, iDateTime, kEvent+"_kfind"+i, iIndex)
                    saveToArrayBuffer(row,arrayBuffer,keySet,k)
                    i = j
                    break
                  }
                }else if (k == length - 1 && kEvent.equalsIgnoreCase(startPlayEvent)) {
                  //处理 i,j....k1,kn的event都是startplay的情况
                  for(h<-i to k){
                    val hTuple=list(h)
                    val hDuration = hTuple._1
                    val hDateTime = hTuple._2
                    val hEvent = hTuple._3
                    val hIndex = hTuple._4
                    val rowH = Row(ikey, hDuration, hDateTime, hEvent+"_h"+h, hIndex)
                    saveToArrayBuffer(rowH,arrayBuffer,keySet,h)
                  }
                  i = k + 1
                  break
                }
                k = k + 1
              }
            }
          } else {
            //处理iRecord为最后一条记录的情况,event肯定是startplay类型[在前面已经单独处理event类型为userExitEvent或selfEndEvent类型的记录]
            val row = Row(ikey, iDuration, iDateTime, iEvent+"_last", iIndex)
            saveToArrayBuffer(row,arrayBuffer,keySet,i)
          }
          i = i + 1
        }
      }
      arrayBuffer.toList
    }).flatMap(x => x)



    println("shortDataFrame.schema.fields:" + shortDataFrame.schema.fields.foreach(e => println(e.name)))
    val combineDFTmp = sqlContext.createDataFrame(rddCombineTmp, StructType(shortDataFrame.schema.fields))
    combineDFTmp.registerTempTable(combineTmpTable)
    writeToHDFS(combineDFTmp, baseOutputPathCombineTmp)
    val df = sqlContext.sql(
      s"""select a.*,b.event as end_event,b.datetime as fDatetime,b.duration as fDuration
          | from   $shortDataFrameTable     a join
          |        $combineTmpTable         b on a.${INDEX_NAME}=b.${INDEX_NAME}
      """.stripMargin)
    df
  }

  override def load(params: Params, df: DataFrame): Unit = {
    val isBaseOutputPathExist = HdfsUtil.IsDirExist(baseOutputPath)
    if (isBaseOutputPathExist) {
      HdfsUtil.deleteHDFSFileOrPath(baseOutputPath)
      println(s"删除 $topicName 的基础目录: $baseOutputPath")
    }
    println("load记录条数:" + df.count())
    println("load结果输出目录为：" + baseOutputPath)
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
