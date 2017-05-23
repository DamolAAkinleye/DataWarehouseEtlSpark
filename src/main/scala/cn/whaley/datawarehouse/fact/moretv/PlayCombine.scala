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
  val topicName = "combine"
  val baseOutputPath = FACT_HDFS_BASE_PATH_CHECK + File.separator + topicName
  val topicNameCombine = "combineTmp"
  val baseOutputPathCombine = FACT_HDFS_BASE_PATH_CHECK + File.separator + topicNameCombine
  val short = "short"
  val baseOutputPathShort = FACT_HDFS_BASE_PATH_CHECK + File.separator + short
  val fact = "fact"
  val baseOutputPathFact = FACT_HDFS_BASE_PATH_CHECK + File.separator + fact


  val fact_table_name = "log_data"
  val INDEX_NAME = "record_index"
  val startPlayEvent = "startplay"
  val userExitEvent = "userexit"
  val selfEndEvent = "selfend"
  val noEnd = "noEnd"
  val shortDataFrameTable="shortDataFrameTable"
  val combineTable="combineTable"

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
          medusaDf
        } else {
          throw new RuntimeException(s" $medusa_input_dir not exist")
        }
      }
      case None =>
        throw new RuntimeException("未设置时间参数！")
    }
  }

  /** 合并操作 */
  /** 遍历点1：以下标i的ikey为基调，去寻找相同jkey【jKey为紧邻iKey的下一个值】
    * 如果iKey=jKey
    * a.iKey.event='startplay'，jKey.event='startplay',那么继续遍历jKey的下一个值
    * 如果一直没有找到kRow[userexit或selfend类型的event记录k],自己作为一条合并记录存在【end_event为null】，
    * 并标记ikey基调无userexit或selfend类型Key,节省接下来都为startPlay类型的key对下一级的遍历
    * 如果找到kRow,判断kRow是否在临时Set中，
    * 否，合并为一条记录【datetime使用startplay的datetime，end_event为记录的event】,并标记kRow为被合并记录放入临时Set
    * 是，跳过循环
    * b.iKey.event='userexit或selfend'，自己作为一条合并记录存在【datetime使用记录的datetime，end_event为记录的event】
    * jKey进入遍历点1
    *
    * 如果iKey不等于jKey
    * iKey自己作为一条合并记录存在
    * 如果iKey.event='startplay'【end_event为null】,jKey进入遍历点1
    * 如果iKey.event='userexit或selfend'【datetime使用记录的datetime，end_event为记录的event】,jKey进入遍历点1
    *
    * // 线上替换为 on concat_ws('_',a.userId,a.episodeSid,a.videoSid,a.pathMain,a.realIP)=b.key
    *
    * 测试用
    * 合并后的记录存入arrayBuffer[Row]作为表b,orderByTable作为表a
    * 筛选出
    * select a.*,b.event as end_event,b.datetime as finalDatetime
    * from a join b on
    * a.key=b.key and a.record_index=b.record_index
    * a.datetime=b.datetime
    *
    * 特殊情况处理：
    * 1.a表存在key和datatime都相同的记录，和b表关联会产生笛卡尔积,
    * 解决方式：1.需要是对列进行唯一编号解决
    * 2.去掉event字段的distinct Row作为新的表a解决[弊端：如果存在打点bug等因素，不能保证同一个play session里，开始和结束日志的其他字段完全相同]
    */
  override def transform(params: Params, factDataFrame: DataFrame): DataFrame = {
    //进行索引编号
    val factDataFrameWithIndex = DataFrameUtil.dfZipWithIndex(factDataFrame, INDEX_NAME)
    factDataFrameWithIndex.cache()
    println("factDataFrameWithIndex.count():" + factDataFrameWithIndex.count())
    factDataFrameWithIndex.repartition(2000).registerTempTable(fact_table_name)
    writeToHDFS(factDataFrameWithIndex,baseOutputPathFact)

    //获得key
    val sqlString =
      s"""select concat_ws('_',userId,episodeSid,videoSid,pathMain,realIP) as key,duration,datetime,event,${INDEX_NAME}
          |from $fact_table_name
       """.stripMargin
    val shortDataFrame = sqlContext.sql(sqlString)
    writeToHDFS(shortDataFrame, baseOutputPathShort)
    shortDataFrame.registerTempTable(shortDataFrameTable)
    //(key,(duration,datetime,event,record_index))
    val rdd1 = shortDataFrame.map(row => (row.getString(0),(row.getLong(1),row.getString(2),row.getString(3),row.getLong(4)))).groupByKey()
    import scala.util.control.Breaks._
    /**
      * (key,Iterable(tuple))
      * */
    val rddCombine=rdd1.map(x => {
      val ikey = x._1
      val tupleIterable=x._2
      //order by datetime
      val list=tupleIterable.toList.sortBy(_._2)
      val length=list.length
      //存储合并后的日志记录
      val arrayBuffer = ArrayBuffer.empty[Row]
      var i: Int = 0
      while (i < length) {
        /** keyToIndexMap有两个作用:
          * 1.自动跳过已经匹配上的,event为userExit or selfEnd记录
          * 2.负责标记已经匹配过的记录，防止重复匹配的情况,例如i与k已经匹配，如果接下来的j也与k匹配，那么跳过k，寻找新的匹配h
          * */
       val keyToIndexMap = scala.collection.mutable.HashMap.empty[String, scala.collection.mutable.Set[Int]]

        // create map to save match record which event is userExit or selfEnd,so we can skip this record which already matched.
        breakable {
          val iTuple= list(i)
          if(null==iTuple){
            i=i+1
            break
          }
          val iDuration= iTuple._1
          val iDateTime= iTuple._2
          val iEvent= if(null==iTuple._3) "" else iTuple._3
          val iIndex= iTuple._4

          if (keyToIndexMap.contains(ikey)) {
            val ikeySet = keyToIndexMap.get(ikey).get
            if (ikeySet.contains(i)) {
              //skip this index which event is userExit or selfEnd,and already combine with previous record which event is startplay
              i = i + 1
            }
          }

          //if iEvent is userExit or selfEnd,save record to arrayBuffer directly[单独处理]
          if (iEvent.equalsIgnoreCase(userExitEvent) || iEvent.equalsIgnoreCase(selfEndEvent)) {
            val row = Row(ikey,iDuration, iDateTime, noEnd, iIndex)
            arrayBuffer.+=(row)
            i = i + 1
            break
          }
          val j = i + 1
          if (j < length) {
            val jTuple = list(j)
            val jDuration= jTuple._1
            val jDateTime= jTuple._2
            val jEvent= jTuple._3
            val jIndex= jTuple._4

              if (jEvent.equalsIgnoreCase(userExitEvent) || jEvent.equalsIgnoreCase(selfEndEvent)) {
                //发现可以合并的记录，进行合并
                val row = Row(ikey,jDuration,iDateTime, jEvent, iIndex)
                arrayBuffer.+=(row)
                i = j + 1
              } else {
                //find kRow where event is userExit or selfEnd
                var k = j + 1

                if (!(k < length)) {
                  //处理 i,j key相同，并且event都是startplay的情况
                  val rowI = Row(ikey,iDuration,iDateTime, noEnd, iIndex)
                  arrayBuffer.+=(rowI)
                  //jRow的event肯定也是userExitEvent或selfEndEvent,否则会和iRow合并，所以也将jRow存入arrayBuffer
                  val rowJ = Row(ikey,jDuration,jDateTime, noEnd, jIndex)
                  arrayBuffer.+=(rowJ)
                  i = k
                  break
                }

                while (k < length) {
                  val kTuple = list(k)
                  val kDuration= kTuple._1
                  val kEvent= kTuple._3

                    if (kEvent.equalsIgnoreCase(userExitEvent) || kEvent.equalsIgnoreCase(selfEndEvent)) {
                      val row = Row(ikey, kDuration,iDateTime, kEvent, iIndex)
                      //save k to set for skip this array index
                      if (keyToIndexMap.contains(ikey)) {
                        val set = keyToIndexMap.get(ikey).get
                        if (set.contains(k)) {
                          //防止重复匹配的情况,例如i与k已经匹配，如果接下来的j也与k匹配，那么跳过k，寻找新的匹配h
                          k = k + 1
                        } else {
                          keyToIndexMap.get(ikey).get.+(k)
                          arrayBuffer.+=(row)
                          i = j
                          break
                        }
                      } else {
                        val mutableSet = scala.collection.mutable.Set(k)
                        keyToIndexMap.put(ikey, mutableSet)
                        arrayBuffer.+=(row)
                        i = j
                        break
                      }
                    }
                  k = k + 1
                }
              }
          } else {
            //处理iRecord为最后一条记录的情况,event肯定是startplay类型[在前面已经单独处理event类型为userExitEvent或selfEndEvent类型的记录]
            val row = Row(ikey,iDuration, iDateTime, noEnd, iIndex)
            arrayBuffer.+=(row)
          }
          i = i + 1
        }
      }
      arrayBuffer.toList
    }).flatMap(x=>x)

    println("shortDataFrame.schema.fields:" + shortDataFrame.schema.fields.foreach(e => println(e.name)))
    val combineDF = sqlContext.createDataFrame(rddCombine, StructType(shortDataFrame.schema.fields))
    combineDF.registerTempTable(combineTable)
    writeToHDFS(combineDF, baseOutputPathCombine)
    val df = sqlContext.sql(
      s"""select a.*,b.event as end_event,b.datetime as fDatetime,b.duration as fDuration
          | from   $shortDataFrameTable  a join
          |        $combineTable         b on a.${INDEX_NAME}=b.${INDEX_NAME}
      """.stripMargin)
    df
  }

  override def load(params: Params, df: DataFrame): Unit = {
    println(s"load df to $baseOutputPath")
    val isBaseOutputPathExist = HdfsUtil.IsDirExist(baseOutputPath)
    if (isBaseOutputPathExist) {
      HdfsUtil.deleteHDFSFileOrPath(baseOutputPath)
      println(s"删除 $topicName 的基础目录: $baseOutputPath")
    }
    println("过滤后记录条数:" + df.count())
    println("过滤后结果输出目录为：" + baseOutputPath)
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
