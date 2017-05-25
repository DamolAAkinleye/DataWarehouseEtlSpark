package cn.whaley.datawarehouse.fact.moretv

import cn.whaley.datawarehouse.BaseClass
import cn.whaley.datawarehouse.global.{LogTypes, LogConfig}
import cn.whaley.datawarehouse.util.{HdfsUtil, Params}
import cn.whaley.sdk.dataexchangeio.DataIO
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.types.StructType
import scala.collection.mutable.{ArrayBuffer}

/**
  * Created by michael on 2017/5/18.
  * 整体思路：剔除单个用户在短时间内连续上抛同一个视频的播放日志的信息
  *具体方案：
  * 获取同一个用户播放同一个视频（episodeSid）的有序播放时间戳信息列表A，对A中的时间信息根据时间间隔阈值x分割获得多组时间段，计算每段时间内的上抛日志量n以及两条日志之间的平均时间间隔t。
  * 过滤参数值：x=30分钟；n=5；t=5分钟
  *case1:
  *  A...B...C分段，时间间隔30分钟，A到C为超过三分钟，然后拿A到B之间的时间段做check？
  *    错，确认后，分别检测AB,BC时间段间隔是否超过30分钟，如果BC时间间隔超过30分钟,对A...B之间的记录做check
  *case2:
  *
  */
object Play2xFilter extends BaseClass with LogConfig {
  //val topicName = "x2"
  //val baseOutputPath = FACT_HDFS_BASE_PATH_CHECK + File.separator + topicName
  //val topicNameFilter = "x2filter"
  //val baseOutputPathFilter = FACT_HDFS_BASE_PATH_CHECK + File.separator + topicNameFilter

  val fact_table_name = "log_data"

  //多组时间段间隔阀值：30分钟
  val time_quantum_threshold = 1800
  //同一个用户播放同一个剧集的播放次数阀值
  val play_times_threshold = 5
  //两条日志之间的平均时间间隔阀值：5分钟
  val avg_second_threshold = 300

  /** 加载2.x play数据 */
  override def extract(params: Params): DataFrame = {
    params.paramMap.get("date") match {
      case Some(d) => {
        val startDate = d.toString
        val moretv_input_dir = DataIO.getDataFrameOps.getPath(MORETV, LogTypes.PLAYVIEW, startDate)
        val moretvFlag = HdfsUtil.IsInputGenerateSuccess(moretv_input_dir)
        if (moretvFlag) {
          val moretvDf = DataIO.getDataFrameOps.getDF(sqlContext, Map[String, String](), MORETV, LogTypes.PLAYVIEW, startDate)
          moretvDf
        } else {
          throw new RuntimeException(s" $moretv_input_dir not exist")
        }
      }
      case None =>
        throw new RuntimeException("未设置时间参数！")
    }
  }

  /** 获得过滤结果 */
  override def transform(params: Params, factDataFrame: DataFrame): DataFrame = {
    factDataFrame.repartition(1000).registerTempTable(fact_table_name)
    println("factDataFrame.count():" + factDataFrame.count())
    val sqlStr =
      s"""select concat_ws('_',userId,episodeSid) as key,datetime,unix_timestamp(datetime) as timestampValue
          |from $fact_table_name
          |order by concat_ws('_',userId,episodeSid),datetime
       """.stripMargin
    val orderByDF = sqlContext.sql(sqlStr)
    orderByDF.cache()
    orderByDF.registerTempTable("orderbyTable")
    val array = orderByDF.collect()
    val length = array.length
    println("array size:" + length)

    //存储将被过滤掉的记录
    val arrayBuffer = ArrayBuffer.empty[Row]

    import scala.util.control.Breaks._
    var i: Int = 0

    /** 以下标i的key为基调，去寻找区块段i到k，k始终是j的下一个row，通过kRow和jRow进行比较，
      * 如果kRow和jRow之间的时间差大于30分钟，那么拿iRow和jRow【闭区间】的值进行check
      *
      */
    while (i < length) {
      val irow = array.apply(i)
      val ikey = irow.getString(0)
      breakable {
        var j = i + 1
        while (j < length - 1) {
          val jRow = array.apply(j)
          val jKey = jRow.getString(0)
          val jTimestampValue = jRow.getLong(2)

          //如果jKey不同于iKey,属于根据key不同划分区块,检测iRow到jRow之间的值，然后跳过j层循环
          if (!ikey.equalsIgnoreCase(jKey)) {
            saveRecordToArray(array, i, j, arrayBuffer)
            i = j
            break
          }
          //取jRow的下一个值
          val k = j + 1
          val kRow = array.apply(k)
          val kKey = kRow.getString(0)
          val kTimestampValue = kRow.getLong(2)

          //发现用来比较时间间隔的kKey不等于jKey，属于根据key不同划分区块,需要检测iRow到jRow之间的值，然后跳过j层循环
          if (!jKey.equalsIgnoreCase(kKey)) {
            saveRecordToArray(array, i, k, arrayBuffer)
            i = k
            break
          }

          //如果key相同，检测是否可以根据时间阀值来划分区块
          if (kTimestampValue - jTimestampValue > time_quantum_threshold) {
            saveRecordToArray(array, i, k, arrayBuffer)
            i = k
            break
          }
          j = j + 1
        }
        i = i + 1
      }
    }

    println("arrayBuffer size:" + arrayBuffer.size)
    val rdd = sc.parallelize(arrayBuffer, 1000)
    println("orderByDF.schema.fields:" + orderByDF.schema.fields.foreach(e => println(e.name)))
    val filterDF = sqlContext.createDataFrame(rdd, StructType(orderByDF.schema.fields))
    filterDF.registerTempTable("filterTable")
    //writeToHDFS(filterDF, baseOutputPathFilter)
    val df = sqlContext.sql(
      s"""select a.*
        | from       $fact_table_name   a
        | left join  filterTable        b on
        |    concat_ws('_',a.userId,a.episodeSid)=b.key  and
        |    a.datetime=b.datetime
        |where b.key is null
      """.stripMargin).withColumnRenamed("duration","fDuration").withColumnRenamed("event","end_event").withColumnRenamed("datetime","fDatetime")
    df
  }

  //将数组中i到j之间[左闭右开区间]的数值存入另一个list,用来做过滤
  def saveRecordToArray(array: Array[Row], i: Int, j: Int, arrayBuffer: ArrayBuffer[Row]) {
    val check_play_times_threshold = j - i
    val endRow = array.apply(j - 1)
    val endTimestampValue = endRow.getLong(2)

    val startRow = array.apply(i)
    val startTimestampValue = startRow.getLong(2)

    val check_avg_second_threshold = (endTimestampValue - startTimestampValue) / check_play_times_threshold
    if (check_play_times_threshold > play_times_threshold && check_avg_second_threshold < avg_second_threshold) {

      for (h <- i until j) {
        arrayBuffer.+=(array.apply(h))
      }
    }
  }

  override def load(params: Params, df: DataFrame): Unit = {
    val date = params.paramMap("date").toString
    val baseOutputPath= DataIO.getDataFrameOps.getPath(MERGER,LogTypes.MEDUSA_PLAY_2X_FILTER_RESULT,date)
    val isBaseOutputPathExist = HdfsUtil.IsDirExist(baseOutputPath)
    if (isBaseOutputPathExist) {
      HdfsUtil.deleteHDFSFileOrPath(baseOutputPath)
      println(s"删除目录: $baseOutputPath")
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
