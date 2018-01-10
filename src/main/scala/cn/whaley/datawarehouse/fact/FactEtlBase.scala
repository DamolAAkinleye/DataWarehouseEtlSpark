package cn.whaley.datawarehouse.fact

import java.io.File
import java.util.Calendar

import cn.whaley.datawarehouse.BaseClass
import cn.whaley.datawarehouse.common.{DimensionColumn, UserDefinedColumn}
import cn.whaley.datawarehouse.fact.constant.Constants._
import cn.whaley.datawarehouse.fact.constant.LogPath
import cn.whaley.datawarehouse.global.Globals._
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.{DataExtractUtils, DataFrameUtil, HdfsUtil, Params}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

/**
  * Created by Tony on 17/4/5.
  */
abstract class FactEtlBase extends BaseClass {

  private val INDEX_NAME = "source_index"

  var columnsFromSource: List[(String, String)] = _

  var topicName: String = _

  var parquetPath: String = _

  var odsTableName: String = _

  var addColumns: List[UserDefinedColumn] = _

  var dimensionColumns: List[DimensionColumn] = _

  /**
    * 在最终获取事实表字段时需要用到的维度表名称
    */
  var dimensionsNeedInFact: List[String] = _

  var partition: Int = 0

  /**
    * 事实发生的时间，格式yyyy-MM-dd HH:mm:ss
    */
  var factTime: String = "concat(dim_date, '', dim_time)"

  var dfLineCount = 0L

  //  override def execute(params: Params): Unit = {
  //    val result = doExecute(params)
  //
  //    HdfsUtil.deleteHDFSFileOrPath(MEDUSA_FACT_HDFS_BASE_PATH + File.separator + topicName + File.separator + params.startDate)
  //    result.write.parquet(MEDUSA_FACT_HDFS_BASE_PATH + File.separator + topicName + File.separator + params.startDate)
  //  }

  /**
    * 源数据读取函数, ETL中的Extract
    * 如需自定义，可以在子类中重载实现
    *
    * @return
    */
  override def extract(params: Params): DataFrame = {
    params.paramMap.get("date") match {
      case Some(d) => {
        println("数据时间：" + d)
        if (partition == 0) {
          readSource(d.toString, params.startHour)
        } else {
          readSource(d.toString, params.startHour).repartition(partition)
        }
      }
      case None =>
        throw new RuntimeException("未设置时间参数！")
    }
  }

  def readSource(sourceDate: String, sourceHour: String): DataFrame = {
    if (sourceDate == null) {
      null
    } else if (readSourceType == null || readSourceType == ods) {
      DataExtractUtils.readFromOds(sqlContext, odsTableName, sourceDate, sourceHour)
    } else if (readSourceType == ods_parquet) {
      DataExtractUtils.readFromOdsParquet(sqlContext, odsTableName, sourceDate, sourceHour)
    } else if (readSourceType == parquet) {
      DataExtractUtils.readFromParquet(sqlContext, parquetPath, sourceDate)
    }
    else {
      null
    }
  }

  /**
    * 数据转换函数，ETL中的Transform
    *
    * @return
    */
  override def transform(params: Params, sourceDf: DataFrame): DataFrame = {
    val filteredSourceDf = filterRows(sourceDf)
    val completeSourceDf = addNewColumns(filteredSourceDf)
    completeSourceDf.printSchema()
    completeSourceDf.persist(StorageLevel.MEMORY_AND_DISK_SER)

    dfLineCount = completeSourceDf.count()
    println("完整事实表行数：" + dfLineCount)

    if (dfLineCount == 0) {
      throw new RuntimeException("未读取到源数据！")
    }
    //    if (debug) {
    //      println("完整事实表行数：" + completeSourceDf.count())
    //      HdfsUtil.deleteHDFSFileOrPath(FACT_HDFS_BASE_PATH + File.separator + topicName + File.separator + "debug" + File.separator + "completeSource")
    //      completeSourceDf.write.parquet(FACT_HDFS_BASE_PATH + File.separator + topicName + File.separator + "debug" + File.separator + "completeSource")
    //    }


    val dimensionJoinDf = parseDimension(completeSourceDf, dimensionColumns, INDEX_NAME, factTime)
    //    if (debug) {
    //      dimensionJoinDf.persist()
    //      println("维度关联表行数：" + dimensionJoinDf.count())
    //    }

    //关联源数据和join到的维度
    var df = completeSourceDf.join(dimensionJoinDf, List(INDEX_NAME), "leftouter").as("source")

    // 关联用到的维度
    if (dimensionColumns != null && dimensionsNeedInFact != null) {
      dimensionColumns.foreach(c => {
        if (dimensionsNeedInFact.contains(c.dimensionNameAs)) {
          val dimensionDf = sqlContext.read.parquet(DIMENSION_HDFS_BASE_PATH + File.separator + c.dimensionName)
          df = df.join(dimensionDf.as(c.dimensionNameAs),
            expr("source." + c.factSkColumnName + " = " + c.dimensionNameAs + "." + c.dimensionSkName),
            "leftouter")
        }
      })
    }

    //筛选指定的列
    val result = df.selectExpr(
      columnsFromSource.map(
        c => if (c._2.contains(" ") || c._2.contains("."))
          c._2 + " as " + c._1
        else
          "source." + c._2 + " as " + c._1)
        ++ dimensionJoinDf.schema.fields.filter(_.name != INDEX_NAME || debug).map("source." + _.name)
        : _*
    )

    //    if(debug) {
    //      println("最终结果行数：" + result.count())
    //    }
    result
  }

  def filterRows(sourceDf: DataFrame): DataFrame = {
    sourceDf
  }

  private def addNewColumns(sourceDf: DataFrame): DataFrame = {
    var result = sourceDf
    if (addColumns != null) {
      addColumns.foreach(column => {
        result = result.withColumn(column.name, column.udf(column.inputColumns.map(col): _*))
      }
      )
    }
    DataFrameUtil.dfZipWithIndex(result, INDEX_NAME)
//    DataFrameUtil.dfAddIndex(result, INDEX_NAME, expr(factTime).asc)
  }

  override def load(params: Params, df: DataFrame): Unit = {
    /* HdfsUtil.deleteHDFSFileOrPath(FACT_HDFS_BASE_PATH + File.separator + topicName + File.separator + params.paramMap("date") + File.separator + "00")
     if (partition == 0) {
       df.write.parquet(FACT_HDFS_BASE_PATH + File.separator + topicName + File.separator + params.paramMap("date") + File.separator + "00")
     }else{
       df.repartition(partition).write.parquet(FACT_HDFS_BASE_PATH + File.separator + topicName + File.separator + params.paramMap("date") + File.separator + "00")
     }*/
    backup(params, df, topicName)
  }

  /**
    * 用来备份实时表数据，然后将维度数据生成在临时目录，当isOnline参数为true的时候，将临时目录的数据替换线上维度
    *
    * @param p  the main args
    * @param df the DataFrame from execute function
    * @return a Unit.
    */
  private def backup(p: Params, df: DataFrame, topicName: String): Unit = {
    val date = p.paramMap("date")
    val hour = if (p.startHour == null) {
      "00"
    } else {
      p.startHour
    }
    val onLineFactDir = FACT_HDFS_BASE_PATH + File.separator + topicName + File.separator + p.paramMap("date") + File.separator + hour
    val onLineFactParentDir = FACT_HDFS_BASE_PATH + File.separator + topicName + File.separator + p.paramMap("date")
    val onLineFactBackupDir = FACT_HDFS_BASE_PATH_BACKUP + File.separator + topicName + File.separator + date + File.separator + hour
    val onLineFactBackupParentDir = FACT_HDFS_BASE_PATH_BACKUP + File.separator + topicName + File.separator +  date
    val onLineFactDirTmp = FACT_HDFS_BASE_PATH_TMP + File.separator + topicName + File.separator + date + File.separator + hour
    println("线上数据目录:" + onLineFactDir)
    println("线上数据备份目录:" + onLineFactBackupDir)
    println("线上数据临时目录:" + onLineFactDirTmp)

    //防止文件碎片
    val total_count = BigDecimal(dfLineCount)
    val load_to_hdfs_partition = (total_count / FACT_THRESHOLD_VALUE).intValue() + 1
    println("load_to_hdfs_partition:" + load_to_hdfs_partition)

    val isTmpExist = HdfsUtil.IsDirExist(onLineFactDirTmp)
    if(isTmpExist){
      HdfsUtil.deleteHDFSFileOrPath(onLineFactDirTmp)
    }
    println("生成线上维度数据到临时目录:" + onLineFactDirTmp)
    df.repartition(load_to_hdfs_partition).write.parquet(onLineFactDirTmp)
    /*if (partition == 0) {
      df.write.parquet(onLineFactDirTmp)
    } else {
      df.repartition(partition).write.parquet(onLineFactDirTmp)
    }*/

    println("数据是否准备上线:" + p.isOnline)
    if (p.isOnline) {
      val isOnlineFileExist = HdfsUtil.IsDirExist(onLineFactDir)
      println("数据上线:" + onLineFactDir)
      if (isOnlineFileExist) {
        println("生成线上维度备份数据:" + onLineFactBackupDir)
        HdfsUtil.deleteHDFSFileOrPath(onLineFactBackupDir)
        HdfsUtil.createDir(onLineFactBackupParentDir)
        val isSuccessBackup = HdfsUtil.rename(onLineFactDir, onLineFactBackupDir)
        println("备份数据状态:" + isSuccessBackup)
      }

      val isOnlineFileExistAfterRename = HdfsUtil.IsDirExist(onLineFactDir)
      if (isOnlineFileExistAfterRename) {
        throw new RuntimeException("rename failed")
      } else {
        val isOnLineFactParentDir = HdfsUtil.createDir(onLineFactParentDir)
        println("数据上线的父目录是否创建成功:" + isOnLineFactParentDir)
        val isSuccess = HdfsUtil.rename(onLineFactDirTmp, onLineFactDir)
        println("数据上线状态:" + isSuccess)
      }
    }
  }

}
