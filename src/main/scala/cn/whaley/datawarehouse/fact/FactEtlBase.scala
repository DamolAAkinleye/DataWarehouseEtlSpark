package cn.whaley.datawarehouse.fact

import java.util.Calendar

import cn.whaley.datawarehouse.BaseClass
import cn.whaley.datawarehouse.common.{DimensionColumn, UserDefinedColumn}
import cn.whaley.datawarehouse.fact.constant.Constants._
import cn.whaley.datawarehouse.fact.constant.LogPath
import cn.whaley.datawarehouse.global.Globals._
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.{DateFormatUtils, DataFrameUtil, HdfsUtil, Params}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

import scala.reflect.io.File


/**
  * Created by Tony on 17/4/5.
  */
abstract class FactEtlBase extends BaseClass {

  private val INDEX_NAME = "source_index"

  var columnsFromSource: List[(String, String)] = _

  var topicName: String = _

  var parquetPath: String = _

  var addColumns: List[UserDefinedColumn] = _

  var dimensionColumns: List[DimensionColumn] = _

  /**
    * 事实发生的时间，格式yyyy-MM-dd HH:mm:ss
    */
  var factTime: String = "concat(dim_date, '', dim_time)"

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
        readSource(d.toString)
      }
      case None =>
        throw new RuntimeException("未设置时间参数！")
    }
  }

  def readSource(sourceDate: String): DataFrame = {
    if (sourceDate == null) {
      null
    } else if (readSourceType == null || readSourceType == parquet) {
      readFromParquet(parquetPath, sourceDate)
    } else {
      null
    }
  }

  def readFromParquet(path: String, sourceDate: String): DataFrame = {
    val filePath = path.replace(LogPath.DATE_ESCAPE, sourceDate)
    val sourceDf = sqlContext.read.parquet(filePath)
    sourceDf
  }

  /**
    * 数据转换函数，ETL中的Transform
    *
    * @return
    */
  override def transform(params: Params, sourceDf: DataFrame): DataFrame = {
    println("------- before transform "+Calendar.getInstance().getTime)
    val filteredSourceDf = filterRows(sourceDf)
    val completeSourceDf = addNewColumns(filteredSourceDf)
    println("-------start completeSourceDf.cache()"+Calendar.getInstance().getTime)
    completeSourceDf.cache()
    println("-------end completeSourceDf.cache()"+Calendar.getInstance().getTime)

    if(debug) {
      println("完整事实表行数：" + completeSourceDf.count())
//      completeSourceDf.show()
    }


    val dimensionJoinDf = parseDimension(completeSourceDf, dimensionColumns, INDEX_NAME, factTime)
    if(debug) {
      dimensionJoinDf.persist()
      println("维度关联表行数：" + dimensionJoinDf.count())
//      dimensionJoinDf.show()
    }

    println("-------before completeSourceDf join dimensionJoinDf"+Calendar.getInstance().getTime)
    val df = completeSourceDf.join(dimensionJoinDf, List(INDEX_NAME), "leftouter").as("source")
    println("-------after completeSourceDf join dimensionJoinDf"+Calendar.getInstance().getTime)
    /*if (dimensionColumns != null) {
      //关联所有的维度  TODO 判断只关联用到的维度
      dimensionColumns.foreach(c => {
        val dimensionDf = sqlContext.read.parquet(DIMENSION_HDFS_BASE_PATH + File.separator + c.dimensionName)
        df = df.join(dimensionDf.as(c.dimensionName),
          expr("source." + c.dimensionColumnName + " = " + c.dimensionName + "." + c.dimensionSkName),
          "leftouter")
      })
    }*/
    println("-------before 筛选特定列"+Calendar.getInstance().getTime)
    val result = df.selectExpr(
      columnsFromSource.map(
        c => if (c._2.contains(" ") || c._2.contains("."))
          c._2 + " as " + c._1
        else
          "source." + c._2 + " as " + c._1)
        ++ dimensionJoinDf.schema.fields.filter(_.name != INDEX_NAME).map("source." + _.name)
        : _*
    )
    println("-------after 筛选特定列"+Calendar.getInstance().getTime)

    if(debug) {
      println("最终结果行数：" + result.count())
    }
    println("------- last line in transform "+Calendar.getInstance().getTime)
    result
  }

  def filterRows(sourceDf: DataFrame): DataFrame = {
    sourceDf
  }

  private def addNewColumns(sourceDf: DataFrame): DataFrame = {
    println("-------before addNewColumns "+Calendar.getInstance().getTime)
    var result = DataFrameUtil.dfZipWithIndex(sourceDf, INDEX_NAME)
    if (addColumns != null) {
      addColumns.foreach(column =>{
        println("-------start add column: "+column.name+","+Calendar.getInstance().getTime)
        result = result.withColumn(column.name, column.udf(column.inputColumns.map(col): _*))
        println("-------end add column: "+","+column.name+Calendar.getInstance().getTime)
      }
      )
    }
    println("-------after addNewColumns "+Calendar.getInstance().getTime)
    result
  }
/*
  private def addNewColumns(sourceDf: DataFrame): DataFrame = {
    val sourceDfWithIndex = DataFrameUtil.dfZipWithIndex(sourceDf, INDEX_NAME)
    if (addColumns != null) {
      val buf = scala.collection.mutable.ListBuffer.empty[DataFrame]
      addColumns.foreach(column =>{
         val result2 = addNewColumn(sourceDfWithIndex,column)
        buf.+=(result2)
      }
      )
    }
    sourceDfWithIndex
  }

  private def addNewColumn(sourceDf: DataFrame,column:UserDefinedColumn): DataFrame = {
    sourceDf.withColumn(column.name, column.udf(column.inputColumns.map(col): _*))
  }*/

  override def load(params: Params, df: DataFrame): Unit = {
    HdfsUtil.deleteHDFSFileOrPath(FACT_HDFS_BASE_PATH + File.separator + topicName + File.separator + params.paramMap("date") + File.separator + "00")
    df.repartition(2000).write.parquet(FACT_HDFS_BASE_PATH + File.separator + topicName + File.separator + params.paramMap("date") + File.separator + "00")
    //backup(params, df, topicName)
  }

  /**
    * 用来备份维度数据，然后将维度数据生成在临时目录，当isOnline参数为true的时候，将临时目录的数据替换线上维度
    *
    * @param p  the main args
    * @param df the DataFrame from execute function
    * @return a Unit.
    */
  private def backup(p: Params, df: DataFrame, topicName: String): Unit = {
    val cal = Calendar.getInstance
    val date = DateFormatUtils.readFormat.format(cal.getTime)
    val onLineFactDir = FACT_HDFS_BASE_PATH + File.separator + topicName + File.separator + p.paramMap("date") + File.separator + "00"
    val onLineFactParentDir = FACT_HDFS_BASE_PATH + File.separator + topicName + File.separator + p.paramMap("date")
    val onLineFactBackupDir = FACT_HDFS_BASE_PATH_BACKUP + File.separator + date + File.separator + topicName
    val onLineFactDirTmp = FACT_HDFS_BASE_PATH_TMP + File.separator + topicName
    val onLineFactDirDelete = FACT_HDFS_BASE_PATH_DELETE + File.separator + topicName
    println("线上数据目录:" + onLineFactDir)
    println("线上数据备份目录:" + onLineFactBackupDir)
    println("线上数据临时目录:" + onLineFactDirTmp)
    println("线上数据等待删除目录:" + onLineFactDirDelete)

    //df.persist(StorageLevel.MEMORY_AND_DISK)

    val isOnlineFileExist = HdfsUtil.IsDirExist(onLineFactDir)
    if (isOnlineFileExist) {
      val isBackupExist = HdfsUtil.IsDirExist(onLineFactBackupDir)
      if (isBackupExist) {
        println("数据已经备份,跳过备份过程")
      } else {
        println("生成线上维度备份数据:" + onLineFactBackupDir)
        val isSuccessBackup = HdfsUtil.copyFilesInDir(onLineFactDir, onLineFactBackupDir)
        println("备份数据状态:" + isSuccessBackup)
      }
    } else {
      println("无可用备份数据")
    }

    //防止文件碎片
/*    val total_count = BigDecimal(df.count())
    val partition = Math.max(1, (total_count / THRESHOLD_VALUE).intValue())
    println("repartition:" + partition)*/

    val isTmpExist = HdfsUtil.IsDirExist(onLineFactDirTmp)
    if (isTmpExist) {
      println("删除线上维度临时数据:" + onLineFactDirTmp)
      HdfsUtil.deleteHDFSFileOrPath(onLineFactDirTmp)
    }
    println("生成线上维度数据到临时目录:" + onLineFactDirTmp)
    df.repartition(2000).write.parquet(onLineFactDirTmp)

    println("数据是否上线:" + p.isOnline)
    if (p.isOnline) {
      println("数据上线:" + onLineFactDir)
      if (isOnlineFileExist) {
        println("移动线上数据:from " + onLineFactDir + " to " + onLineFactDirDelete)
        val isRenameSuccess = HdfsUtil.rename(onLineFactDir, onLineFactDirDelete)
        println("isRenameSuccess:" + isRenameSuccess)
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
      println("删除过期数据:" + onLineFactDirDelete)
      HdfsUtil.deleteHDFSFileOrPath(onLineFactDirDelete)
    }
  }

}
