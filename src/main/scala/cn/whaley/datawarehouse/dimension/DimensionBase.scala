package cn.whaley.datawarehouse.dimension

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import cn.whaley.datawarehouse.BaseClass
import cn.whaley.datawarehouse.dimension.constant.Constants._
import cn.whaley.datawarehouse.dimension.constant.SourceType._
import cn.whaley.datawarehouse.util.{DataFrameUtil, DateFormatUtils, HdfsUtil, ParamsParseUtil}
import org.apache.commons.lang3.time.DateUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

import scala.reflect.io.File

/**
  * Created by Tony on 17/3/8.
  */
abstract class DimensionBase extends BaseClass {

  /**
    * 维度表列相关配置
    */
  val columns = new Columns

  /**
    * 读取原始数据dataframe的方式，默认是jdbc
    *
    * @see [[SourceType]]
    */
  var readSourceType: Value = jdbc

  /**
    * 来源读取配置jdbc参数，仅当readSourceType设置为jdbc时有效
    */
  var sourceDb: Map[String, String] = _

  /**
    * 维度表名称，同时也是hdfs上的目录名
    */
  var dimensionName: String = _

  /**
    * 过滤源数据使用的where条件，仅当filterSource方法未在子类中重载时有效
    */
  var sourceFilterWhere: String = _

  /**
    * 维度表字段与源数据字段的对应关系，仅当filterSource方法未在子类中重载时有效
    */
  var sourceColumnMap: Map[String, String] = Map()

  override def execute(args: Array[String]): Unit = {

    val result = doExecute()

    println("backup start ....")
    backup(args, result, dimensionName)
    println("backup end ....")

    //TODO 新数据验证

  }

  def doExecute(): DataFrame = {

    //TODO 初始化参数处理和验证

    val onlineDimensionDir = DIMENSION_HDFS_BASE_PATH + File.separator + dimensionName

    //读取源数据
    val sourceDf = readSource(readSourceType)

    //过滤源数据
    val filteredSourceDf = filterSource(sourceDf)

    //TODO 过滤后源数据主键唯一性判断和处理

    filteredSourceDf.persist()

    //首次创建维度
    if (!HdfsUtil.pathIsExist(onlineDimensionDir)) {
      val result = DataFrameUtil.dfZipWithIndex(
        DataFrameUtil.addDimTime(filteredSourceDf, DimensionBase.defaultValidTime, null),
        columns.skName
      )
      return result
    }

    val today = DateUtils.truncate(new Date(), Calendar.DATE)

    //读取现有维度
    val originalDf = sqlContext.read.parquet(onlineDimensionDir)

    println("现有维度：\n")
    originalDf.show

    //新增的行
    val addDf =
      filteredSourceDf.as("b").join(
        originalDf.where(columns.invalidTimeKey + " is null").as("a"), columns.primaryKeys, "leftouter"
      ).where(
        "a." + columns.skName + " is null"
      ).selectExpr(
        columns.getSourceColumns.map(s => "b." + s): _*
      )

    //更新后维度表中需要添加的行，包括新增的和追踪列变化的
    val extendDf =
      if (columns.trackingColumns == null || columns.trackingColumns.isEmpty) {
        addDf
      } else {
        addDf.unionAll(
          //追踪列变化的行
          filteredSourceDf.as("b").join(
            originalDf.where(columns.invalidTimeKey + " is null").as("a"), columns.primaryKeys, "leftouter"
          ).where(
            columns.trackingColumns.map(s => "a." + s + " != b." + s).mkString(" or ")
          ).selectExpr(columns.getSourceColumns.map(s => "b." + s): _*)
        )
      }

    println("需要增加的行：\n")
    extendDf.show

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val todayStr = sdf.format(today)


    //现有维度表中已经存在的行，已经根据现有源信息做了字段更新，但是未更新dim_invalid_time
    //如果维度表中已经存在的业务键在源信息中被删除了，则会把该业务键对应行的所有otherColumns设置为null
    val originalExistDf = originalDf.as("a").join(
      filteredSourceDf.as("b"), columns.primaryKeys, "leftouter"
    ).selectExpr(
      List("a." + columns.skName) ++ columns.primaryKeys
        ++ columns.trackingColumns.map(s => "a." + s) ++ columns.otherColumns.map(s => "b." + s)
        ++ List(columns.validTimeKey, columns.invalidTimeKey).map(s => "a." + s): _*
    )

    //现有维度表中已经存在的行，已经根据现有源信息做了字段更新，并且更新了dim_invalid_time
    val df =
      if (columns.trackingColumns == null || columns.trackingColumns.isEmpty) {
        originalExistDf
      } else {
        //变更后需要标注失效时间的行，包含代理键和失效时间两列
        val invalidColumnsDf =
          filteredSourceDf.as("b").join(
            originalDf.where(columns.invalidTimeKey + " is null").as("a"), columns.primaryKeys, "leftouter"
          ).where(
            columns.trackingColumns.map(s => "a." + s + " != b." + s).mkString(" or ")
          ).selectExpr(List("a." + columns.skName)
            ++ List("'" + todayStr + "' as " + columns.invalidTimeKey): _*)

        println("需要变更失效时间的行：\n")
        invalidColumnsDf.show

        //更新失效时间
        originalExistDf.as("origin").join(invalidColumnsDf.as("invalid"), List(columns.skName), "leftouter"
        ).selectExpr(
          List(columns.skName) ++ columns.primaryKeys
            ++ columns.trackingColumns ++ columns.otherColumns
            ++ List(columns.validTimeKey)
            ++ List("CASE WHEN invalid." + columns.invalidTimeKey + " is not null THEN invalid." + columns.invalidTimeKey
            + " ELSE origin." + columns.invalidTimeKey + " END as " + columns.invalidTimeKey): _*
        )
      }

    println("原有维度数据更新后的：\n")
    df.show

    //合并上述形成最终结果
    val result = df.unionAll(
      DataFrameUtil.dfZipWithIndex(
        DataFrameUtil.addDimTime(extendDf, today, null)
        , columns.skName
        , df.selectExpr("max(" + columns.skName + ")").first().getLong(0)
      )
    )

    println("最终生成的新维度：\n")
    result.show

    result
  }

  /**
    * 自定义的源数据读取方法
    * 默认是从jdbc中读取，如需自定义，可以在子类中重载实现
    *
    * @return
    */
  def readSource(readSourceType : Value): DataFrame = {
    if (readSourceType == jdbc) {
      sqlContext.read.format("jdbc").options(sourceDb).load()
    } else {
      null
    }
  }

  /**
    * 处理原数据的自定义的方法
    * 默认可以通过配置实现，如果需要自定义处理逻辑，可以再在子类中重载实现
    *
    * @param sourceDf
    * @return
    */
  def filterSource(sourceDf: DataFrame): DataFrame = {
      val filtered = sourceDf.selectExpr(columns.getSourceColumns.map(
        s => if (sourceColumnMap.contains(s))
          sourceColumnMap(s) + " as " + s
        else s
      ): _*)
      if (sourceFilterWhere != null) filtered.where(sourceFilterWhere) else filtered
  }

  /**
    * 用来备份维度数据，然后将维度数据生成在临时目录，当isOnline参数为true的时候，将临时目录的数据替换线上维度
    *
    * @param args the main args
    * @param df   the DataFrame from execute function
    * @return a Unit.
    */
  def backup(args: Array[String], df: DataFrame, dimensionType: String): Unit = {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val cal = Calendar.getInstance
        val date = DateFormatUtils.readFormat.format(cal.getTime)
        val onLineDimensionDir = DIMENSION_HDFS_BASE_PATH + File.separator + dimensionType
        val onLineDimensionBackupDir = DIMENSION_HDFS_BASE_PATH_BACKUP + File.separator + date + File.separator + dimensionType
        val onLineDimensionDirTmp = DIMENSION_HDFS_BASE_PATH_TMP + File.separator + dimensionType
        println("onLineDimensionDir:" + onLineDimensionDir)
        println("onLineDimensionBackupDir:" + onLineDimensionBackupDir)
        println("onLineDimensionDirTmp:" + onLineDimensionDirTmp)


        val isFirstRun = HdfsUtil.IsDirExist(onLineDimensionDir)
        if (!isFirstRun) {
          val isBackupExist = HdfsUtil.IsDirExist(onLineDimensionBackupDir)
          if (isBackupExist) {
            println("数据已经备份,跳过备份过程")
          } else {
            println("生成线上维度备份数据:" + onLineDimensionBackupDir)
            val isSuccessBackup = HdfsUtil.copyFilesInDir(onLineDimensionDir, onLineDimensionBackupDir)
            println("备份数据状态:" + isSuccessBackup)
          }
        }

        //防止文件碎片
        df.persist(StorageLevel.MEMORY_AND_DISK)
        val total_count= BigDecimal(df.collect().size)
        val partition=Math.max(1,(total_count/THRESHOLD_VALUE).intValue())
        println("repartition:"+partition)

        val isTmpExist = HdfsUtil.IsDirExist(onLineDimensionDirTmp)
        if (isTmpExist) {
          println("删除线上维度临时数据:" + onLineDimensionDirTmp)
          HdfsUtil.deleteHDFSFileOrPath(onLineDimensionDirTmp)
        }
        println("生成线上维度数据到临时目录:" + onLineDimensionDirTmp)
        df.repartition(partition).write.parquet(onLineDimensionDirTmp)

        println("数据是否上线:" + p.isOnline)
        if (p.isOnline) {
          println("数据上线:" + onLineDimensionDir)
          println("删除线上维度数据:" + onLineDimensionDir)
          HdfsUtil.deleteHDFSFileOrPath(onLineDimensionDir)
          val isSuccess = HdfsUtil.copyFilesInDir(onLineDimensionDirTmp, onLineDimensionDir)
          println("数据上线状态:" + isSuccess)
        }
      }
      case None => {
        throw new RuntimeException("parameters wrong")
      }
    }
  }
}

object DimensionBase {
  val defaultValidTime: Date = DateUtils.parseDate("2000-01-01", "yyyy-MM-dd")
}
