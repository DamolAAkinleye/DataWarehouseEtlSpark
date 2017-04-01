package cn.whaley.datawarehouse.dimension

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import cn.whaley.datawarehouse.BaseClass
import cn.whaley.datawarehouse.dimension.constant.Constants._
import cn.whaley.datawarehouse.dimension.constant.SourceType._
import cn.whaley.datawarehouse.util.{DataFrameUtil, DateFormatUtils, HdfsUtil, ParamsParseUtil}
import org.apache.commons.lang3.time.DateUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
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
    * 应使用维度表字段名
    */
  var sourceFilterWhere: String = _

  /**
    * 维度表字段与源数据字段的对应关系，仅当filterSource方法未在子类中重载时有效
    * key是维度表字段名，value是数据源中获取方式，支持spark sql表达
    */
  var sourceColumnMap: Map[String, String] = Map()

  var debug = false

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

    //过滤后源数据主键唯一性判断和处理
    checkPrimaryKeys(filteredSourceDf, columns.primaryKeys)

    println("成功获取源数据")
    if(debug) filteredSourceDf.show

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

    println("成功获取现有维度")
    if(debug) originalDf.show

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
            columns.trackingColumns.map(s => s"a.$s != b.$s").mkString(" or ") //若trackingColumn原本为null，不增加新行
          ).selectExpr(columns.getSourceColumns.map(s => s"b.$s"): _*)
        )
      }

    println("计算完成需要增加的行")
    if(debug) extendDf.show

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val todayStr = sdf.format(today)


    //现有维度表中已经存在的行，已经根据现有源信息做了字段更新，更新了因源数据删除导致的失效，但是未更新因为追踪历史记录的列变化导致的失效
    //如果维度表中已经存在的业务键在源信息中被删除了，则会保留维度表中的值
    //若trackingColumn原本为null，源数据有值后，会在直接在该主键的所有行上变更，也就是说，历史记录默认不记录null值
    val originalExistDf = originalDf.as("a").join(
      filteredSourceDf.as("b"),
      columns.primaryKeys.map(s => originalDf(s) === filteredSourceDf(s)).reduceLeft(_ && _),
      "leftouter"
    ).selectExpr(
      List("a." + columns.skName) //++ columns.primaryKeys
        //        ++ columns.trackingColumns.map(s => "CASE WHEN a." + s + " is not null THEN a." + s + " ELSE b." + s + " END as " + s)
        //        ++ columns.allColumns.map(s => "b." + s)
        ++ columns.getSourceColumns.map(s => {
        if (columns.primaryKeys.contains(s)) s"a.$s"
        else if (columns.trackingColumns.contains(s)) s"CASE WHEN a.$s is not null THEN a.$s ELSE b.$s END as $s"
        else "CASE WHEN b." + columns.primaryKeys.head + s" is not null THEN b.$s ELSE a.$s END as $s"
      })
        ++ List(columns.validTimeKey).map(s => "a." + s)
        ++ List(columns.invalidTimeKey).map(s =>
        "CASE WHEN b." + columns.primaryKeys.head + s" is null and a.$s is null THEN '$todayStr' ELSE a.$s END as $s")
        : _*
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

        println("计算完成需要变更失效时间的行")
        if(debug) invalidColumnsDf.show

        //更新失效时间
        originalExistDf.as("origin").join(invalidColumnsDf.as("invalid"), List(columns.skName), "leftouter"
        ).selectExpr(
          List(columns.skName) ++ columns.getSourceColumns
            ++ List(columns.validTimeKey)
            ++ List("CASE WHEN invalid." + columns.invalidTimeKey + " is not null THEN invalid." + columns.invalidTimeKey
            + " ELSE origin." + columns.invalidTimeKey + " END as " + columns.invalidTimeKey): _*
        )
      }

    println("计算完成原有维度数据更新后")
    if(debug) df.show

    //合并上述形成最终结果
    val offset =
      if (df.count() > 0) {
        df.agg(max(columns.skName)).first().getLong(0)
      } else {
        println("WARN! 原维度表为空")
        0
      }

    val result = df.unionAll(
      DataFrameUtil.dfZipWithIndex(
        DataFrameUtil.addDimTime(extendDf, today, null)
        , columns.skName
        , offset
      )
    )

    println("计算完成最终生成的新维度")
    if(debug) result.show

    result
  }

  /**
    * 自定义的源数据读取方法
    * 默认是从jdbc中读取，如需自定义，可以在子类中重载实现
    *
    * @return
    */
  def readSource(readSourceType: Value): DataFrame = {
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

  def checkPrimaryKeys(sourceDf: DataFrame, primaryKeys: List[String]): DataFrame = {

    val primaryKeyNullCount = sourceDf.where(primaryKeys.map(s => s + " is null").mkString(" or ")).count
    if (primaryKeyNullCount > 0){
      throw new RuntimeException("存在业务主键是null")
    }

    val duplicatePkDf = sourceDf.groupBy(
      primaryKeys.map(s => col(s)): _*
    ).agg(
      count("*").as("count")
    ).where("count > 1")

    //当前在出现重复主键时直接报错
    val duplicatePkCount = duplicatePkDf.count()
    if (duplicatePkCount > 0) {
      duplicatePkDf.dropDuplicates(primaryKeys).selectExpr(primaryKeys: _*).show
      throw new RuntimeException("存在重复业务主键" + duplicatePkCount + "个！部分展示如上")
    }
    sourceDf
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
        val onLineDimensionDirDelete = DIMENSION_HDFS_BASE_PATH_DELETE + File.separator + dimensionType
        println("线上数据目录:" + onLineDimensionDir)
        println("线上数据备份目录:" + onLineDimensionBackupDir)
        println("线上数据临时目录:" + onLineDimensionDirTmp)
        println("线上数据等待删除目录:" + onLineDimensionDirDelete)

        df.persist(StorageLevel.MEMORY_AND_DISK)
        val isOnlineFileExist = HdfsUtil.IsDirExist(onLineDimensionDir)
        if (isOnlineFileExist) {
          val isBackupExist = HdfsUtil.IsDirExist(onLineDimensionBackupDir)
          if (isBackupExist) {
            println("数据已经备份,跳过备份过程")
          } else {
            println("生成线上维度备份数据:" + onLineDimensionBackupDir)
            val isSuccessBackup = HdfsUtil.copyFilesInDir(onLineDimensionDir, onLineDimensionBackupDir)
            println("备份数据状态:" + isSuccessBackup)
          }
        } else {
          println("无可用备份数据")
        }

        //防止文件碎片
        val total_count = BigDecimal(df.count())
        val partition = Math.max(1, (total_count / THRESHOLD_VALUE).intValue())
        println("repartition:" + partition)

        val isTmpExist = HdfsUtil.IsDirExist(onLineDimensionDirTmp)
        if (isTmpExist) {
          println("删除线上维度临时数据:" + onLineDimensionDirTmp)
          HdfsUtil.deleteHDFSFileOrPath(onLineDimensionDirTmp)
        }
        println("生成线上维度数据到临时目录:" + onLineDimensionDirTmp)
        df.repartition(partition).orderBy(columns.skName).write.parquet(onLineDimensionDirTmp)

        println("数据是否上线:" + p.isOnline)
        if (p.isOnline) {
          println("数据上线:" + onLineDimensionDir)
          if (isOnlineFileExist) {
            println("移动线上维度数据:from " + onLineDimensionDir + " to " + onLineDimensionDirDelete)
            val isRenameSuccess = HdfsUtil.rename(onLineDimensionDir, onLineDimensionDirDelete)
            println("isRenameSuccess:" + isRenameSuccess)
          }

          val isOnlineFileExistAfterRename = HdfsUtil.IsDirExist(onLineDimensionDir)
          if (isOnlineFileExistAfterRename) {
            throw new RuntimeException("rename failed")
          } else {
            val isSuccess = HdfsUtil.rename(onLineDimensionDirTmp, onLineDimensionDir)
            println("数据上线状态:" + isSuccess)
          }
          println("删除过期数据:" + onLineDimensionDirDelete)
          HdfsUtil.deleteHDFSFileOrPath(onLineDimensionDirDelete)
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
