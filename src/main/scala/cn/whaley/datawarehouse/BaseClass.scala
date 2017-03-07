package cn.whaley.datawarehouse

import java.util.Calendar

import cn.whaley.datawarehouse.global.LogConfig
import cn.whaley.datawarehouse.util.{HdfsUtil, DateFormatUtils, ParamsParseUtil}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

import scala.reflect.io.File

/**
  * Created by Tony on 16/12/21.
  */
trait BaseClass extends LogConfig {

  /**
    * define some parameters
    */
  var sc: SparkContext = null
  var hiveContext:HiveContext = null
  implicit var sqlContext: SQLContext = null
  val config = new SparkConf()
//    .set("spark.executor.memory", "4g")
//    .set("spark.executor.cores", "3")
    .set("spark.scheduler.mode", "FAIR")
    .set("spark.eventLog.enabled", "true")
    .set("spark.eventLog.dir", "hdfs://hans/spark-log/spark-events")
//    .set("spark.cores.max", "72")
    .set("spark.driver.maxResultSize", "2g")
//    .setAppName(this.getClass.getSimpleName)


  def main(args: Array[String]) {
    System.out.println("init start ....")
    init()
    System.out.println("init success ....")

    println("execute start ....")
    val result= execute(args)
    println("execute end ....")

    println("backup start ....")
    backup(args,result._1,result._2)
    println("backup end ....")

  }

  /**
    * initialize global parameters
    */
  def init() = {
    sc = new SparkContext(config)
    sqlContext = SQLContext.getOrCreate(sc)

//    hiveContext = new HiveContext(sc)
//    DataIO.init("hdfs://hans/test/config.json")
  }

  /**
    * 用来备份维度数据，然后将维度数据生成在临时目录，最终将临时目录的数据替换线上维度
    * @param args the main args
    * @param df the DataFrame from execute function
    * @return a Unit.
    */
  def backup(args: Array[String],df:DataFrame,dimensionType:String): Unit ={
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val cal = Calendar.getInstance
        val date = DateFormatUtils.readFormat.format(cal.getTime)
        val onLineDimensionDir=DIMENSION_HDFS_BASE_PATH+File.pathSeparator+dimensionType
        val onLineDimensionBackupDir = DIMENSION_HDFS_BASE_PATH+File.pathSeparator+date+File.pathSeparator+dimensionType
        val onLineDimensionDirTmp=s"${onLineDimensionDir}_tmp"
        println("onLineDimensionDir:" + onLineDimensionDir)
        println("onLineDimensionBackupDir:" + onLineDimensionBackupDir)
        println("onLineDimensionDirTmp:" + onLineDimensionDirTmp)

        val isBackupExist = HdfsUtil.IsDirExist(onLineDimensionBackupDir)
        if(isBackupExist){
          println("删除线上维度备份数据:" + onLineDimensionBackupDir)
          HdfsUtil.deleteHDFSFileOrPath(onLineDimensionBackupDir)
        }
        println("生成线上维度备份数据:" + onLineDimensionBackupDir)
        val isSuccessBackup=HdfsUtil.copyFilesInDir(onLineDimensionDir,onLineDimensionBackupDir)
        println("备份数据状态:"+isSuccessBackup)

        val isTmpExist = HdfsUtil.IsDirExist(onLineDimensionDirTmp)
        if(isTmpExist){
          println("删除线上维度临时数据:" + onLineDimensionDirTmp)
          HdfsUtil.deleteHDFSFileOrPath(onLineDimensionDirTmp)
        }
        println("生成线上维度数据到临时目录:" + onLineDimensionDirTmp)
        df.write.parquet(onLineDimensionDirTmp)

        println("数据上线:" + onLineDimensionDir)
        if (p.deleteOld) {
          println("删除线上维度数据:" + onLineDimensionDir)
          HdfsUtil.deleteHDFSFileOrPath(onLineDimensionDir)
        }
        val isSuccess=HdfsUtil.copyFilesInDir(onLineDimensionDirTmp,onLineDimensionDir)
        println("数据上线状态:"+isSuccess)
      }
      case None => {
        throw new RuntimeException("At least need param --dimensionType.")
      }
    }
  }

  /**
    * this method do not complete.Sub class that extends BaseClass complete this method
    */
  def execute(args: Array[String]):(DataFrame,String)

  /**
    * release resource
    */
  def destroy() = {
    if (sc != null) {
      sqlContext.clearCache()
      sc.stop()
    }
  }
}
