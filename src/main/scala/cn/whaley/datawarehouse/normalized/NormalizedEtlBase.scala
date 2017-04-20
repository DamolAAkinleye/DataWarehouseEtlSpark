package cn.whaley.datawarehouse.normalized

import cn.whaley.datawarehouse.BaseClass
import cn.whaley.datawarehouse.global.Globals._
import cn.whaley.datawarehouse.util.{HdfsUtil, Params}
import org.apache.spark.sql.DataFrame

import scala.reflect.io.File

/**
  * Created by Tony on 17/4/19.
  */
abstract class NormalizedEtlBase  extends BaseClass {

  var tableName: String = _


  override def load(params: Params, df: DataFrame): Unit = {
    HdfsUtil.deleteHDFSFileOrPath(NORMALIZED_TABLE_HDFS_BASE_PATH + File.separator + tableName + File.separator +  "current")
    df.write.parquet(NORMALIZED_TABLE_HDFS_BASE_PATH + File.separator + tableName + File.separator + "current")
  }
}
