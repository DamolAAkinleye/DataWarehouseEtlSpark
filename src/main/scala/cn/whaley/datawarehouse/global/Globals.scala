package cn.whaley.datawarehouse.global

/**
  * Created by Tony on 17/4/5.
  */
object Globals {

  val HDFS_BASE_PATH = "/data_warehouse"
  val DIMENSION_HDFS_BASE_PATH: String = HDFS_BASE_PATH + "/dw_dimensions"
  val FACT_HDFS_BASE_PATH: String = HDFS_BASE_PATH + "/dw_facts"

}
