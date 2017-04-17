package cn.whaley.datawarehouse.dimension.moretv.total

import cn.whaley.datawarehouse.BaseClass
import cn.whaley.datawarehouse.util.{HdfsUtil, MysqlDB, Params}
import org.apache.spark.sql.DataFrame

/**
  * Created by Tony on 17/1/6.
  *
  * 特殊路径来源维度，主要是编辑的聚合来源
  *
  * 包含专题subject, 明星star, 音乐电台station, 歌手singer, 音乐精选集omnibus, 音乐榜单
  */
object SourceSpecialTotal extends BaseClass {
  override def execute(params: Params): Unit = {

    sqlContext.udf.register("extractSubjectTypeFromCode",extractSubjectTypeFromCode _)

    //专题 id从100000000开始
    val subjectJdbcDF = sqlContext.read.format("jdbc")
      .options(MysqlDB.medusaCms("mtv_subject", "id", 1, 22351, 10))
      .load()
    subjectJdbcDF.registerTempTable("mtv_subject")

    val subjectDf = sqlContext.sql("SELECT cast (id + 100000000 as long) as source_special_sk, " +
      "'subject' as special_type, " +
      "code as special_code, " +
      "sid  as special_sid," +
      "name as special_name, " +
      "extractSubjectTypeFromCode(code) as special_content_type " +
      " FROM mtv_subject")

    //明星 id从200000000开始
    val starJdbcDF = sqlContext.read.format("jdbc")
      .options(MysqlDB.medusaCms("mtv_cast", "id", 140895, 3832831, 10))
      .load()
    starJdbcDF.registerTempTable("mtv_cast")

    val starDf = sqlContext.sql("SELECT cast (id + 200000000 as long) as source_special_sk, " +
      "'star' as special_type, " +
      "code as special_code, " +
      "sid  as special_sid," +
      "name as special_name, " +
      "'' as special_content_type " +
      " FROM mtv_cast")


    //音乐电台 id从300000000开始
    val stationJdbcDF = sqlContext.read.format("jdbc")
      .options(MysqlDB.medusaCms("mtv_mvradio", "id", 11, 381, 1))
      .load()
    stationJdbcDF.registerTempTable("mtv_mvradio")

    val stationDf = sqlContext.sql("SELECT cast (id + 300000000 as long) as source_special_sk, " +
      "'station' as special_type, " +
      "'' as special_code, " +
      "sid  as special_sid," +
      "title as special_name, " +
      "'' as special_content_type " +
      " FROM mtv_mvradio")


    //歌手 id从400000000开始
    val singerJdbcDF = sqlContext.read.format("jdbc")
      .options(MysqlDB.medusaCms("mtv_singer", "id", 1, 328821, 10))
      .load()
    singerJdbcDF.registerTempTable("mtv_singer")

    val singerDf = sqlContext.sql("SELECT cast (id + 400000000 as long) as source_special_sk, " +
      "'singer' as special_type, " +
      "'' as special_code, " +
      "sid  as special_sid," +
      "name as special_name, " +
      "'' as special_content_type " +
      " FROM mtv_singer")

    //音乐精选集 id从500000000开始
    val omnibusJdbcDF = sqlContext.read.format("jdbc")
      .options(MysqlDB.medusaCms("mtv_mvtopic", "id", 1, 7731, 3))
      .load()
    omnibusJdbcDF.registerTempTable("mtv_mvtopic")

    val omnibusDf = sqlContext.sql("SELECT cast (id + 500000000 as long) as source_special_sk, " +
      "'omnibus' as special_type, " +
      "'' as special_code, " +
      "sid  as special_sid," +
      "title as special_name, " +
      "'' as special_content_type " +
      " FROM mtv_mvtopic")

    //音乐榜单 id从600000000开始
    val topRankJdbcDF = sqlContext.read.format("jdbc")
      .options(MysqlDB.medusaCms("mtv_mvHotList_code", "id", 3, 41, 1))
      .load()
    topRankJdbcDF.registerTempTable("mtv_mvHotList_code")

    val topRankDf = sqlContext.sql("SELECT cast (id + 600000000 as long) as source_special_sk, " +
      "'topRank' as special_type, " +
      "code as special_code, " +
      "''  as special_sid," +
      "name as special_name, " +
      "'' as special_content_type " +
      " FROM mtv_mvHotList_code")


    val df = subjectDf.unionAll(starDf).unionAll(stationDf).unionAll(singerDf)
      .unionAll(omnibusDf).unionAll(topRankDf)

    HdfsUtil.deleteHDFSFileOrPath("/data_warehouse/dw_dimensions/dim_medusa_source_special")
    df.write.parquet("/data_warehouse/dw_dimensions/dim_medusa_source_special")
  }

  def extractSubjectTypeFromCode(subjectCode: String) :String = {
    val types = List("movie","tv","zongyi","comic","kids","jilu","mv","hot","xiqu","sports")
    val list = types.filter(eachType => subjectCode.indexOf(eachType) > -1)
    if (list.isEmpty) "" else list.head
  }

  /**
    * 源数据读取函数, ETL中的Extract
    * 如需自定义，可以在子类中重载实现
    *
    * @return
    */
  override def extract(params: Params): DataFrame = ???

  /**
    * 数据转换函数，ETL中的Transform
    *
    * @return
    */
  override def transform(params: Params, df: DataFrame): DataFrame = ???

  /**
    * 数据存储函数，ETL中的Load
    */
  override def load(params: Params, df: DataFrame): Unit = ???
}
