package cn.whaley.datawarehouse.dimension.moretv

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB
import org.apache.spark.sql.DataFrame


/**
  * Created by witnes on 3/13/17.
  *
  * 电视猫_节目专题维度表
  */
object Subject extends DimensionBase {

  dimensionName = "dim_medusa_subject"

  columns.skName = "subject_sk"

  columns.primaryKeys = List("subject_code")

  columns.trackingColumns = List("subject_name")

  columns.allColumns = List(
    "subject_code",
    "subject_name",
    //    "subject_title",
    "subject_content_type",
    "subject_content_type_name",
    "subject_create_time",
    "subject_publish_time"
  )

  readSourceType = jdbc

  sourceDb = MysqlDB.medusaCms("mtv_subject", "ID", 1, 4000, 5)


  /**
    * 处理原数据的自定义的方法
    * 默认可以通过配置实现，如果需要自定义处理逻辑，可以再在子类中重载实现
    *
    * @param sourceDf
    * @return
    */
  override def filterSource(sourceDf: DataFrame): DataFrame = {

    val sq = sqlContext
    import org.apache.spark.sql.functions._
    import sq.implicits._

    val contentTypeDb = MysqlDB.medusaCms("mtv_content_type", "id", 1, 100, 1)

    val contentTypeDf = sqlContext.read.format("jdbc").options(contentTypeDb).load()
      .select($"code", $"name")

    sourceDf.filter($"code".isNotNull && $"code".notEqual("")
      && $"name".isNotNull && $"name".notEqual("")
      && $"status".notEqual(-1))
      .withColumn("codev", regexp_extract($"code", "[a-z]*", 0)).as("s")

      .join(contentTypeDf.as("c"), $"s.codev" === $"c.code", "left_outer")
      .select(
        $"s.code".as("subject_code"),
        $"s.name".as("subject_name"),
        $"s.title".as("subject_title"),
        $"c.code".as("subject_content_type"),
        $"c.name".as("subject_content_type_name"),
        $"s.create_time".cast("timestamp").as("subject_create_time"),
        $"s.publish_time".cast("timestamp").as("subject_publish_time")
      )

  }
}
