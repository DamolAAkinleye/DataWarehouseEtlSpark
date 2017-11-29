package cn.whaley.datawarehouse.dimension.moretv

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.util.MysqlDB
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.storage.StorageLevel

/**
  * Created by Tony on 17/3/10.
  *
  * 电视猫节目维度表
  */
object Program extends DimensionBase {

  columns.skName = "program_sk"
  columns.primaryKeys = List("sid")
  columns.trackingColumns = List()
  columns.allColumns = List("sid", "title", "content_type", "content_type_name", "duration", "video_type", "episode_index",
    "status", "type", "parent_sid", "area", "year", "video_length_type", "create_time", "publish_time","supply_type",
    "package_code", "package_name", "is_vip", "source")

  val vipProgramColumns = List("program_code","package_code","package_name")

  sourceDb = MysqlDB.medusaCms("mtv_basecontent", "id", 1, 2010000000, 500)
  val contentProgramDB = MysqlDB.medusaMemberDB("content_program")
  val contentPackageProgramDB = MysqlDB.medusaMemberDB("content_package_program_relation")


  dimensionName = "dim_medusa_program"

  override def readSource(readSourceType: _root_.cn.whaley.datawarehouse.global.SourceType.Value): DataFrame = {
    val programDF = sqlContext.read.format("jdbc").options(sourceDb).load()
    val contentProgram = sqlContext.read.format("jdbc").options(contentProgramDB).load()
    val tvbBBCPackageProgram = sqlContext.read.format("jdbc").options(contentPackageProgramDB).load().filter("relation_status = 'bound'")
      .persist(StorageLevel.MEMORY_AND_DISK)
    val tvbPackageProgramDF = tvbBBCPackageProgram.filter("package_code = 'TVB'").select(vipProgramColumns.map(col):_*)
    val bbcPackageProgramDF = tvbBBCPackageProgram.filter("package_code = 'BBC'").select(vipProgramColumns.map(col):_*)
    tvbBBCPackageProgram.unpersist()

    val tencentPackageProgram = contentProgram.filter("source = 'tencent2'")
    val tencentProgramDF = tencentPackageProgram.select("program_code").
      withColumn("package_code",lit("TENCENT")).
      withColumn("package_name", lit("腾讯节目包")).
      select(vipProgramColumns.map(col):_*)
    val vipProgramDF = tencentProgramDF.union(tvbPackageProgramDF).union(bbcPackageProgramDF)
    programDF.join(vipProgramDF,programDF("sid") === vipProgramDF("program_code"),"left_outer").
      withColumn("is_vip", if(col("package_code") != null) lit(1) else lit(0))
  }

  override def filterSource(sourceDf: DataFrame): DataFrame = {
//    sourceDf.persist()

    sqlContext.udf.register("myReplace",myReplace _)

    sourceDf.registerTempTable("mtv_basecontent")

    val programDf = sqlContext.sql("select sid, id, display_name, content_type, " +
      " duration, parent_id, video_type, type, " +
      "(case when status = 1 and origin_status = 1 then 1 else 0 end) status, " +
      " episode, area, year, " +
      " videoLengthType, create_time, publish_time, supply_type ,package_code, package_name, is_vip,source" +
      " from mtv_basecontent where sid is not null and sid <> '' and display_name is not null ")

    programDf.persist()
    programDf.registerTempTable("program_table")

    val contentTypeDb = MysqlDB.medusaCms("mtv_content_type", "id", 1, 100, 1)

    sqlContext.read.format("jdbc").options(contentTypeDb).load().registerTempTable("content_type")

    sqlContext.sql("SELECT a.sid, b.sid as parent_sid, myReplace(a.display_name) as title, a.status, a.type, " +
      "a.content_type, c.name as content_type_name, a.duration, a.video_type, a.episode as episode_index, " +
      "a.area, a.year, a.videoLengthType as video_length_type,a.supply_type, a.package_code, a.package_name, a.is_vip, " +
      "a.source,a.create_time, " +
      "a.publish_time " +
      " from program_table a" +
      " left join program_table b on a.parent_id = b.id" +
      " left join content_type c on a.content_type = c.code " +
      " where a.sid is not null and a.sid <> ''" +
      " ORDER BY a.id").dropDuplicates(List("sid"))
  }

  def myReplace(s:String): String ={
    var t = s
    t = t.replace("'", "")
    t = t.replace("\t", " ")
    t = t.replace("\r\n", "-")
    t = t.replace("\r", "-")
    t = t.replace("\n", "-")
    t
  }
}
