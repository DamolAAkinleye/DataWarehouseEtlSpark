package cn.whaley.datawarehouse.dimension.share

import cn.whaley.datawarehouse.BaseClass
import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.util.{HdfsUtil, MysqlDB}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.dimension.moretv.Subject._

/**
  * Created by Tony on 16/12/23.
  *
  * web地址维度生成ETL
  *
  * 无需自动触发，只需要修改后手动执行一次
  *
  * updated by wu.jiulin on 17/04/20
  * 修改city_info的表结构和数据
  * 维度表增加prefecture_level_city(地级市)字段
  */
object WebLocation extends DimensionBase {

  dimensionName = "dim_web_location"

  columns.skName = "web_location_sk"

  columns.primaryKeys = List("web_location_key")

  columns.trackingColumns = List()

  columns.allColumns = List(
    "web_location_key",
    "ip_section_1",
    "ip_section_2",
    "ip_section_3",
    "country",
    "area",
    "province",
    "city",
    "district",
    "city_level",
    "longitude",
    "latitude",
    "isp",
    "prefecture_level_city"
  )


  readSourceType = custom


  override def readSource(readSourceType: SourceType): DataFrame = {
    val countryBlackList = List("保留地址", "骨干网")

    val ipDataRdd = sc.textFile("hdfs://hans/log/ipLocationData/ip_country.txt", 10)
      .map(r => r.split("\t")).map(r => {
      val ip = r(0).split("\\.")
      Row(ip(0).trim().toLong * 256 * 256 * 256 + ip(1).trim().toLong * 256 * 256 + ip(2).trim().toLong * 256,
        ip(0).trim().toInt, ip(1).trim().toInt, ip(2).trim().toInt,
        r(2), r(3), r(4), r(5), r(8).trim.toDouble, r(9).trim.toDouble)
    })

    val schema = new StructType(Array(
      StructField("web_location_key", LongType),
      StructField("ip_section_1", IntegerType),
      StructField("ip_section_2", IntegerType),
      StructField("ip_section_3", IntegerType),
      StructField("country", StringType),
      StructField("province", StringType),
      StructField("city", StringType),
      StructField("district", StringType),
      StructField("longitude", DoubleType),
      StructField("latitude", DoubleType)
    ))

    val ipDataDf = sqlContext.createDataFrame(ipDataRdd, schema)
    ipDataDf.registerTempTable("a")

    val schema2 = new StructType(Array(
      StructField("web_location_key", LongType),
      StructField("ip_section_1", IntegerType),
      StructField("ip_section_2", IntegerType),
      StructField("ip_section_3", IntegerType),
      StructField("country", StringType),
      StructField("province", StringType),
      StructField("city", StringType),
      StructField("district", StringType),
      StructField("isp", StringType)
    ))

    val ipDataRdd2 = sc.textFile("hdfs://hans/log/ipLocationData/mydata4vipday2.txt", 100)
      .map(r => r.split("\t")).map(r => {
      r.map(s => {
        if (s.trim.equals("*")) ""
        else s
      })
    }).filter(r => {
      var containBlack = false
      for (black <- countryBlackList) {
        if (r(2).indexOf(black) > -1) containBlack = true
      }
      !containBlack
    }).filter(r => {
      val startIp = r(0).split("\\.")
      val endIp = r(1).split("\\.")
      if (startIp.size != 4 || endIp.size != 4) false
      else if (startIp(3).equals("000") && endIp(3).equals("255")) true
      else false
    }).flatMap(r => {
      val list = collection.mutable.Buffer[(Long, Int, Int, Int, String, String, String, String, String)]()
      val startIp = r(0).split("\\.")
      val endIp = r(1).split("\\.")
      val start_key = startIp(0).trim.toLong * 256 * 256 + startIp(1).trim.toLong * 256 + startIp(2).trim.toLong
      val end_key = endIp(0).trim.toLong * 256 * 256 + endIp(1).trim.toLong * 256 + endIp(2).trim.toLong

      for (key <- start_key to end_key) {
        val sec3 = key % 256
        val sec2 = key / 256 % 256
        val sec1 = key / 256 / 256
        val row = (key * 256, sec1.toInt, sec2.toInt, sec3.toInt,
          r(2), r(3), r(4), r(5), r(6))
        list.append(row)
      }
      list.toList
    }).map(r => Row.fromTuple(r))

    val ipDataDf2 = sqlContext.createDataFrame(ipDataRdd2, schema2)
    ipDataDf2.registerTempTable("b")

    sqlContext.sql("select if(a.web_location_key is null, b.web_location_key, a.web_location_key) as web_location_key," +
      "if(a.ip_section_1 is null, b.ip_section_1, a.ip_section_1) as ip_section_1, " +
      "if(a.ip_section_2 is null, b.ip_section_2, a.ip_section_2) as ip_section_2, " +
      "if(a.ip_section_3 is null, b.ip_section_3, a.ip_section_3) as ip_section_3, " +
      "if(a.country is null or a.country = '', b.country, a.country) as country, " +
      "if(a.province is null or a.province = '', b.province, a.province) as province, " +
      "if(a.city is null or a.city = '', b.city, a.city) as city, " +
      "if(a.district is null or a.district = '', b.district, a.district) as district, " +
      " a.longitude, a.latitude, b.isp " +
      " from a full join b on a.web_location_key = b.web_location_key " +
      " order by web_location_key")
      .select(
        "web_location_key", "ip_section_1", "ip_section_2", "ip_section_3",
        "country", "province", "city", "district", "longitude", "latitude", "isp"
      )


  }

  override def filterSource(sourceDf: DataFrame): DataFrame = {

    val sq = sqlContext
    sqlContext.udf.register("replaceStr",(_:String).replaceAll("市",""))
    import sq.implicits._
    import org.apache.spark.sql.functions._

//    val cityInfoDb = MysqlDB.dwDimensionDb("city_info")
//
//    val cityInfoDf = sqlContext.read.format("jdbc").options(cityInfoDb).load()
//      .select($"city", $"area", $"city_level")
//
//    cityInfoDf.join(sourceDf, "city" :: Nil, "rightouter")
//      .select(
//        columns.primaryKeys(0),
//        columns.allColumns: _*
//      )

    val cityInfoDb = MysqlDB.dwDimensionDb("city_info_new")
    val cityInfoDf = sqlContext.read.format("jdbc").options(cityInfoDb).load()

    cityInfoDf.registerTempTable("city_info")
    sourceDf.registerTempTable("ip_info")

    var sqlStr =
      s"""
        |select
        |a.web_location_key as ${columns.primaryKeys(0)},
        |a.ip_section_1 as ${columns.allColumns(1)},
        |a.ip_section_2 as ${columns.allColumns(2)},
        |a.ip_section_3 as ${columns.allColumns(3)},
        |a.country as ${columns.allColumns(4)},
        |b.area as ${columns.allColumns(5)},
        |a.province as ${columns.allColumns(6)},
        |b.city as ${columns.allColumns(7)},
        |a.district as ${columns.allColumns(8)},
        |b.city_level as ${columns.allColumns(9)},
        |a.longitude as ${columns.allColumns(10)},
        |a.latitude as ${columns.allColumns(11)},
        |a.isp as ${columns.allColumns(12)},
        |a.city as ${columns.allColumns(13)}
        |from ip_info a
        |left join
        |(
        |select t1.city,t1.area,t1.city_level,t1.executive_level,t1.prefecture_level_city from
        |city_info t1 left join city_info t2 on t1.city=t2.prefecture_level_city
        |where t2.prefecture_level_city is null
        |) b
        |on if(b.executive_level='县级市',replaceStr(a.district),a.city) = b.city
      """.stripMargin
    sqlContext.sql(sqlStr).registerTempTable("tmp_table")
    /**
      * city_info表的city字段没有 区 的值,如浦东新区,和ip库关联不上导致最终结果有 区 的记录 area,city,city_info三个字段为空
      * 拿上面结果的prefecture_level_city关联city_info的city补上结果
      */
    sqlStr =
      s"""
        |select
        |a.${columns.primaryKeys(0)},
        |a.${columns.allColumns(1)},
        |a.${columns.allColumns(2)},
        |a.${columns.allColumns(3)},
        |a.${columns.allColumns(4)},
        |if(a.${columns.allColumns(5)} is null,b.area,a.${columns.allColumns(5)}) as ${columns.allColumns(5)},
        |a.${columns.allColumns(6)},
        |if(a.${columns.allColumns(7)} is null,b.city,a.${columns.allColumns(7)}) as ${columns.allColumns(7)},
        |a.${columns.allColumns(8)},
        |if(a.${columns.allColumns(9)} is null,b.city_level,a.${columns.allColumns(9)}) as ${columns.allColumns(9)},
        |a.${columns.allColumns(10)},
        |a.${columns.allColumns(11)},
        |a.${columns.allColumns(12)},
        |a.${columns.allColumns(13)}
        |from tmp_table a left join city_info b
        |on a.prefecture_level_city=b.city
      """.stripMargin

    sqlContext.sql(sqlStr)

  }
}
