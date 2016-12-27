package cn.whaley.datawarehouse.dimentions

import cn.whaley.datawarehouse.BaseClass
import cn.whaley.datawarehouse.util.HdfsUtil
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
  * Created by Tony on 16/12/23.
  *
  * web地址维度生成ETL
  *
  * 无需自动触发，只需要修改后手动执行一次
  */
object WebLocation extends BaseClass {

  val countryBlackList = List("保留地址", "骨干网")

  override def execute(args: Array[String]): Unit = {

    val ipDataRdd = sc.textFile("hdfs://hans/log/ipLocationData/ip_country.txt", 10).map(r => r.split("\t")).map(r => {
      val ip = r(0).split("\\.")
      Row(ip(0).trim().toInt * 256 * 256 + ip(1).trim().toInt * 256 + ip(2).trim().toInt,
        ip(0).trim().toInt, ip(1).trim().toInt, ip(2).trim().toInt,
        r(2), r(3), r(4), r(5), r(8).trim.toDouble, r(9).trim.toDouble)
    })

    val schema = new StructType(Array(
      StructField("web_location_sk", IntegerType),
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
      StructField("web_location_sk", IntegerType),
      StructField("ip_section_1", IntegerType),
      StructField("ip_section_2", IntegerType),
      StructField("ip_section_3", IntegerType),
      StructField("country", StringType),
      StructField("province", StringType),
      StructField("city", StringType),
      StructField("district", StringType),
      StructField("isp", StringType)
    ))

    val ipDataRdd2 = sc.textFile("hdfs://hans/log/ipLocationData/mydata4vipday2.txt", 100).map(r => r.split("\t")).map(r => {
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
      val list = collection.mutable.Buffer[(Int, Int, Int, Int, String, String, String, String, String)]()
      val startIp = r(0).split("\\.")
      val endIp = r(1).split("\\.")
      val start_key = startIp(0).trim.toInt * 256 * 256 + startIp(1).trim.toInt * 256 + startIp(2).trim.toInt
      val end_key = endIp(0).trim.toInt * 256 * 256 + endIp(1).trim.toInt * 256 + endIp(2).trim.toInt

      for (key <- start_key to end_key) {
        val sec3 = key % 256
        val sec2 = key / 256 % 256
        val sec1 = key / 256 / 256
        val row = (key, sec1, sec2, sec3,
          r(2), r(3), r(4), r(5), r(6))
        list.append(row)
      }
      list.toList
    }).map(r => Row.fromTuple(r))

    val ipDataDf2 = sqlContext.createDataFrame(ipDataRdd2, schema2)
    ipDataDf2.registerTempTable("b")

    val df = sqlContext.sql("select if(a.web_location_sk is null, b.web_location_sk, a.web_location_sk) as web_location_sk," +
      "if(a.ip_section_1 is null, b.ip_section_1, a.ip_section_1) as ip_section_1, " +
      "if(a.ip_section_2 is null, b.ip_section_2, a.ip_section_2) as ip_section_2, " +
      "if(a.ip_section_3 is null, b.ip_section_3, a.ip_section_3) as ip_section_3, " +
      "if(a.country is null or a.country = '', b.country, a.country) as country, " +
      "'' as area, " +
      "if(a.province is null or a.province = '', b.province, a.province) as province, " +
      "if(a.city is null or a.city = '', b.city, a.city) as city, " +
      "if(a.district is null or a.district = '', b.district, a.district) as district, " +
      "'' as city_level, a.longitude, a.latitude, b.isp " +
      " from a full join b on a.web_location_sk = b.web_location_sk " +
      " order by web_location_sk")

    HdfsUtil.deleteHDFSFileOrPath("/data_warehouse/dimensions/web_location")
    df.write.parquet("/data_warehouse/dimensions/web_location")

  }
}
