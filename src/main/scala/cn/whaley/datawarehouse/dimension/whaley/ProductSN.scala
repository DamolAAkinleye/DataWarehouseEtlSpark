package cn.whaley.datawarehouse.dimension.whaley

import java.lang.{Long => JLong}

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.util.MysqlDB
import org.apache.spark.sql.DataFrame

/**
  * Created by huanghu on 17/3/14.
  *
  * 电视productSN维度表
  */
object ProductSN extends DimensionBase {

  columns.skName = "product_sn_sk"
  columns.primaryKeys = List("product_sn")
  columns.trackingColumns = List("rom_version")
  columns.allColumns = List("product_sn","product_line", "product_model", "user_id", "rom_version", "mac", "open_time", "wifi_mac", "ip", "vip_type", "country", "area", "province", "city","district", "isp", "city_level","prefecture_level_city")


  sourceDb = MysqlDB.whaleyTerminalMember


  dimensionName = "dim_whaley_product_sn"

  override def filterSource(sourceDf: DataFrame): DataFrame = {


    sourceDf.registerTempTable("mtv_terminal")

    //得出用户的信息，status表示用户是否有效，activate_status表示用户是否激活
    //限制id != 1339211 是因为有一条重复数据先去掉（此处为临时解决方案）
    sqlContext.sql("select  serial_number, service_id,rom_version, mac, " +
      "open_time, wifi_mac,current_ip" +
      s" from mtv_terminal where status =1 and activate_status =1 and serial_number not like 'XX%' and id !=1339211 and serial_number is not null and serial_number <> ''").registerTempTable("userinfo")

    //获取用户机型信息
    val productModulInfo = MysqlDB.dwDimensionDb("whaley_sn_to_productmodel")
    sqlContext.read.format("jdbc").options(productModulInfo).load().registerTempTable("whaley_sn_to_productmodel")
    sqlContext.sql("select serial_number , product_model  from whaley_sn_to_productmodel ").registerTempTable("productmodel")

    sqlContext.udf.register("snToProductLine", snToProductLine _)
    sqlContext.udf.register("getSNtwo", getSNtwo _)

    //将用户机型信息加入用户信息中存为中间表“product”
    sqlContext.sql("select a.serial_number as product_sn, snToProductLine(a.serial_number) as product_line ,b.product_model as product_model," +
      " a.service_id as user_id, a.rom_version as rom_version,a.mac as mac,a.open_time as open_time, a.wifi_mac as wifi_mac, a.current_ip as ip" +
      " from userinfo a left join productmodel b on getSNtwo(a.serial_number) = b.serial_number").registerTempTable("product")


    //获取用户的会员信息
    val vipTypeInfo = MysqlDB.whaleyDolphin("dolphin_club_authority", "id", 1, 100000000, 100)

    sqlContext.read.format("jdbc").options(vipTypeInfo).load().registerTempTable("dolphin_club_authority")
    sqlContext.sql("select distinct sn , 'vip' as  vip  from dolphin_club_authority where sn not like 'XX%'").registerTempTable("vipType")


    //将用户会员信息加入用户信息中作为中间表。表明为"userVipInfo"
    sqlContext.sql("select a.product_sn as product_sn, a.product_line as product_line ,a.product_model as product_model," +
      " a.user_id as user_id, a.rom_version as rom_version,a.mac as mac,a.open_time as open_time, a.wifi_mac as wifi_mac, a.ip as ip," +
      "case b.vip when 'vip' then 'vip' else '未知' end  as vip_type from product a left join vipType b on a.product_sn = b.sn")
      .registerTempTable("userVipInfo")

    //获取用户的城市信息

    sqlContext.udf.register("getFirstIpInfo", getFirstIpInfo _)
    sqlContext.udf.register("getSecondIpInfo", getSecondIpInfo _)
    sqlContext.udf.register("getThirdIpInfo", getThirdIpInfo _)

    sqlContext.read.parquet("/data_warehouse/dw_dimensions/dim_web_location").registerTempTable("log_data")
    sqlContext.sql("select ip_section_1,ip_section_2,ip_section_3,country,area,province,city,district,isp,city_level,prefecture_level_city,dim_invalid_time from log_data").registerTempTable("countryInfo")

    sqlContext.sql("select a.product_sn as product_sn, a.product_line as product_line ,a.product_model as product_model," +
      " a.user_id as user_id, a.rom_version as rom_version,a.mac as mac,a.open_time as open_time, a.wifi_mac as wifi_mac, a.ip as ip," +
      "a.vip_type  as vip_type ,case when b.country = '' then '未知' when  b.country is null  then '未知' else b.country end  as country," +
      "case when b.area = '' then '未知' when b.area is null then '未知' else b.area end  as area," +
      "case when b.province = '' then '未知' when b.province is null then '未知' else b.province end as province," +
      "case when b.city = '' then '未知' when b.city is null then '未知' else b.city end as city," +
      "case when b.district = '' then '未知' when b.district is null then '未知' else b.district end as district," +
      "case when b.isp = '' then '未知' when b.isp is null then '未知' else b.isp end as isp," +
      "case when b.city_level = '' then '未知' when b.city_level is null then '未知' else b.city_level end as city_level ," +
      "case when b.prefecture_level_city = '' then '未知' when b.prefecture_level_city is null then '未知' else b.prefecture_level_city end as prefecture_level_city" +
      " from userVipInfo a left join countryInfo b on getFirstIpInfo(a.ip) = b.ip_section_1" +
      " and getSecondIpInfo(a.ip) = b.ip_section_2 and getThirdIpInfo(a.ip) = b.ip_section_3 and b.dim_invalid_time is null")


  }

  def snToProductLine(productSN: String) = {
    if (productSN == null || productSN == "") {
      "helios"
    } else if (productSN.startsWith("P")) {
      "orca"
    } else "helios"
  }


  /**
    * 由版本号映射出对应的版本
    *
    * @param sn
    * @return 版本
    */

  def getSNtwo(sn: String) = {
    if (sn != null) {
      if (sn.length() >= 10) {
        val snPrefix = sn.substring(0, 2)
        snPrefix
      } else null
    } else null
  }

  def getFirstIpInfo(ip: String) = {
    var firstip = -1
    if (ip != null) {
      val ipinfo = ip.split("\\.")
      if (ipinfo.length >= 3) {
        firstip = ipinfo(0).trim().toInt
      }
    }
    firstip
  }

  def getSecondIpInfo(ip: String) = {
    var secondip = -1
    if (ip != null) {
      val ipinfo = ip.split("\\.")
      if (ipinfo.length >= 3) {
        secondip = ipinfo(1).trim().toInt
      }
    }
    secondip
  }

  def getThirdIpInfo(ip: String) = {
    var thirdip = -1
    if (ip != null) {
      val ipinfo = ip.split("\\.")
      if (ipinfo.length >= 3) {
        thirdip = ipinfo(2).trim().toInt
      }
    }
    thirdip
  }

}
