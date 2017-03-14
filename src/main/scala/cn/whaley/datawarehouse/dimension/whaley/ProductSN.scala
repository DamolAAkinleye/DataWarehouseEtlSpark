package cn.whaley.datawarehouse.dimension.whaley

import java.text.SimpleDateFormat
import java.util.Calendar
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

  columns.skName = "productSN_sk"
  columns.primaryKeys = List("productSN")
  columns.trackingColumns = List()
  //columns.otherColumns = List("productLine", "product_model","userId","rom_version","mac","open_time","wifi_mac","ip", "country", "province", "city", "isp", "leavel",
    //"vip_type")
  columns.otherColumns = List("productLine", "product_model","userId","rom_version","mac","open_time","wifi_mac","ip",
  "vip_type")

  sourceDb = MysqlDB.whaleyTerminalMember


  dimensionName = "dim_whaley_productSN"

  override def filterSource(sourceDf: DataFrame): DataFrame = {
    val s = sqlContext
    import s.implicits._
    val cal = Calendar.getInstance()
    cal.add(Calendar.DAY_OF_MONTH,-1)
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val date = format.format(cal.getTime)
    println(date)

    sourceDf.registerTempTable("mtv_terminal")

    sqlContext.sql("select  serial_number, service_id,rom_version, mac, " +
      "open_time, wifi_mac,current_ip"+
      s" from mtv_terminal where status =1 and activate_status =1 and serial_number not like 'XX%' and id !=1339211 and serial_number is not null and serial_number <> ''").registerTempTable("userinfo")



    val vipTypeInfo = MysqlDB.whaleyDolphin("dolphin_club_authority", "id", 1, 100000000, 100)

    sqlContext.read.format("jdbc").options(vipTypeInfo).load().registerTempTable("dolphin_club_authority")
     sqlContext.sql("select distinct sn , 'vip' as  vip  from dolphin_club_authority where sn not like 'XX%'").registerTempTable("viptype")


     //totalRdd.toDF("productSN","product_model","userId","rom_version","mac","open_time","wifi_mac","ip", "country","province", "city", "isp", "leavel","vip_type" )
    //totalRdd.toDF("productSN","productLine","product_model","userId","rom_version","mac","open_time","wifi_mac","ip","vip_type" )
    sqlContext.udf.register("snToProductLine",snToProductLine _)
    sqlContext.udf.register("pmMapping",pmMapping _)

    sqlContext.sql("select a.serial_number as productSN, snToProductLine(a.serial_number) as productLine ,pmMapping(a.serial_number) as product_model,"+
                   " a.service_id as userId, a.rom_version as rom_version,a.mac as mac,a.open_time as open_time, a.wifi_mac as wifi_mac, a.current_ip as ip,"+
                   "case b.vip when 'vip' then 'vip' else '未知' end  as vip_type from userinfo a left join viptype b on a.serial_number = b.sn")


  }


  def snToProductLine(productSN:String) = {
    if(productSN == null || productSN == ""){
      "helios"
    } else if(productSN.startsWith("P")){
      "orca"
    }else "helios"
  }

  val productModelMap = Map(
    "KA" -> "WTV55K1",
    "KB" -> "WTV43K1",
    "KC" -> "WTV43K1J",
    "KD" -> "WTV55K1J",
    "KE" -> "WTV55K1L",
    "KF" -> "W50J",
    "KG" -> "W50T",
    "KH" -> "WTV55K1T",
    "KI" -> "WTV55K1X",
    "KJ" -> "WTV55K1G",
    "KK" -> "WTV55K1S",
    "KM" -> "W55C1T",
    "KN" -> "W55C1J",
    "KO" -> "W50A",
    "KP" -> "W43F",
    "KQ" -> "W49F",
    "KR" -> "W40F",
    "KS" -> "W65L",
    "XX" -> "测试机",
    "ZA" -> "WTV55K1_特殊版",
    "LB" -> "W65C1J",
    "KT" -> "W78J",
    "KU" -> "W78T",
    "KV" -> "W32H",
    "PA" -> "WP3K1A",
    "PB" -> "WP3K1B",
    "LC" -> "W40T",
    "LM" -> "W55J2",
    "LN" -> "W55T2",
    "LL" -> "W55G2",
    "LW" -> "W55JEU3",
    "LV" -> "W65JEU3Z",
    "LT" -> "W55TEU2P",
    "LS" -> "W55JRU2P",
    "LU" -> "W55GEU2P ",
    "LR" -> "WTV55K1M",
    "LD" -> "W32S",
    "LE" -> "W40K",
    "LF" -> "W43K",
    "LG" -> "W49K",
    "LH" -> "WTV43K1G",
    "LI" -> "W50G",
    "LJ" -> "WTV55K1S",
    "LK" -> "W55C1G",
    "LL" -> "W55G2",
    "LP" -> "W65S",
    "FE" -> "40H2F3000",
    "FD" -> "43D2FA",
    "FG" -> "55D2UA",
    "FH" -> "55D2U3000",
    "FF" -> "49D2U3000",
    "HC" -> "K43W-KONKA",
    "KW" -> "K40W-KONKA",
    "KX" -> "U55W-KONKA",
    "KY" -> "K32W-KONKA",
    "X1" -> "40PFF5331/T3-Philips",
    "X2" -> "55PUF6301/T3-Philips",
    "W1" -> "43PFF5231/T3-Philips",
    "W2" -> "49PUF7031/T3-Philips",
    "W3" -> "55PUF7031/T3-Philips",
    "W5" -> "49PUF6121/T3-Philips",
    "W6" -> "55PUF6121/T3-Philips",
    "W7" -> "55PUF6101/T3-Philips"
  )

  /**
    * 由版本号映射出对应的版本
    *
    * @param sn
    * @return 版本
    */

  def pmMapping(sn: String) = {
    if (sn != null) {
      if (sn.length() >= 10) {
        val snPrefix = sn.substring(0, 2)
        productModelMap.get(snPrefix) match {
          case Some(x) => x.toString()
          case None => {
            "未知"
          }
        }
      } else "未知"
    } else null
  }
  def getSNtwo(sn: String) = {
    if (sn != null) {
      if (sn.length() >= 10) {
        val snPrefix = sn.substring(0, 2)
      } else "未知"
    } else null
  }


}
