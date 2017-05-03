package cn.whaley.datawarehouse.fact.constant

/**
  * Created by Tony on 17/4/5.
  */
object LogPath {

  val DATE_ESCAPE = "#{startDate}"
  val LOGIN_LOG_PATH = s"/log/moretvloginlog/parquet/$DATE_ESCAPE/loginlog"
  val HELIOS_WHALEYVIP_GETBUYVIPPROCESS = s"/log/whaley/parquet/$DATE_ESCAPE/helios-whaleyvip-getBuyVipProcess"
  val HELIOS_ON = s"/log/whaley/parquet/$DATE_ESCAPE/on"
  val HELIOS_OFF = s"/log/whaley/parquet/$DATE_ESCAPE/off"


}
