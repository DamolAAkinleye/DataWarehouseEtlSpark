package cn.whaley.datawarehouse.fact.constant

/**
  * Created by Tony on 17/4/5.
  */
object LogPath {

  val DATE_ESCAPE = "#{startDate}"
  val MEDUSA_LOGIN_LOG_PATH = s"/log/moretvloginlog/parquet/$DATE_ESCAPE/loginlog"
  val HELIOS_WHALEYVIP_GETBUYVIPPROCESS = s"/log/whaley/parquet/$DATE_ESCAPE/helios-whaleyvip-getBuyVipProcess"
  val HELIOS_ON = s"/log/whaley/parquet/$DATE_ESCAPE/on"
  val HELIOS_OFF = s"/log/whaley/parquet/$DATE_ESCAPE/off"
  val HELIOS_PLAY = s"/log/whaley/parquet/$DATE_ESCAPE/play"
  val HELIOS_DETAIL = s"/log/whaley/parquet/$DATE_ESCAPE/detail"
  //微鲸站点树维度表
  val DIM_WHALEY_SOURCE_SITE = s"/data_warehouse/dw_dimensions/dim_whaley_source_site"
  //首页入口维度表
  val DIM_WHALEY_LAUNCHER_ENTRANCE = s"/data_warehouse/dw_dimensions/dim_whaley_launcher_entrance"
  //各频道入口维度表
  val DIM_WHALEY_PAGE_ENTRANCE = s"/data_warehouse/dw_dimensions/dim_whaley_page_entrance"

}
