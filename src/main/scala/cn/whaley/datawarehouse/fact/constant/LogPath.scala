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

  //微鲸播放质量日志
  val HELIOS_GET_VIDEO = s"/log/whaley/parquet/$DATE_ESCAPE/helios-player-sdk-getVideoInfo"
  val HELIOS_START_PLAY = s"/log/whaley/parquet/$DATE_ESCAPE/helios-player-sdk-startPlay"
  val HELIOS_PARSE = s"/log/whaley/parquet/$DATE_ESCAPE/helios-player-sdk-inner_outer_auth_parse"
  val HELIOS_BUFFER = s"/log/whaley/parquet/$DATE_ESCAPE/helios-player-sdk-buffer"
  val HELIOS_END_PLAY = s"/log/whaley/parquet/$DATE_ESCAPE/helios-player-sdk-endPlay"

  //微鲸站点树维度表
  val DIM_WHALEY_SOURCE_SITE = s"/data_warehouse/dw_dimensions/dim_whaley_source_site"
  //首页入口维度表
  val DIM_WHALEY_LAUNCHER_ENTRANCE = s"/data_warehouse/dw_dimensions/dim_whaley_launcher_entrance"
  //各频道入口维度表
  val DIM_WHALEY_PAGE_ENTRANCE = s"/data_warehouse/dw_dimensions/dim_whaley_page_entrance"

  val DIM_WHALEY_SUBJECT = s"/data_warehouse/dw_dimensions/dim_whaley_subject"
  val DIM_WHALEY_MV_TOPIC = s"/data_warehouse/dw_dimensions/dim_whaley_mv_topic"
  val DIM_WHALEY_MV_HOT_LIST = s"/data_warehouse/dw_dimensions/dim_whaley_mv_hot_list"
  val DIM_WHALEY_SINGER = s"/data_warehouse/dw_dimensions/dim_whaley_singer"
  val DIM_WHALEY_RADIO = s"/data_warehouse/dw_dimensions/dim_whaley_radio"
  val DIM_WHALEY_SPOTRS_MATCH = s"/data_warehouse/dw_dimensions/dim_whaley_sports_match"

}
