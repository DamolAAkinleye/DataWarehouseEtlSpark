package cn.whaley.datawarehouse.fact.whaley.util

import cn.whaley.datawarehouse.global.LogConfig

/**
  * Created by zhangyu on 17/5/16.
  * 频道首页入口维度解析
  * 目前包含 电影热门推荐/少儿首页/音乐首页/体育首页(不含联赛)/收藏首页/会员俱乐部首页
  * 补充资讯/奇趣首页
  */
object ChannelLauncherEntranceUtils extends LogConfig {

  private val PAGECODE = "page_code"
  private val AREACODE = "area_code"
  private val LOCATIONCODE = "location_code"


  def getPageEntrancePageCode(path: String, contentType: String, romVersion: String, firmwareVersion: String): String = {
    val wui = RomVersionUtils.getRomVersion(romVersion, firmwareVersion)
    getPageEntranceCode(path, contentType, PAGECODE, wui)
  }

  def getPageEntranceAreaCode(path: String, contentType: String, romVersion: String, firmwareVersion: String): String = {
    val wui = RomVersionUtils.getRomVersion(romVersion, firmwareVersion)
    getPageEntranceCode(path, contentType, AREACODE, wui)
  }

  def getPageEntranceLocationCode(path: String, contentType: String, romVersion: String, firmwareVersion: String): String = {
    val wui = RomVersionUtils.getRomVersion(romVersion, firmwareVersion)
    getPageEntranceCode(path, contentType, LOCATIONCODE, wui)
  }


  def getPageEntranceCode(path: String, contentType: String, flag: String, wuiVersion: String = ""): String = {
    var result: String = null
    var page: String = null
    var area: String = null
    var location: String = null

    val wui = if (wuiVersion == null) "" else wuiVersion

    if (path == null || path.isEmpty) {
      result
    } else {
      val tmp = path.split("-")
      if (tmp.length == 2) {
        //处理OTA20开始的资讯/奇趣频道的播放小窗(路径为home-hot或者home-interest)
        if (wui >= "02.02.02" && (CHANNEL_HOT == tmp(1) || CHANNEL_INTEREST == tmp(1))) {
          page = tmp(1)
          area = "scale_play"
        }
      } else if(tmp.length >= 3) {
        val tmpPage = ContentTypeUtils.getContentType(path, contentType)
        tmpPage match {
          case CHANNEL_MOVIE | CHANNEL_KIDS | CHANNEL_SPORTS | CHANNEL_VIP => {
            page = tmp(1)
            area = tmp(2)
          }
          case CHANNEL_HOT | CHANNEL_INTEREST => {
            page = tmp(1)
            if (wui >= "02.02.02") {
              //处理单片订阅推荐区或单片人工推荐区
              if (tmp(2).startsWith("subscribe_recommend") || tmp(2).startsWith("div_recommend")) {
                val areaIndex = tmp(2).lastIndexOf("_")
                area = tmp(2).substring(0, areaIndex)
              }
              //处理栏目中心我的订阅区或者推荐栏目区
              else {
                area = tmp(2)
                if (tmp.length >= 4) {
                  if (tmp(3).startsWith("columnPage")) {
                    location = "columnPage"
                  } else if (tmp(3).startsWith("interestColumnPage")) {
                    location = "interestColumnPage"
                  } else location = tmp(3)
                }
              }
            }
          }
          case CHANNEL_MV => {
            page = tmp(1)
            area = tmp(2)
            //处理分类/榜单/账号部分
            area match {
              case "class" | "myAccount" => {
                if (tmp.length >= 4) {
                  location = tmp(3)
                }
              }
              case "rank" => {
                //处理榜单的名称,截取前面部分
                if (tmp.length >= 4) {
                  val rankTmp = tmp(3).split("_")
                  location = rankTmp(0)
                }
              }
              case _ =>
            }
          }
          case _ => {
            tmp(1) match {
              //处理首页收藏频道
              case "collection" | "collect" => {
                page = "collect"
                area = tmp(2)
              }
              case _ =>
            }
          }
        }
      }else {

      }
      flag match {
        case PAGECODE => {
          result = page
        }
        case AREACODE => {
          result = area
        }
        case LOCATIONCODE => {
          result = location
        }
      }
      result

    }
  }

  /**
    * 获取频道首页推荐位索引值(目前只有电影频道热门推荐中有49个推荐位)
    * 补充资讯/奇趣首页推荐位索引值
    *
    * @param locationIndex
    * @param contentType
    * @return
    */
  def getPageEntranceLocationIndex(locationIndex: String, contentType: String, romVersion: String, firmwareVersion: String): Int = {
    val wui = RomVersionUtils.getRomVersion(romVersion, firmwareVersion)
    contentType match {
      case CHANNEL_MOVIE  | CHANNEL_HOT | CHANNEL_INTEREST
      => {
        if (locationIndex == null || locationIndex.isEmpty) {
          -1
        } else locationIndex.toInt + 1
      }
      case _ => -1
    }

  }

  /**
    * 根据点击日志获取频道首页推荐位索引值
    *
    * @param locationIndex
    * @return
    */
  def getPageLocationIndexFromClick(locationIndex: String): Int = {
    {
      if (locationIndex == null || locationIndex.isEmpty) {
        -1
      } else locationIndex.toInt + 1
    }
  }

  /**
    * 根据点击日志获取频道首页location信息
    *
    * @param contentType
    * @param locationCode
    * @return
    */

  def getPageLocationFromClick(contentType: String, locationCode: String): String = {
    try {
      if (contentType == CHANNEL_MOVIE || contentType == CHANNEL_HOT || contentType == CHANNEL_INTEREST) {
        null
      } else locationCode
    } catch {
      case ex: Exception => ""
    }
  }

  /**
    * 根据点击日志获取频道首页area信息
    *
    * @param contentType
    * @return areaName
    */

  def getPageAreaFromClick(contentType: String, areaName: String): String = {
    try {
      if (contentType == CHANNEL_MOVIE && areaName == "moive_recommend") "movie_recommend"
      else if(contentType == CHANNEL_INTEREST || contentType == CHANNEL_HOT ) {
        if (areaName.startsWith("subscribe_recommend") || areaName.startsWith("div_recommend")) {
          val areaIndex = areaName.lastIndexOf("_")
          areaName.substring(0, areaIndex)
        }else areaName
      }
      else areaName
    } catch {
      case ex: Exception => ""
    }
  }


}





