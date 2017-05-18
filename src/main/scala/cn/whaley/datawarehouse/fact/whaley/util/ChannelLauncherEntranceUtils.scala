package cn.whaley.datawarehouse.fact.whaley.util

import cn.whaley.datawarehouse.global.LogConfig

/**
 * Created by zhangyu on 17/5/16.
 * 频道首页入口维度解析
 * 目前包含 电影热门推荐/少儿首页/音乐首页/体育首页(不含联赛)/收藏首页/会员俱乐部首页
 */
object ChannelLauncherEntranceUtils extends LogConfig {

  private val PAGECODE = "page_code"
  private val AREACODE = "area_code"
  private val LOCATIONCODE = "location_code"


  def getPageEntrancePageCode(path: String, contentType: String): String = {
    getPageEntranceCode(path, contentType, PAGECODE)
  }

  def getPageEntranceAreaCode(path: String, contentType: String): String = {
    getPageEntranceCode(path, contentType, AREACODE)
  }

  def getPageEntranceLocationCode(path: String, contentType: String): String = {
    getPageEntranceCode(path, contentType, LOCATIONCODE)
  }


  def getPageEntranceCode(path: String, contentType: String, flag: String): String = {
    var result: String = null
    var page: String = null
    var area: String = null
    var location: String = null

    if (path == null || path.isEmpty) {
      result
    } else {
      val tmp = path.split("-")
      if (tmp.length < 3) {
        result
      } else {
        val tmpPage = ContentTypeUtils.getContentType(path, contentType)
        tmpPage match {
          case CHANNEL_MOVIE | CHANNEL_KIDS | CHANNEL_SPORTS | CHANNEL_VIP => {
            page = tmp(1)
            area = tmp(2)
          }
          case CHANNEL_MV => {
            page = tmp(1)
            area = tmp(2)
            area match {
              case "class" | "myAccount" => {
                if (tmp.length >= 4) {
                  location = tmp(3)
                }
              }
              case "rank"  => {
                if(tmp.length >= 4){
                  val rankTmp = tmp(3).split("_")
                  location = rankTmp(0)
                }
              }
              case _ =>
            }
          }
          case _ => {
            tmp(1) match {
              case "collection" | "collect" => {
                page = "collect"
                area = tmp(2)
              }
              case _ =>
            }
          }
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
  }

  /**
   * 获取频道首页推荐位索引值(目前只有电影频道热门推荐中有49个推荐位)
   * @param locationIndex
   * @param contentType
   * @return
   */
  def getPageEntranceLocationIndex(locationIndex: String, contentType: String): Int = {

    contentType match {
      case CHANNEL_MOVIE => {
        if (locationIndex == null || locationIndex.isEmpty) {
          -1
        } else locationIndex.toInt + 1
      }
      case _ => -1
    }

  }
}





