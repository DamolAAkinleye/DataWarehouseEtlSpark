package cn.whaley.datawarehouse.fact.util

import cn.whaley.datawarehouse.common.{DimensionColumn, DimensionJoinCondition}
import cn.whaley.datawarehouse.global.LogConfig
import org.apache.avro.TestAnnotation

/**
  * Created by michael on 2017/4/24.
  * updated by wu.jiulin on 2017/4/27.
  */
object EntranceTypeUtils extends LogConfig{
  /*get from com.moretv.bi.report.medusa.newsRoomKPI.ChannelEntrancePlayStat
     用来做 不同入口播放统计
    */
  private val sourceRe = ("(home\\*classification|search|home\\*my_tv\\*history|" +
    "home\\*my_tv\\*collect|home\\*recommendation|home\\*my_tv\\*[a-zA-Z0-9&\\u4e00-\\u9fa5]{1,})").r
  private val sourceRe1 = ("(classification|history|hotrecommend|search)").r

  /**
    * 对于medusa日志live,recommendation,search,setting没有location_code
    * 对于moretv日志只有live,search有对应的路径信息且只有area_code
    */
  private val MEDUSA_ENTRANCE_REGEX = ("home\\*(classification|foundation|my_tv)\\*[0-9-]{0,2}([a-z_]*)").r  //area_code: group(1), location_code: group(2)
  private val MEDUSA_ENTRANCE_REGEX_WITHOUT_LOCATION_CODE = ("home(\\*|-|\\*navi\\*)(live|recommendation|search|setting).*").r  //area_code: group(2)
  private val MORETV_ENTRANCE_REGEX = ("home-(live|search)-.*").r  //area_code: group(1)

  def getEntranceTypeByPathETL(pathMain: String, path: String, flag: String): String = {
    val specialPattern = "home\\*my_tv\\*[a-zA-Z0-9&\\u4e00-\\u9fa5]{1,}".r
    flag match {
      case "medusa" => {
        if (null == pathMain) {
          s"未知${flag}"
        } else {
          sourceRe findFirstMatchIn pathMain match {
            case Some(p) => {
              p.group(1) match {
                case "home*classification" => "分类入口"
                case "home*my_tv*history" => "历史"
                case "home*my_tv*collect" => "收藏"
                case "home*recommendation" => "首页推荐"
                case "search" => "搜索"
                case _ => {
                  if (specialPattern.pattern.matcher(p.group(1)).matches) {
                    "自定义入口"
                  }
                  else {
                    "其它3"
                  }
                }
              }
            }
            case None => "其它3"
          }
        }
      }
      case "moretv" => {
        if (null == path) {
          s"未知${flag}"
        } else {
          sourceRe1 findFirstMatchIn path match {
            case Some(p) => {
              p.group(1) match {
                case "classification" => "分类入口"
                case "history" => "历史"
                case "hotrecommend" => "首页推荐"
                case "search" => "搜索"
                case _ => "其它2"
              }
            }
            case None => "其它2"
          }
        }
      }
    }
  }

  def getEntranceAreaCode(pathMain: String, path: String, flag: String): String = {
    var areaCode: String = null
    val code = "area"
    flag match {
      case MEDUSA => {
        areaCode = getEntranceCodeByPathETL(pathMain, flag, code)
      }
      case MORETV => {
        areaCode = getEntranceCodeByPathETL(path, flag, code)
      }
    }

    areaCode
  }

  def getEntranceLocationCode(pathMain: String, path: String, flag: String): String = {
    var locationCode: String = null
    val code = "location"
    flag match {
      case MEDUSA => {
        locationCode = getEntranceCodeByPathETL(pathMain, flag, code)
      }
      case MORETV => {
        locationCode = getEntranceCodeByPathETL(path, flag, code)
      }
    }

    locationCode
  }

  def getEntranceCodeByPathETL(path: String, flag: String, code: String): String = {
    var result: String = null
    var launcher_area_code: String = null
    var launcher_location_code: String = null
    if (path == null || flag == null || code == null) return result
    flag match {
      case MEDUSA => {
        if (path.contains("home*classification") || path.contains("home*foundation") || path.contains("home*my_tv")){
          MEDUSA_ENTRANCE_REGEX findFirstMatchIn path match {
            case Some(p) => {
              launcher_area_code = p.group(1)
              launcher_location_code = p.group(2)
            }
            case None =>
          }
        } else {
          MEDUSA_ENTRANCE_REGEX_WITHOUT_LOCATION_CODE findFirstMatchIn path match {
            case Some(p) => {
              launcher_area_code = p.group(2)
            }
            case None =>
          }
        }
      }
      case MORETV => {
        MORETV_ENTRANCE_REGEX findFirstMatchIn path match {
          case Some(p) => {
            launcher_area_code = p.group(1)
          }
          case None =>
        }
      }
    }

    code match {
      case "area" => result = launcher_area_code
      case "location" => result = launcher_location_code
    }

    result
  }

  /** 通过launcher_area_code和launcher_location_code取得launcher_entrance_sk */
  def getLauncherEntranceSK() :DimensionColumn = {
    new DimensionColumn("dim_medusa_launcher_entrance",
      List(
        DimensionJoinCondition(
        /**launcher_location_code is not null,join with launcher_area_code and launcher_location_code. (classification,foundation,my_tv) */
        Map("launcherAreaCode" -> "launcher_area_code","launcherLocationCode" -> "launcher_location_code"),
        " launcher_area_code in ('classification','foundation','my_tv')",null," launcherAreaCode in ('classification','foundation','my_tv')"
        ),
        DimensionJoinCondition(
          /**launcher_location_code is null,join with launcher_area_code. (live,recommendation,search,setting) */
          Map("launcherAreaCode" -> "launcher_area_code"),
          " launcher_area_code in ('live','recommendation','search','setting')",null," launcherAreaCode in ('live','recommendation','search','setting')"
        )
      ),
      "launcher_entrance_sk")
  }

  @TestAnnotation
  def main(args: Array[String]): Unit = {
    val path = ""
    val pathMain = "home*my_tv*kids-kids_home-tingerge*随便听听"
//    val pathMain = "home*my_tv*6-sports*welfareHomePage*no2cijvw7pgh"
    println(path)
    println(pathMain)
    println(getEntranceAreaCode(pathMain, path, MEDUSA) + "---" + getEntranceLocationCode(pathMain, path, MEDUSA))
  }

}
