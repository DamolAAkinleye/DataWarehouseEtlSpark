package cn.whaley.datawarehouse.fact.util

import cn.whaley.datawarehouse.common.{DimensionColumn, DimensionJoinCondition}
import cn.whaley.datawarehouse.global.LogConfig
import org.apache.avro.TestAnnotation

/**
  * Created by michael on 2017/4/24.
  * updated by wu.jiulin on 2017/4/27.
  * 首页入口维度工具类
  */
object EntranceTypeUtils extends LogConfig {

  /**
    * 对于medusa日志live,recommendation,search,setting没有location_code
    * 对于moretv日志只有live,search有对应的路径信息且只有area_code
    */
  private val MEDUSA_ENTRANCE_REGEX = ("home\\*(classification|foundation|my_tv)\\*[0-9-]{0,2}([a-z_]*)").r
  private val MEDUSA_ENTRANCE_REGEX_WITHOUT_LOCATION_CODE = ("(live|recommendation|search|setting)").r
  private val MORETV_ENTRANCE_REGEX = ("home-(TVlive|live|search)").r

  private def getEntranceCodeByPathETL(path: String, flag: String, code: String): String = {
    var result: String = null
    var launcher_area_code: String = null
    var launcher_location_code: String = null
    if (null != path && null != flag && null != code) {
      flag match {
        case MEDUSA => {
          if (path.contains("home*classification") || path.contains("home*foundation") || path.contains("home*my_tv")) {
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
                launcher_area_code = p.group(1)
              }
              case None =>
            }
          }
        }
        case MORETV => {
          MORETV_ENTRANCE_REGEX findFirstMatchIn path match {
            case Some(p) => {
              launcher_area_code = p.group(1)
              if(launcher_area_code.equalsIgnoreCase("TVlive")){
                launcher_area_code="live"
              }
            }
            case None =>
          }
        }
      }

      code match {
        case "area" => result = launcher_area_code
        case "location" => result = launcher_location_code
      }
    }
    result
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

  /** 通过launcher_area_code和launcher_location_code取得launcher_entrance_sk */
  def getLauncherEntranceSK(): DimensionColumn = {
    new DimensionColumn("dim_medusa_launcher_entrance",
      List(
        DimensionJoinCondition(
          /** launcher_location_code is not null,join with launcher_area_code and launcher_location_code. (classification,foundation,my_tv) */
          Map("launcherAreaCode" -> "launcher_area_code", "launcherLocationCode" -> "launcher_location_code"),
          " launcher_area_code in ('classification','foundation','my_tv')", null, " launcherAreaCode in ('classification','foundation','my_tv')"
        ),
        DimensionJoinCondition(
          /** launcher_location_code is null,join with launcher_area_code. (live,recommendation,search,setting) */
          Map("launcherAreaCode" -> "launcher_area_code"),
          " launcher_area_code in ('live','recommendation','search','setting')", null, " launcherAreaCode in ('live','recommendation','search','setting')"
        )
      ),
      "launcher_entrance_sk")
  }

}
