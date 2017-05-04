package cn.whaley.datawarehouse.fact.util

import cn.whaley.datawarehouse.common.{DimensionColumn, DimensionJoinCondition}
import cn.whaley.datawarehouse.global.LogConfig
import org.apache.avro.TestAnnotation

/**
  * Created by wu.jiulin on 2017/5/2.
  *
  * 频道主页来源维度(少儿，音乐，体育)
  * 解析出pathMain或者path里的area_code和location_code，然后关联dim_medusa_page_entrance维度表，获得代理键
  * 1.kids和sports没有location_code. mv下的mvTopHomePage和mvRecommendHomePage有一部分location_code is null的情况,需要特殊处理
  * 2.kids的路径解析不出dim_medusa_page_entrance维度表的area_code,需要根据medusa_path_program_site_code_map维度表获得
  * 3.kids有medusa和moretv,mv和sports只有medusa
  * 4.mvRecommendHomePage下的mv_station需要特殊处理
  * 5.mvTopHomePage下的rege,xinge,biaoshen需要特殊处理
  */
object PageEntrancePathParseUtils extends LogConfig {

  private val PAGE_ENTRANCE_KIDS_REGEX = (".*(kids_home)-([A-Za-z_]*)").r
  private val PAGE_ENTRANCE_MV_REGEX = (".*(mv)\\*([A-Za-z_]*)\\*([a-zA-Z_]*)").r
  private val PAGE_ENTRANCE_SPORTS_REGEX = (".*(sports)\\*([A-Za-z_]*)").r
  private val PAGE_ENTRANCE_LOCATION_CODE_LIST = List("personal_recommend", "site_mvsubject", "biaoshen", "site_concert", "site_dance", "site_mvyear", "site_collect", "site_mvarea", "site_mvstyle", "mv_station", "xinge", "rege", "site_hotsinger", "search")
  //  private val PAGE_ENTRANCE_REGEX = (".*(mv|kids_home|sports)[\\*-]{1,}([A-Za-z_]*)[\\*-]{1,}([a-zA-Z_]*)").r
  //  private val PAGE_ENTRANCE_WITHOUT_LOCATION_CODE_REGEX = (".*(mv|kids_home|sports)[\\*-]{1,}([A-Za-z_]*)").r

  def getPageEntrancePageCode(pathMain: String, path: String, flag: String): String = {
    var pageCode: String = null
    val code = "page"
    flag match {
      case MEDUSA =>
        pageCode = getPageEntranceCodeByPathETL(pathMain, flag, code)
      case MORETV =>
        pageCode = getPageEntranceCodeByPathETL(path, flag, code)
    }
    pageCode
  }

  def getPageEntranceAreaCode(pathMain: String, path: String, flag: String): String = {
    var areaCode: String = null
    val code = "area"
    flag match {
      case MEDUSA =>
        areaCode = getPageEntranceCodeByPathETL(pathMain, flag, code)
      case MORETV =>
        areaCode = getPageEntranceCodeByPathETL(path, flag, code)
    }
    areaCode
  }

  def getPageEntranceLocationCode(pathMain: String, path: String, flag: String): String = {
    var locationCode: String = null
    val code = "location"
    flag match {
      case MEDUSA =>
        locationCode = getPageEntranceCodeByPathETL(pathMain, flag, code)
      case MORETV =>
        locationCode = getPageEntranceCodeByPathETL(path, flag, code)
    }
    locationCode
  }

  def getPageEntranceCodeByPathETL(path: String, flag: String, code: String): String = {
    var result: String = null
    var page_code: String = null
    var area_code: String = null
    var location_code: String = null
    if (path == null || flag == null || code == null) return result
    /** kids: without location_code */
    if (path.contains("kids_home")) {
      PAGE_ENTRANCE_KIDS_REGEX findFirstMatchIn path match {
        case Some(p) => {
          page_code = p.group(1).split("_")(0)
          area_code = p.group(2)
        }
        case None =>
      }
      area_code match {
        case "xuezhishi" => area_code = "kids_knowledge"
        case "tingerge" => area_code = "show_kidsSongSite"
        case "kandonghua" => area_code = "show_kidsSite"
        case "kids_anim" => area_code = "show_kidsSite"
        case "kids_rhymes" => area_code = "show_kidsSongSite"
        case "kids_songhome" => area_code = "show_kidsSongSite"
        case "kids_seecartoon" => area_code = "show_kidsSite"
        case "kids_recommend" => area_code = "kids_recommend"
        case "kids_cathouse" => area_code = "kids_collect"
        case "kids_collect" => area_code = "kids_collect"
        case _ =>
      }
    }
    /** mv: mvRecommendHomePage,mvTopHomePage一部分没有location_code */
    if (path.contains("mv*")) {
      /** home*classification*mv-mv*mvRecommendHomePage*8qlmwxd3abnp-mv_station */
      if (path.contains("mv_station") && path.contains("mvRecommendHomePage")) {
        page_code = "mv"
        area_code = "mvRecommendHomePage"
        location_code = "mv_station"
      } else {
        PAGE_ENTRANCE_MV_REGEX findFirstMatchIn path match {
          case Some(p) => {
            page_code = p.group(1)
            area_code = p.group(2)
            location_code = p.group(3)
          }
          case None =>
        }
        location_code match {
          case "rege_" => location_code = location_code.split("_")(0)
          case "xinge_" => location_code = location_code.split("_")(0)
          case "biaoshen_" => location_code = location_code.split("_")(0)
          case _ =>
        }
        if (!PAGE_ENTRANCE_LOCATION_CODE_LIST.contains(location_code)) location_code = null
      }
    }
    /** sports: without location_code */
    if (path.contains("sports")) {
      PAGE_ENTRANCE_SPORTS_REGEX findFirstMatchIn path match {
        case Some(p) => {
          page_code = p.group(1)
          area_code = p.group(2)
        }
        case None =>
      }
    }

    code match {
      case "page" => result = page_code
      case "area" => result = area_code
      case "location" => result = location_code
    }

    result
  }

  /** 通过area_code和location_code取得page_entrance_sk */
  def getPageEntranceSK(): DimensionColumn = {
    new DimensionColumn("dim_medusa_page_entrance",
      List(
        DimensionJoinCondition(
          /** location_code is not null,join with page_code, area_code and location_code. */
          Map("pageEntrancePageCode" -> "page_code", "pageEntranceAreaCode" -> "area_code", "pageEntranceLocationCode" -> "location_code"),
          " location_code is not null", null, " pageEntranceLocationCode is not null"
        ),
        DimensionJoinCondition(
          /** location_code is null,join with page_code and area_code. */
          Map("pageEntrancePageCode" -> "page_code", "pageEntranceAreaCode" -> "area_code"),
          " location_code is null", null, " pageEntranceLocationCode is null"
        )
      ),
      "page_entrance_sk")
  }

  @TestAnnotation
  def main(args: Array[String]): Unit = {
    val path = ""
    val pathMain = "home*classification*mv-mv*mvRecommendHomePage*8qc3op34qr23-mv_station"
    println(path)
    println(pathMain)
    println(getPageEntrancePageCode(pathMain, path, MEDUSA))
    println(getPageEntranceAreaCode(pathMain, path, MEDUSA))
    println(getPageEntranceLocationCode(pathMain, path, MEDUSA))

    //    println(PAGE_ENTRANCE_LOCATION_CODE_LIST.map(e=>
    //      "'" + e + "'"
    //    ).mkString(","))

//    PAGE_ENTRANCE_MV_REGEX findFirstMatchIn pathMain match {
//      case Some(p) => {
//        (1 until p.groupCount + 1).foreach(i => {
//          println(p.group(i))
//        })
//      }
//      case None =>
//    }

  }

}
