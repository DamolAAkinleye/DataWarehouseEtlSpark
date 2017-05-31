package cn.whaley.datawarehouse.fact.moretv.util

import cn.whaley.datawarehouse.common.{DimensionColumn, DimensionJoinCondition}
import cn.whaley.datawarehouse.fact.constant.UDFConstantDimension
import cn.whaley.datawarehouse.global.{DimensionTypes, LogConfig}
import cn.whaley.sdk.udf.UDFConstant

/**
  * Created by baozhiwang on 2017/4/24.
  * Updated by wujiulin on 2017/5/10.
  */
object ListCategoryUtils extends LogConfig {

  private val MEDUSA_LIST_PAGE_LEVEL_1_REGEX = UDFConstantDimension.MEDUSA_LIST_Page_LEVEL_1.mkString("|")
  private val regex_medusa_list_category_other = (s".*($MEDUSA_LIST_PAGE_LEVEL_1_REGEX)\\*([a-zA-Z0-9&\u4e00-\u9fa5]+)").r
  private val regex_medusa_list_retrieval = (s".*($MEDUSA_LIST_PAGE_LEVEL_1_REGEX)[-*]?(${UDFConstantDimension.RETRIEVAL_DIMENSION}|${UDFConstantDimension.RETRIEVAL_DIMENSION_CHINESE}).*").r
  private val regex_medusa_list_search = (s".*($MEDUSA_LIST_PAGE_LEVEL_1_REGEX)[-*]?(${UDFConstantDimension.SEARCH_DIMENSION}|${UDFConstantDimension.SEARCH_DIMENSION_CHINESE}).*").r
  /** 用于频道分类入口统计，解析出资讯的一级入口、二级入口 */
  private val regex_medusa_recommendation = (s"home\\*recommendation\\*[\\d]{1}-($MEDUSA_LIST_PAGE_LEVEL_1_REGEX)\\*(.*)").r

  def getListMainCategory(pathMain: String, path: String, flag: String): String = {
    var result: String = null
    flag match {
      case MEDUSA => {
        result = getListCategoryMedusaETL(pathMain, 1)
      }
      case MORETV => {
        result = getListCategoryMoretvETL(path, 1)
      }
    }
    result
  }

  def getListSecondCategory(pathMain: String, path: String, flag: String): String = {
    var result: String = null
    flag match {
      case MEDUSA => {
        result = getListCategoryMedusaETL(pathMain, 2)
      }
      case MORETV => {
        result = getListCategoryMoretvETL(path, 2)
      }
    }
    result
  }

  def getListThirdCategory(pathMain: String, path: String, flag: String): String = {
    var result: String = null
    flag match {
      case MEDUSA => {
        result = getListCategoryMedusaETL(pathMain, 3)
      }
      case MORETV => {
        result = getListCategoryMoretvETL(path, 3)
      }
    }
    result
  }

  /** 解析列表页四级入口，针对sports */
  def getListFourthCategory(pathMain: String, path: String, flag: String): String = {
    var result: String = null
    if (pathMain==null || !pathMain.contains(UDFConstantDimension.SPORTS_LIST_DIMENSION_TRAIT)) return result
    flag match {
      case MEDUSA => {
        result = getListCategoryMedusaETL(pathMain, 4)
      }
      case MORETV => result = null
    }
    result
  }

  /**
    * 获取列表页入口信息
    * 第一步，过滤掉包含search字段的pathMain
    * 第二步，判别是来自classification还是来自my_tv
    * 第三步，分音乐、体育、少儿以及其他类型【电视剧，电影等】获得列表入口信息,根据具体的分类做正则表达
    */
  def getListCategoryMedusaETL(pathMain: String, index_input: Int): String = {
    var result: String = null
    if (null == pathMain || pathMain.contains(UDFConstantDimension.HOME_SEARCH)) {
      result = null
    }
      /** 少儿kids */
      else if (pathMain.contains(UDFConstantDimension.KIDS)) {
        result = KidsPathParserUtils.pathMainParse(pathMain, index_input)
        if(2 == index_input && null != result){
          result match {
            case "tingerge" => result="show_kidsSongSite"
            case "kids_rhymes" => result="show_kidsSongSite"
            case "kids_songhome" => result="show_kidsSongSite"
            case "kids_seecartoon" => result="show_kidsSite"
            case "kandonghua" => result="show_kidsSite"
            case "kids_anim" => result="show_kidsSite"
            case "xuezhishi" => result="kids_knowledge"
            case _ =>
          }
        }
      }
      /** 音乐mv */
      else if (pathMain.contains(UDFConstantDimension.MV_CATEGORY) || pathMain.contains(UDFConstantDimension.MV_POSTER)) {
        result = MvPathParseUtils.pathMainParse(pathMain,index_input)
      }
      /** 体育sports */
      else if (pathMain.contains(UDFConstantDimension.SPORTS_LIST_DIMENSION_TRAIT)) {
        result = SportsPathParserUtils.pathMainParse(pathMain, index_input)
        if(2 == index_input && "League".equalsIgnoreCase(result)){
          result = "leagueEntry"
        }
      }
      /** 筛选维度 */
      else if (pathMain.contains(UDFConstantDimension.RETRIEVAL_DIMENSION) || pathMain.contains(UDFConstantDimension.RETRIEVAL_DIMENSION_CHINESE)) {
        regex_medusa_list_retrieval findFirstMatchIn pathMain match {
          case Some(p) => {
            if (index_input == 1) {
              result = p.group(1)
            } else if (index_input == 2) {
              result = UDFConstantDimension.RETRIEVAL_DIMENSION_CHINESE
            }
          }
          case None =>
        }
      }
      /** 搜索维度 */
      else if (pathMain.contains(UDFConstantDimension.SEARCH_DIMENSION) || pathMain.contains(UDFConstantDimension.SEARCH_DIMENSION_CHINESE)) {
        regex_medusa_list_search findFirstMatchIn pathMain match {
          case Some(p) => {
            if (index_input == 1) {
              result = p.group(1)
            } else if (index_input == 2) {
              result = UDFConstantDimension.SEARCH_DIMENSION_CHINESE
            }
          }
          case None =>
        }
      }
      /** home*recommendation*1-hot*今日焦点 解析出 hot,今日焦点 */
      else if (pathMain.contains(UDFConstantDimension.HOME_RECOMMENDATION)) {
        regex_medusa_recommendation findFirstMatchIn pathMain match {
          case Some(p) => {
            if (index_input == 1) {
              result = p.group(1)
            } else if (index_input == 2) {
              result = p.group(2)
            }
          }
          case None =>
        }
      }
      /** 其他频道,例如 电影，电视剧 */
      else {
        regex_medusa_list_category_other findFirstMatchIn pathMain match {
          case Some(p) => {
            if (index_input == 1) {
              result = p.group(1)
            } else if (index_input == 2) {
              result = p.group(2)
            }
          }
          case None =>
        }
      }

    result
  }

  /**
    * 2.x，原有统计分析没有做少儿；体育最新的逻辑解析没有上线
    * SportsPathParserUtils现在没有解析2.x path路径
    **/
  def getListCategoryMoretvETL(path: String, index_input: Int): String = {
    var result: String = null
    //去除过滤包含"search"的逻辑, !path.contains(UDFConstantDimension.SEARCH_DIMENSION)
    if (null != path) {
      //少儿使用最新逻辑
      if (path.contains(UDFConstantDimension.KIDS)) {
        result = KidsPathParserUtils.pathParse(path, index_input)
        if(2 == index_input && null != result) {
          result match {
            case "tingerge" => result="show_kidsSongSite"
            case "kids_rhymes" => result="show_kidsSongSite"
            case "kids_songhome" => result="show_kidsSongSite"
            case "kids_cathouse" => result="show_kidsSongSite"
            case "kids_seecartoon" => result="show_kidsSite"
            case "kandonghua" => result="show_kidsSite"
            case "kids_anim" => result="show_kidsSite"
            case "xuezhishi" => result="kids_knowledge"
            case _ =>
          }
        }
      } else {
        //其他类型仍然使用原有逻辑
        if (index_input == 1) {
          result = PathParserUtils.getSplitInfo(path, 2)
          if (result != null) {
            // 如果accessArea为“navi”和“classification”，则保持不变，即在launcherAccessLocation中
            if (!UDFConstant.MoretvLauncherAccessLocation.contains(result)) {
              // 如果不在launcherAccessLocation中，则判断accessArea是否在uppart中
              if (UDFConstant.MoretvLauncherUPPART.contains(result)) {
                result = "MoretvLauncherUPPART"
              } else {
                result = null
              }
            }
          }
        } else if (index_input == 2) {
          result = PathParserUtils.getSplitInfo(path, 3)
          if (result != null) {
            if (PathParserUtils.getSplitInfo(path, 2) == "sports") {
              result = PathParserUtils.getSplitInfo(path, 3) + "-" + PathParserUtils.getSplitInfo(path, 4)
            }
            if (!UDFConstant.MoretvPageInfo.contains(PathParserUtils.getSplitInfo(path, 2))) {
              result = null
            }
          }
        }
      }
    }
    result
  }


  def getSourceSiteSK() :DimensionColumn = {
    new DimensionColumn("dim_medusa_source_site",
      List(
        //获得MEDUSA中除了少儿，体育和音乐的列表维度sk，[只有一级，二级维度]
        DimensionJoinCondition(
          Map("mainCategory" -> "site_content_type","secondCategory" -> "second_category"),
          "site_content_type is not null and main_category_code in " +
            "('site_tv','site_movie','site_xiqu','site_comic','site_zongyi','site_hot','site_jilu')",
          null,s" flag='$MEDUSA' and mainCategory not in ('$CHANNEL_SPORTS','$CHANNEL_KIDS','$CHANNEL_MV')"
        ),
        //获得MORETV中除了少儿，体育和音乐的列表维度sk ，[只有一级，二级维度]
        DimensionJoinCondition(
          Map("mainCategory" -> "site_content_type","secondCategory" -> "second_category_code"),
          "site_content_type is not null and main_category_code in " +
            "('site_tv','site_movie','site_xiqu','site_comic','site_zongyi','site_hot','site_jilu')",
          null,s" flag='$MORETV' and mainCategory not in ('$CHANNEL_SPORTS','$CHANNEL_KIDS','$CHANNEL_MV')"
        ),
        //获得音乐的列表维度sk ，热门歌手，精选集，电台，排行榜只到二级维度 [只有有一级，二级维度]
        DimensionJoinCondition(
          Map("mainCategory" -> "site_content_type","secondCategory" -> "second_category_code"),
          s"site_content_type in ('$CHANNEL_MV') and main_category_code in ('mv_site') and second_category_code in ('site_hotsinger','site_mvtop','site_mvradio','site_mvsubject')" ,
          null,s" mainCategory in ('$CHANNEL_MV') and secondCategory in ('site_hotsinger','site_mvtop','site_mvradio','site_mvsubject') "
        ),
        //moretv日志里的少儿维度，三级入口需要使用code关联, [有一级，二级,三级维度]
        DimensionJoinCondition(
          Map("mainCategory" -> "site_content_type","secondCategory" -> "second_category_code","thirdCategory"->"third_category_code"),
          s"site_content_type in ('$CHANNEL_KIDS') and main_category_code in " +
            "('kids_site')",
          null,s" flag='$MORETV' and mainCategory in ('$CHANNEL_KIDS')"
        ),
        //获得少儿和音乐的列表维度sk ，[有一级，二级,三级维度]
        DimensionJoinCondition(
          Map("mainCategory" -> "site_content_type","secondCategory" -> "second_category_code","thirdCategory"->"third_category"),
          s"site_content_type in ('$CHANNEL_KIDS','$CHANNEL_MV') and main_category_code in " +
            "('kids_site','mv_site')",
          null,s" mainCategory in ('$CHANNEL_KIDS','$CHANNEL_MV')"
        ),
        //获得体育列表维度sk ，[有一级，二级,三级,四级维度]
        DimensionJoinCondition(
          Map("mainCategory" -> "site_content_type","secondCategory" -> "second_category_code","thirdCategory"->"third_category_code","fourthCategory"->"fourth_category"),
          s"site_content_type in ('$CHANNEL_SPORTS') and fourth_category is not null and fourth_category<>'' and fourth_category<>'null'",
          null,s" mainCategory in ('$CHANNEL_SPORTS') and fourthCategory is not null and fourthCategory<>'' and fourthCategory<>'null'"
        ),
        //获得体育列表维度sk ，[只有一级，二级,三级维度]
        DimensionJoinCondition(
          Map("mainCategory" -> "site_content_type","secondCategory" -> "second_category_code","thirdCategory"->"third_category_code"),
          s"site_content_type in ('$CHANNEL_SPORTS') and (fourth_category is null or fourth_category='' or fourth_category='null') ",
          null,s" mainCategory in ('$CHANNEL_SPORTS') and (fourthCategory is null or fourthCategory='' or fourthCategory='null')"
        )
      ),
      "source_site_sk")
  }




  def getSportsSecondCategory() :DimensionColumn = {
    //获得体育的列表页二级入口中文名称
    new DimensionColumn("dim_medusa_page_entrance",
      List(DimensionJoinCondition(
        Map("mainCategory" -> "page_code","secondCategory" -> "area_code"),
        s"page_code='$CHANNEL_SPORTS' ",
        null,s"mainCategory='$CHANNEL_SPORTS'"
      )),
      "page_entrance_sk")
  }

  def c() :DimensionColumn = {
    new DimensionColumn(s"${DimensionTypes.DIM_MEDUSA_SOURCE_SITE}",
      List(DimensionJoinCondition(
        Map("subjectCode" -> "subject_code"),
        null,null,null
      ),
        DimensionJoinCondition(
          Map("subjectName" -> "subject_name"),
          null,null,null
        )
      ),
      "subject_sk")
  }
}
