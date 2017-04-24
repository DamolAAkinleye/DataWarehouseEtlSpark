package cn.whaley.datawarehouse.fact.util

/**
  * Created by michael on 2017/4/24.
  */
object EntranceTypeUtils {
  /*get from com.moretv.bi.report.medusa.newsRoomKPI.ChannelEntrancePlayStat
     用来做 不同入口播放统计
    */
  private val sourceRe = ("(home\\*classification|search|home\\*my_tv\\*history|" +
    "home\\*my_tv\\*collect|home\\*recommendation|home\\*my_tv\\*[a-zA-Z0-9&\\u4e00-\\u9fa5]{1,})").r
  private val sourceRe1 = ("(classification|history|hotrecommend|search)").r

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
}
