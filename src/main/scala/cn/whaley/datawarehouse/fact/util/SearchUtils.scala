package cn.whaley.datawarehouse.fact.util

import cn.whaley.datawarehouse.common.{DimensionColumn, DimensionJoinCondition}
import cn.whaley.datawarehouse.global.LogConfig

/**
  * Created by michael on 2017/5/3.
  */
object SearchUtils extends LogConfig {

  private val regex_search_medusa = (".*-([\\w]+)-search\\*([\\w]+)[\\*]?.*").r
  private val regex_search_medusa_home = ("(home)-search\\*([\\w]+)[\\*]?.*").r
  private val regex_search_moretv = (".*-([\\w]+)-search-([\\w]+)[-]?.*").r
  private val regex_search_moretv_home = ("(home)-search-([\\w]+)[-]?.*").r

  def getSearchFrom(pathMain: String, path: String, flag: String): String = {
    var result: String = null
    flag match {
      case MEDUSA => {
        result = getSearchDimension(pathMain, 1, flag)
      }
      case MORETV => {
        result = getSearchDimension(path, 1, flag)
      }
    }
    result
  }

  def getSearchKeyword(pathMain: String, path: String, flag: String): String = {
    var result: String = null
    flag match {
      case MEDUSA => {
        result = getSearchDimension(pathMain, 2, flag)
      }
      case MORETV => {
        result = getSearchDimension(path, 2, flag)
      }
    }
    result
  }

  private def getSearchDimension(path: String, index: Int, flag: String): String = {
    var result: String = null
    if (null != path) {
      flag match {
        case MEDUSA => {
          regex_search_medusa findFirstMatchIn path match {
            case Some(p) => {
              result = p.group(index)
            }
            case None =>
          }
          regex_search_medusa_home findFirstMatchIn path match {
            case Some(p) => {
              result = p.group(index)
            }
            case None =>
          }
        }
        case MORETV => {
          regex_search_moretv findFirstMatchIn path match {
            case Some(p) => {
              result = p.group(index)
            }
            case None =>
          }
          regex_search_moretv_home findFirstMatchIn path match {
            case Some(p) => {
              result = p.group(index)
            }
            case None =>
          }
        }
      }
    }
    result
  }

 /** 搜索维度表，获得sk
  事实表中字段                              维度表中字段
  searchFrom  (udf解析出字段)        对应   search_from
  resultIndex (日志自带)            对应    search_result_index
  tabName     (日志自带)            对应    search_tab
  extraPath   (日志自带)            对应    search_from_hot_word
  */
 def getSearchSK(): DimensionColumn = {
   new DimensionColumn("dim_medusa_search",
     List(
       DimensionJoinCondition(
         Map("searchFrom" -> "search_from","resultIndex" -> "search_result_index","tabName" -> "search_tab","extraPath" -> "search_from_hot_word"),
         null, null, null
       )
     ),
     "search_sk")
 }
}
