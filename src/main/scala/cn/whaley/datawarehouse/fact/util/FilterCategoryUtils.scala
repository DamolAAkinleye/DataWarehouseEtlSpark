package cn.whaley.datawarehouse.fact.util

import cn.whaley.datawarehouse.global.LogConfig
/**
  * Created by michael on 2017/5/2.
  */
object FilterCategoryUtils extends LogConfig{
  private val regex_moretv_filter = (".*multi_search-(hot|new|score)-([\\S]+?)-([\\S]+?)-(all|qita|[0-9]+[-0-9]*)").r
  private val regex_medusa_filter = (".*retrieval\\*(hot|new|score)\\*([\\S]+?)\\*([\\S]+?)\\*(all|qita|[0-9]+[\\*0-9]*)").r

  //获取筛选维度【排序方式：最新、最热、得分；标签；地区；年代】
  def getFilterCategory(path: String, index: Int, flag: String): String = {
    var result: String = null
    if (null == path) {
      result = null
    } else if (index > 4) {
      result = null
    } else {
      flag match {
        case MEDUSA => {
          regex_medusa_filter findFirstMatchIn path match {
            case Some(p) => {
              result = p.group(index)
            }
            case None =>
          }
        }
        case MORETV => {
          regex_moretv_filter findFirstMatchIn path match {
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

  def getFilterCategoryFirst(pathMain: String, path: String, flag: String): String = {
    var result: String = null
    flag match {
      case MEDUSA => {
        result = getFilterCategory(pathMain,1,MEDUSA)
      }
      case MORETV => {
        result = getFilterCategory(path,1,MORETV)
      }
    }
    result
  }

  def getFilterCategorySecond(pathMain: String, path: String, flag: String): String = {
    var result: String = null
    flag match {
      case MEDUSA => {
        result = getFilterCategory(pathMain,2,MEDUSA)
      }
      case MORETV => {
        result = getFilterCategory(path,2,MORETV)
      }
    }
    result
  }

  def getFilterCategoryThird(pathMain: String, path: String, flag: String): String = {
    var result: String = null
    flag match {
      case MEDUSA => {
        result = getFilterCategory(pathMain,3,MEDUSA)
      }
      case MORETV => {
        result = getFilterCategory(path,3,MORETV)
      }
    }
    result
  }

  def getFilterCategoryFourth(pathMain: String, path: String, flag: String): String = {
    var result: String = null
    flag match {
      case MEDUSA => {
        result = getFilterCategory(pathMain,4,MEDUSA)
      }
      case MORETV => {
        result = getFilterCategory(path,4,MORETV)
      }
    }
    result
  }

}
