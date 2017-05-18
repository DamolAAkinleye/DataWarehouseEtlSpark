package cn.whaley.datawarehouse.fact.whaley.util

/**
  * Created by Tony on 17/5/18.
  */
object SearchUtils {

  def getSearchFrom(path: String):String = {
    if(path == null || path.indexOf("-search") == -1) {
      null
    } else if(path.indexOf("home-search") == 0){
      "home"
    }else {
      val tmp = path.split("-")
      if (tmp.length >= 2) {
        tmp(1)
      } else "未知"
    }
  }

  def getSearchResultIndex(searchResultIndex: String):Int = {
    try {
      if (searchResultIndex == null || searchResultIndex.isEmpty) {
        -1
      } else if (searchResultIndex.trim.toInt + 1 > 100) {
        -2
      } else {
        searchResultIndex.trim.toInt + 1
      }
    }catch {
      case ex: Exception => -1
    }
  }

  def isHotSearchWord(hotSearchWord: String): Int = {
    if(hotSearchWord == null || hotSearchWord.isEmpty) {
      0
    } else {
      1
    }
  }

  def isAssociationalSearchWord(searchAssociationalWord: String): Int = {
    if(searchAssociationalWord == null || searchAssociationalWord.isEmpty) {
      0
    } else {
      1
    }
  }

}
