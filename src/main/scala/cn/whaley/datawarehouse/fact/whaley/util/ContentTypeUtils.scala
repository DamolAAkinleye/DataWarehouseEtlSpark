package cn.whaley.datawarehouse.fact.whaley.util

/**
 * Created by zhangyu on 17/5/18.
 * 存储相关的contentType的函数
 */
object ContentTypeUtils {

  def getContentType(path: String, contentType: String): String = {
    if (path == null || path.isEmpty) {
      contentType
    }else {
      val tmp = path.split("-")
      if (tmp.length >= 2) {
        if(tmp(1) == "my_tv"){
          contentType
        } else tmp(1)
      } else contentType
    }
  }

}
