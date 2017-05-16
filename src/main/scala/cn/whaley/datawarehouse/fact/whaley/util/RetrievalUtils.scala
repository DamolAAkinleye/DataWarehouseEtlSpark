package cn.whaley.datawarehouse.fact.whaley.util

/**
  * Created by huanghu 2017/5/15.
  * 收集所有关于筛选的信息工具类到此类中
  */
object RetrievalUtils {
  //筛选
  def getSortType(retrieval: String) = {

    if(retrieval != null){
      val totalInformation = retrieval.split("-")
      totalInformation(0)
    }else null
  }

  def getFilterCategoryFirst(retrieval: String) = {

    if(retrieval != null){
      val totalInformation = retrieval.split("-")

      totalInformation(1)

    }else null

  }

  def getFilterCategorySecond(retrieval: String) = {

    if(retrieval != null){
      val totalInformation = retrieval.split("-")

      totalInformation(2)

    }else null

  }

  def getFilterCategoryThird(retrieval: String) = {

    if(retrieval != null){
      val totalInformation = retrieval.split("-")
      if(totalInformation.length == 4)
      totalInformation(3)
      else if (totalInformation.length == 5)
        totalInformation(3) + "-" + totalInformation(4)

    }else null

  }


}


