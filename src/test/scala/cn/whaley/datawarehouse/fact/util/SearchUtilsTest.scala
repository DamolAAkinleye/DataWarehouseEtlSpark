package cn.whaley.datawarehouse.fact.util

import cn.whaley.datawarehouse.fact.moretv.util.SearchUtils
import org.junit.Assert._
import org.junit.{Before, Test}

/**
  * Created by baozhiwang on 2017/5/3.
  */
class SearchUtilsTest {
  var MORETV:String=_
  var MEDUSA:String=_

  @Before def initialize() {
    MORETV = "moretv"
    MEDUSA = "medusa"
  }

  /**
    *

    */
  @Test
  def getFilterCategoryFirst: Unit ={
   val moretvTestCaseList=List(
     ("xxx","home-search-CNWD",MORETV,"home","CNWD"),
     ("xxx","home-kids_home-kids_seecartoon-search-SHIW",MORETV,"kids_seecartoon","SHIW"),
     ("xxx","home-tv-search-LIAOZHAI",MORETV,"tv","LIAOZHAI"),
     ("xxx","home-tv-search-LIAOZHAI-similar",MORETV,"tv","LIAOZHAI")
   )

    val medusaTestCaseList=List(
      ("home-search*SSJG","xxx",MEDUSA,"home","SSJG"),
      ("home*my_tv*tv-tv-search*DQD","xxx",MEDUSA,"tv","DQD"),
      ("home*my_tv*mv-mv-search*WMDMT","xxx",MEDUSA,"mv","WMDMT"),
      ("home*classification*mv-mv-search*GZQY","xxx",MEDUSA,"mv","GZQY"),
      ("home*my_tv*kids-kids_home-kandonghua-search*WW","xxx",MEDUSA,"kandonghua","WW")
    )
    val testCaseList=moretvTestCaseList++medusaTestCaseList
    testCaseList.foreach(w => {
      //println(w._1+","+w._2+","+w._3+","+w._4+","+w._5)
      val searchFrom=SearchUtils.getSearchFrom(w._1,w._2,w._3)
      val searchKeyword=SearchUtils.getSearchKeyword(w._1,w._2,w._3)
      assertEquals(w._4,searchFrom)
      assertEquals(w._5,searchKeyword)
    })
  }
}
