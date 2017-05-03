package cn.whaley.datawarehouse.fact.util

import org.junit.Test
import org.junit.Assert._
import org.junit.Before
/**
  * Created by michael on 2017/5/3.
  */
class FilterCategoryUtilsTest {
  var MORETV:String=_
  var MEDUSA:String=_

  @Before def initialize() {
        MORETV = "moretv"
        MEDUSA = "medusa"
  }

  @Test
  def getFilterCategoryFirst: Unit ={
    val testCaseList = List(
      ("home*classification*movie-movie-retrieval*hot*kongbu*qita*all","xxx",MEDUSA,"hot"),
      ("home*classification*movie-movie-retrieval*hot*xiju*all*all","xxx",MEDUSA,"hot"),
      ("xxx","home-movie-multi_search-new-all-all-all",MORETV,"new"))
      testCaseList.foreach(w => {
        val firstFilterCategory=FilterCategoryUtils.getFilterCategoryFirst(w._1,w._2,w._3)
        assertEquals(w._4,firstFilterCategory)
    })
  }

}
