package cn.whaley.datawarehouse.fact.util

import org.junit.Assert._
import org.junit.{Test, Before}

/**
  * Created by michael on 2017/5/4.
  */
class EntranceTypeUtilsTest {
  var MORETV:String=_
  var MEDUSA:String=_

  @Before def initialize() {
    MORETV = "moretv"
    MEDUSA = "medusa"
  }

  //my_tv,classification,foundation test case


  //search,recommendation,live,setting test case

  @Test
  def getEntranceCodeByPathETLTest: Unit ={
    val my_tv_medusa_list=List(
      ("home*my_tv*movie-movie*好莱坞巨制","xxx",MEDUSA,"my_tv","movie"),
      ("home*my_tv*tv-tv-search*WSZZ","xxx",MEDUSA,"my_tv","tv"),
      ("home*my_tv*movie-movie-retrieval*hot*dongzuo*gangtai*2013","xxx",MEDUSA,"my_tv","movie"),
      ("home*my_tv*kids-kids_home-kandonghua-search*SLATM","xxx",MEDUSA,"my_tv","kids"),
      ("home*my_tv*7-sports*recommend*tvn8aco8wx9v","xxx",MEDUSA,"my_tv","sports"),
      ("home*my_tv*7-sports*League*dzjj-league*英雄联盟","xxx",MEDUSA,"my_tv","sports"),
      ("home*my_tv*mv-mv*mineHomePage*site_collect-mv_collection","xxx",MEDUSA,"my_tv","mv"),
      ("home*my_tv*mv-mv*function*site_hotsinger-mv_poster","xxx",MEDUSA,"my_tv","mv")
    )

    val foundation_medusa_list=List(
      ("home*foundation*top_new-rank*top_new","xxx",MEDUSA,"foundation","top_new"),
      ("home*foundation*top_new","xxx",MEDUSA,"foundation","top_new"),
      ("home*foundation*top_star-rank*top_star","xxx",MEDUSA,"foundation","top_star"),
      ("home*foundation*interest_location","xxx",MEDUSA,"foundation","interest_location"),
      ("home*foundation*top_collect-rank*top_collect","xxx",MEDUSA,"foundation","top_collect"),
      ("home*foundation*interest_location-everyone_watching-everyone_nearby","xxx",MEDUSA,"foundation","interest_location"),
      ("home*foundation*top_star","xxx",MEDUSA,"foundation","top_star"),
      ("home*foundation*top_hot-rank*top_hot","xxx",MEDUSA,"foundation","top_hot"),
      ("home*foundation*interest_location-everyone_watching","xxx",MEDUSA,"foundation","interest_location")
    )

    val classification_medusa_list=List(
      ("home*classification*mv-mv*function*site_dance-mv_category*舞蹈教程","xxx",MEDUSA,"classification","mv"),
      ("home*classification*kids-kids_home-kandonghua-search*XSL","xxx",MEDUSA,"classification","kids"),
      ("home*classification*tv-tv-retrieval*hot*all*oumei*2017","xxx",MEDUSA,"classification","tv"),
      ("home*classification*zongyi-zongyi-search*ER ","xxx",MEDUSA,"classification","zongyi"),
      ("home*classification*3-sports*League*dzjj-league*王者荣耀","xxx",MEDUSA,"classification","sports"),
      ("home*classification*3-sports*horizontal*tvn8ackmikab","xxx",MEDUSA,"classification","sports")
    )

    val live_medusa_list=List(
      ("home*live*z206","xxx",MEDUSA,"live",null),
      ("home*live*eagle-movie-retrieval*hot*dongzuo*all*2016","xxx",MEDUSA,"live",null),
      ("home*live*eagle-kids_home-tingerge*随便听听","xxx",MEDUSA,"live",null),
      ("home*live*eagle-movie*犀利动作","xxx",MEDUSA,"live",null),
      ("home*live*s9n8opxyfhbc","xxx",MEDUSA,"live",null)
    )

    val recommendation_medusa_list=List(
      ("home*recommendation*1-hot*今日焦点","xxx",MEDUSA,"recommendation",null),
      ("home*recommendation*1-hot-hot","xxx",MEDUSA,"recommendation",null),
      ("home*recommendation*1-hot-search*YRYJZ","xxx",MEDUSA,"recommendation",null),
      ("home*recommendation*2","xxx",MEDUSA,"recommendation",null),
      ("home*recommendation*1-hot-hot-hot-hot*新闻热点","xxx",MEDUSA,"recommendation",null)
    )

    val search_medusa_list=List(
      ("home-search*DZL","xxx",MEDUSA,"search",null),
      ("tv-search*JUEDA","xxx",MEDUSA,"search",null),
      ("search*BP","xxx",MEDUSA,"search",null)
    )

    val setting_medusa_list=List(
      ("home*navi*setting-setmain-setaccount","xxx",MEDUSA,"setting",null),
      ("home*navi*setting-setmain","xxx",MEDUSA,"setting",null)
    )

    val live_moretv_list=List(
      ("xxx","home-live-1-s9n8opxyv0x0-jingpin-similar",MORETV,"live",null),
      ("xxx","home-live-1-12oq12k7a23e-jingpin",MORETV,"live",null),
      ("xxx","home-TVlive-1-s9n8op9wnpwx-jingpin",MORETV,"live",null)
    )

    val testCaseList=my_tv_medusa_list++foundation_medusa_list++classification_medusa_list++
      live_medusa_list++recommendation_medusa_list++search_medusa_list++setting_medusa_list++live_moretv_list
    //println("首页入口维度工具类测试用例")
    testCaseList.foreach(w => {
      //println(w._1+","+w._2+","+w._3+","+w._4+","+w._5)
      val areaCode=EntranceTypeUtils.getEntranceAreaCode(w._1,w._2,w._3)
      val locationCode=EntranceTypeUtils.getEntranceLocationCode(w._1,w._2,w._3)
      assertEquals(w._4,areaCode)
      assertEquals(w._5,locationCode)
    })
  }

}
