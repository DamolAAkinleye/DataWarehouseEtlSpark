package cn.whaley.datawarehouse.fact.whaley.util

/**
  * 创建人：郭浩
  * 创建时间：2017/5/15
  * 程序作用：站点树解析
  * 数据输入：
  * 数据输出：
  */
object ListCategoryUtils {
  /**
    * 获取站点树
    * @param path
    */
  def getLastCategory(path:String):String ={
    var sourceSite :String = null
    val paths = path.split("-")
    if(paths.length >2){
      val contentType = paths(1)
         contentType match {
           case "sports" => sourceSite ={
             //sports 站点树取第四位
             if(paths.length>=4){
               sourceSite= paths(3)
             }
             sourceSite
           }
           case "mv"  =>sourceSite={
             //mv 站点树取第5位

             if(paths.length>=5){
               sourceSite= paths(4)
             }
             sourceSite
           }
           case "kids" =>sourceSite = {
             val kids_type=paths(2)

             kids_type match{
               //鲸鲸学院->精选推荐
               case "kids_value_added_package" => sourceSite = path
               case _ =>  sourceSite={
                 if(paths.length>=4){
                   sourceSite= paths(3)
                 }
                 sourceSite
               }
             }
             sourceSite
           }
           case _  => sourceSite = {
             //其他频道 站点树取第3位
             if(paths.length>=3){
               sourceSite= paths(2)
             }
             sourceSite
           }
         }
    }
    sourceSite
  }

  /**
    * 取前一个
    * @param path
    * @return
    */
  def getListThirdCategory(path:String):String ={
    var thirdCategory :String = null
    val paths = path.split("-")
    if(paths.length >2){
      val contentType = paths(1)
      contentType match {

        case "sports" => thirdCategory ={
          //sports 站点树取第四位 ok
          if(paths.length>=4){
            thirdCategory= paths(2)
          }
          thirdCategory
        }
        case "mv"  =>thirdCategory={
          //mv 站点树取第5位 ok
          if(paths.length>=5){
            thirdCategory= paths(3)
          }
          thirdCategory
        }
        case "kids" =>thirdCategory = {
          val kids_type=paths(2)

          kids_type match{
            //鲸鲸学院->精选推荐
            case "kids_value_added_package" => thirdCategory = path
            case _ =>  thirdCategory={
              if(paths.length>=4){
                //需要处理
                thirdCategory= paths(2)
                thirdCategory match{
                  //鲸鲸学园
                  case "wcampus" => thirdCategory="kids_value_added_package"
                   //看动画片
                   case "animation" => thirdCategory="show_kidsSite"
                  //听儿歌
                  case "rhyme" => thirdCategory="show_kidsSongSite"
                   //学知识
                  case "learn" => thirdCategory="kids_learning"
                  case "recommendation" => thirdCategory="kids_scroll"

                }
              }
              thirdCategory
            }
          }
          thirdCategory
        }
        case _  => thirdCategory = {
          //其他频道 站点树取第3位 ok
          if(paths.length>=3){
            thirdCategory= contentType
          }
          thirdCategory
        }
      }
    }
    thirdCategory
  }


  def contentType(path:String):String={
    var contentType:String = null
    val paths = path.split("-")
    if(paths.length >2){
      contentType = paths(1)
    }
    contentType
  }


  def main(args: Array[String]): Unit = {
    val paths = Array("home-mv-class-site_mvstyle-1_mv_style_pop",
      "home-sports-cba-league_matchreplay","home-kids-kids_value_added_package-kids_jingxuanzhuanqu-kids_bbc_animation-kids_bbc_hot-movie851")

    paths.foreach(path=>{
      println(getLastCategory(path))
    })
  }

}
