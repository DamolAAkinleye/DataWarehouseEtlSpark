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
  def getLastFirstCode(path:String):String ={
    var lastFirstCode :String = null
    val paths = path.split("-")
    if(paths.length >2){
      val contentType = paths(1)
         contentType match {
           case "sports" => lastFirstCode ={
             //sports 站点树取第四位
             if(paths.length>=4){
               lastFirstCode= paths(3)
             }
             lastFirstCode
           }
           case "mv"  =>lastFirstCode={
             //mv 站点树取第5位

             if(paths.length>=5){
               lastFirstCode= paths(4)
             }
             lastFirstCode
           }
           case "kids" =>lastFirstCode = {
             val kids_type=paths(2)

             kids_type match{
               //鲸鲸学院->精选推荐
               case "kids_value_added_package" =>
                 lastFirstCode = {
                   if(paths.length>=6){
                     lastFirstCode = paths(5)
                   }
                   lastFirstCode
                 }
               case _ =>  lastFirstCode={
                 if(paths.length>=4){
                   lastFirstCode= paths(3)
                 }
                 lastFirstCode
               }
             }
             lastFirstCode
           }
           case _  => lastFirstCode = {
             //其他频道 站点树取第3位
             if(paths.length>=3){
               lastFirstCode= paths(2)
             }
             lastFirstCode
           }
         }
    }
    lastFirstCode
  }

  /**
    * 取前一个
    * @param path
    * @return
    */
  def getLastSecondCode(path:String):String ={
    var lastSecondCode :String = null
    val paths = path.split("-")
    if(paths.length >2){
      val contentType = paths(1)
      contentType match {

        case "sports" => lastSecondCode ={
          //sports 站点树取第四位 ok
          if(paths.length>=4){
            lastSecondCode= paths(2)
          }
          lastSecondCode
        }
        case "mv"  =>lastSecondCode={
          //mv 站点树取第5位 ok
          if(paths.length>=5){
            lastSecondCode= paths(3)
          }
          lastSecondCode
        }
        case "kids" =>lastSecondCode = {
          val kids_type=paths(2)
          kids_type match{
            //鲸鲸学院->精选推荐
            case "kids_value_added_package" =>
              lastSecondCode ={
                if(paths.length>=6){
                  lastSecondCode = paths(4)
                }
                lastSecondCode
              }
            case _ =>
              lastSecondCode={
                if(paths.length>=4){
                  //需要处理
                  lastSecondCode= paths(2)
                  lastSecondCode match{
                    //鲸鲸学园
                    case "wcampus" => lastSecondCode="kids_value_added_package"
                     //看动画片
                     case "animation" => lastSecondCode="show_kidsSite"
                    //听儿歌
                    case "rhyme" => lastSecondCode="show_kidsSongSite"
                     //学知识
                    case "learn" => lastSecondCode="kids_learning"
                    case "recommendation" => lastSecondCode="kids_scroll"

                  }
                }
                lastSecondCode
              }
          }
          lastSecondCode
        }
        case _  => lastSecondCode = {
          //其他频道 站点树取第3位 ok
          if(paths.length>=3){
            lastSecondCode= contentType
          }
          lastSecondCode
        }
      }
    }
    lastSecondCode
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
      println(getLastSecondCode(path)+" :  "+getLastFirstCode(path))
    })
  }

}
