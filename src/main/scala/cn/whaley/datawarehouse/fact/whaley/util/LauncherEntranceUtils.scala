package cn.whaley.datawarehouse.fact.whaley.util

/**
 * Created by zhangyu on 17/5/16.
 * 解析微鲸播放日志首页入口维度的几个指标
 */
object LauncherEntranceUtils{

  /**
   * 获取首页WUI版本
   * @param romVersion
   * @return
   */
  def wuiVersionFromPlay(romVersion:String,firmwareVersion:String):String = {
    val wui = RomVersionUtils.getRomVersion(romVersion,firmwareVersion)
    if(wui == null || wui.isEmpty){
      null
    }else{
      val startIndex = wui.indexOf(".")
      if(startIndex > 0){
        val wuiVersion = wui.substring(0,startIndex)
        val tmp = wui.split('.')
        if(tmp.length == 4 || wuiVersion =="02"){
          "02"
        } else if(wuiVersion == "00" || wuiVersion == "01") {
         "01" //将00版本替换为01版本
        }else null
      }else null
    }
  }

  /**
   * 获取WUI首页各行的入口维度
   * @param path
   * @param linkValue
   * @return
   */

  def launcherAccessLocationFromPath(path:String,linkValue:String):String = {
    if(path == null || path.isEmpty) {
      null
    }else{
      val tmp = path.split("-")
      if(tmp.length >= 2){
        val secondPath = tmp(1)
        getAccessLocation(secondPath,linkValue)
      }else null
    }
  }

  /**
   * 获取WUI首页今日推荐/精选推荐的推荐位
   * @param recommendLocation
   * @return
   */

  def  launcherLocationIndexFromPlay(recommendLocation:String):Int = {
    if(recommendLocation == null || recommendLocation.isEmpty){
      -1
    }else{
      if(recommendLocation.contains("-")){   //剔除01版本中的大小推荐位信息
        val tmp = recommendLocation.split("-")
        tmp(0).toInt + 1
      }else {recommendLocation.toInt + 1}
    }
  }

  /**
   * 将 发现 一行的首页入口替换为具体的榜单信息
   * 处理hot11的路径bug(home-hot11)
   * @param secondPath
   * @param linkValue
   * @return
   */

  def getAccessLocation(secondPath:String,linkValue:String):String = {
    if(secondPath == "top"){
      linkValue
    }else if(secondPath == "hot11"){
      "recommendation"
    }else secondPath
  }




}
