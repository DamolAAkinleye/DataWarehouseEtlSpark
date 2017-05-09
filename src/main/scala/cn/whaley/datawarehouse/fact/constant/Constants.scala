package cn.whaley.datawarehouse.fact.constant

import cn.whaley.datawarehouse.global.Globals._

/**
  * Created by michael on 17/5/5.
  */
object Constants {
  val FACT_HDFS_BASE_PATH_BACKUP: String = FACT_HDFS_BASE_PATH + "/backup"
  val FACT_HDFS_BASE_PATH_TMP: String = FACT_HDFS_BASE_PATH + "/tmp"
  val FACT_HDFS_BASE_PATH_DELETE: String = FACT_HDFS_BASE_PATH + "/delete"
  val THRESHOLD_VALUE = 5120000

  val schemaMedusaArray= Array("accessArea","accessLocation","accessSource","accountId","action","apkSeries","apkVersion",
    "appAnterWay","appEnterWay","appSid","belongTo","bufferTimes","buildDate","button","channelSid","collectClass",
    "collectContent","collectType","contentType","corruptKey","date","datetime","day","duration","entrance","entryWay",
    "episodeSid","episodyed_android_youku","event","groupId","happenTime","homeContent","homeLocation","homeType","ip",
    "jsonLog","liveName","liveSid","liveType","locationIndex","logType","logVersion","mark","match","matchSid",
    "newEntrance","oldEntrance","page","path","pathMain","pathSpecial","pathSub","productModel","programSid",
    "programType","promotionChannel","region","retrieval","searchText","singer","source","station","stationcode",
    "subjectCode","subscribeContent","subscribeType","switch","uploadTime","userId","versionCode","videoName",
    "videoSid","weatherCode")

  val schemaMoretvArray= Array("accessArea","accessLocation","accessSource","accountId","action","apkSeries","apkVersion",
    "appAnterWay","appEnterWay","appSid","belongTo","bufferTimes","buildDate","button","channelSid","collectClass",
    "collectContent","collectType","contentType","corruptKey","date","datetime","day","duration","entrance","entryWay",
    "episodeSid","episodyed_android_youku","event","groupId","happenTime","homeContent","homeLocation","homeType","ip",
    "jsonLog","liveName","liveSid","liveType","locationIndex","logType","logVersion","mark","match","matchSid",
    "newEntrance","oldEntrance","page","path","pathMain","pathSpecial","pathSub","productModel","programSid",
    "programType","promotionChannel","region","retrieval","searchText","singer","source","station","stationcode",
    "subjectCode","subscribeContent","subscribeType","switch","uploadTime","userId","versionCode","videoName",
    "videoSid","weatherCode")

}
