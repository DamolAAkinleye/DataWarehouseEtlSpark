package cn.whaley.datawarehouse.dimension

import java.util.Date

import org.apache.commons.lang3.time.DateUtils

/**
  * Created by Tony on 17/3/8.
  *
  * 配置维度表的字段信息
  */
class Columns {

  /**
    * 代理键
    */
  var skName: String = "sk"

  /**
    * 业务键，可以是联合主键
    */
  var primaryKeys : List[String] = _

  /**
    * 需要追踪历史记录的列
    */
  var trackingColumns : List[String] = List()

  /**
    * 其他列
    */
  var otherColumns : List[String] = _

  /**
    * 生效时间
    */
  var validTimeKey : String = "dim_valid_time"

  /**
    * 失效时间
    */
  var invalidTimeKey : String = "dim_invalid_time"

  /**
    * 维度表字段与源数据字段的对应关系
    */
  var sourceColumnMap: Map[String, String] = Map()

  /**
    * 获取需要从源数据中获取信息的列
    * @return
    */
  def getSourceColumns: List[String] = {
    primaryKeys ++ trackingColumns ++ otherColumns
  }

}

