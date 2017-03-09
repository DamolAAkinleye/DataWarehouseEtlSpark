package cn.whaley.datawarehouse.dimension

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
  var primaryKeys: List[String] = _

  /**
    * 需要追踪历史记录的列
    * 配置注意事项：对于已经存在的维度表，增加追踪列只能从otherColumns中移入，不能直接增加新列；可以减少列
    */
  var trackingColumns: List[String] = List()

  /**
    * 其他列
    * 可以在维度生成或自由增加或减少列
    */
  var otherColumns: List[String] = _

  /**
    * 生效时间
    */
  var validTimeKey: String = "dim_valid_time"

  /**
    * 失效时间
    */
  var invalidTimeKey: String = "dim_invalid_time"


  /**
    * 获取需要从源数据中获取信息的列
    *
    * @return
    */
  def getSourceColumns: List[String] = {
    primaryKeys ++ trackingColumns ++ otherColumns
  }

}

