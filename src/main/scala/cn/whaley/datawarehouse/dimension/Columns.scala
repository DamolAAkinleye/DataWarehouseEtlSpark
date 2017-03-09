package cn.whaley.datawarehouse.dimension

/**
  * Created by Tony on 17/3/8.
  */
class Columns {

  var primaryKeys : List[String] = _

  var trackingColumns : List[String] = _

  var otherColumns : List[String] = _

  var skName: String = "sk"

  var validTimeKey : String = "dim_valid_time"

  var invalidTimeKey : String = "dim_invalid_time"

  var sourceColumnMap: Map[String, String] = Map()

  def getSourceColumns: List[String] = {
    primaryKeys ++ trackingColumns ++ otherColumns
  }

}
