package cn.whaley.datawarehouse.fact.whaley

import cn.whaley.datawarehouse.fact.FactEtlBase
import cn.whaley.datawarehouse.fact.constant.LogPath

/**
  * Created by Tony on 17/5/16.
  */
object DetailView extends FactEtlBase{

  topicName = "fact_whaley_detail_view"

  parquetPath = LogPath.HELIOS_DETAIL


}
