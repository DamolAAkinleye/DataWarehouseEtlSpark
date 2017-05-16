package cn.whaley.datawarehouse.fact.whaley

import cn.whaley.datawarehouse.common.{DimensionColumn, DimensionJoinCondition, UserDefinedColumn}
import cn.whaley.datawarehouse.fact.FactEtlBase
import cn.whaley.datawarehouse.fact.constant.LogPath
import cn.whaley.datawarehouse.fact.whaley.util.SubjectUtils
import org.apache.spark.sql.functions.udf

/**
  * Created by Tony on 17/5/16.
  */
object Play extends FactEtlBase{

  topicName = "fact_whaley_play"

  parquetPath = LogPath.HELIOS_PLAY

  factTime = null

  addColumns = List(
    UserDefinedColumn("subject_code", udf(SubjectUtils.getSubjectCode: String => String), List("path"))
  )

  columnsFromSource = List(
    ("duration", "duration"),
    ("subject_code", "subject_code")
  )

  dimensionColumns = List(
    new DimensionColumn("dim_whaley_subject",
      List(DimensionJoinCondition(Map("subject_code" -> "subject_code"))), "subject_sk")
  )

}
