package cn.whaley.datawarehouse.fact.whaley

import cn.whaley.datawarehouse.common.{DimensionColumn, DimensionJoinCondition, UserDefinedColumn}
import cn.whaley.datawarehouse.fact.FactEtlBase
import cn.whaley.datawarehouse.fact.constant.LogPath
import cn.whaley.datawarehouse.fact.whaley.util.{EntranceTypeUtils, SubjectUtils}
import org.apache.spark.sql.functions.udf

/**
  * Created by Tony on 17/5/16.
  */
object Play extends FactEtlBase {

  topicName = "fact_whaley_play"

  parquetPath = LogPath.HELIOS_PLAY

  factTime = null

  addColumns = List(
    UserDefinedColumn("subject_code", udf(SubjectUtils.getSubjectCode: String => String), List("path")),
    UserDefinedColumn("wui_version", udf(EntranceTypeUtils.wuiVersionFromPlay: (String, String) => String),
      List("romVersion", "firmwareVersion")),
    UserDefinedColumn("launcher_access_location",
      udf(EntranceTypeUtils.launcherAccessLocationFromPath: (String, String) => String),
      List("path", "linkValue")),
    UserDefinedColumn("launcher_location_index",
      udf(EntranceTypeUtils.launcherLocationIndexFromPlay: String => Int),
      List("recommendLocation"))

  )

  columnsFromSource = List(
    ("duration", "duration"),
    ("subject_code", "subject_code"),
    ("wui_version", "wui_version"),
    ("launcher_access_location", "launcher_access_location"),
    ("launcher_location_index", "launcher_location_index")
  )

  dimensionColumns = List(
    new DimensionColumn("dim_whaley_subject",
      List(DimensionJoinCondition(Map("subject_code" -> "subject_code"))), "subject_sk"),
    new DimensionColumn("dim_whaley_launcher_entrance",
      List(DimensionJoinCondition(
        Map("wui_version" -> "launcher_version",
          "launcher_access_location" -> "access_location_code",
          "launcher_location_index" -> "launcher_location_index")),
        DimensionJoinCondition(
          Map("wui_version" -> "launcher_version",
            "launcher_access_location" -> "access_location_code"),
          "launcher_location_index = -1")
      ), "launcher_entrance_sk")
  )

}
