package cn.whaley.datawarehouse.util

import java.util.Date

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

/**
  * Created by Tony on 17/1/24.
  */
object DataFrameUtil {

  def dfZipWithIndex(df: DataFrame,
                     colName: String = "sk",
                     offset: Long = 0,
                     inFront: Boolean = true
                    ): DataFrame = {
    df.sqlContext.createDataFrame(
      df.rdd.zipWithIndex.map(ln =>
        Row.fromSeq(
          (if (inFront) Seq(ln._2 + offset + 1) else Seq())
            ++ ln._1.toSeq ++
            (if (inFront) Seq() else Seq(ln._2 + offset + 1))
        )
      ),
      StructType(
        (if (inFront) Array(StructField(colName, LongType, false)) else Array[StructField]())
          ++ df.schema.fields ++
          (if (inFront) Array[StructField]() else Array(StructField(colName, LongType, false)))
      )
    )
  }

  def addDimTime(df: DataFrame,
                 validTime: Date,
                 invalidTime: Date
                ): DataFrame = {
    df.sqlContext.createDataFrame(
      df.rdd.map(ln =>
        Row.fromSeq(ln.toSeq ++
          Seq(if (validTime == null) null else new java.sql.Timestamp(validTime.getTime),
            if (invalidTime == null) null else new java.sql.Timestamp(invalidTime.getTime)))
      )
      ,
      StructType(
        df.schema.fields
          ++ Array(StructField("dim_valid_time", TimestampType, false)
          , StructField("dim_invalid_time", TimestampType, false)))
    )
  }
}
