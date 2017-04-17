package cn.whaley.datawarehouse.dimension.whaley

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.dimension.constant.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB
import org.apache.spark.sql.DataFrame


/**
  * Created by 郭浩 on 17/4/13.
  *
  * 微鲸会员商品
  */
object MemberShipGoods extends DimensionBase {
  columns.skName = "membership_goods_sk"
  columns.primaryKeys = List("goods_no")
  columns.trackingColumns = List()
  columns.allColumns = List("goods_no","goods_name",
    "goods_tag","prime_price","discount","real_price","status","goods_type",
    "goods_title", "is_display","create_time","update_time","publish_time",
    "start_time","end_time")

  readSourceType = jdbc

  sourceDb = MysqlDB.whaleyDolphin("dolphin_whaley_goods","id",1, 1000000000, 10)

  dimensionName = "dim_whaley_membership_goods"

  override def filterSource(sourceDf: DataFrame): DataFrame = {
    val sq = sqlContext
    import sq.implicits._
    import org.apache.spark.sql.functions._
    sourceDf.
      select(
        $"goodsNo".as(columns.allColumns(0)),
        $"goodsName".as(columns.allColumns(1)),
        $"goodsUseTag".as(columns.allColumns(2)),
        $"primePrice".as(columns.allColumns(3)),
        $"discount".cast("double")as(columns.allColumns(4)),
        $"realPrice".as(columns.allColumns(5)),
        $"goodsStatus".as(columns.allColumns(6)),
        $"goodsType".as(columns.allColumns(7)),
        $"goodsTitle".as(columns.allColumns(8)),
        $"isDisplay".as(columns.allColumns(9)),
        $"createTime".cast("timestamp").as(columns.allColumns(10)),
        $"updateTime".cast("timestamp").as(columns.allColumns(11)),
        $"publishTime".cast("timestamp").as(columns.allColumns(12)),
        $"startTime".cast("timestamp").as(columns.allColumns(13)),
        $"endTime".cast("timestamp").as(columns.allColumns(14))
      )
  }

}
