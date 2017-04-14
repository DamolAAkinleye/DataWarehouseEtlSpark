package cn.whaley.datawarehouse.dimension.whaley

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.dimension.constant.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB
import org.apache.spark.sql.DataFrame



/**
  * Created by 郭浩 on 17/4/13.
  *
  * 微鲸会员订单
  */
object MemberShipOrder extends DimensionBase {
  columns.skName = "membership_order_sk"
  columns.primaryKeys = List("membership_order_id")
  columns.trackingColumns = List()
  columns.allColumns = List("membership_order_id","product_sn","membership_account","order_id","deal_status","payment_type",
    "order_type","product_id","product_name","goods_no","prime_price","payment_amount",
    "pay_method","foreign_key","duration","duration_day","over_time","create_time",
    "delivered_time","start_time","end_time","invalid_time"
   )

  readSourceType = jdbc


  sourceDb = MysqlDB.whaleyDolphin("dolphin_whaley_account_order","id",1, 1000000000, 10)

  dimensionName = "dim_whaley_membership_order"

  override def filterSource(sourceDf: DataFrame): DataFrame = {
    val sq = sqlContext
    import sq.implicits._
    import org.apache.spark.sql.functions._

    sourceDf.filter("orderStutas ='1' and substr(sn,2) not in ('XX','XY','XZ','YX','YY','YZ','ZX')").registerTempTable("account_order")
    val dolphin_whaley_delivered_order = MysqlDB.whaleyDolphin("dolphin_whaley_delivered_order","id",1, 1000000000, 10)
    sqlContext.read.format("jdbc").options(dolphin_whaley_delivered_order).load()
      .filter("status ='1' and substr(sn,2) not in ('XX','XY','XZ','YX','YY','YZ','ZX')").registerTempTable("delivered_order")

    val sql =
      s"""
         | select
         |  CONCAT_WS("-",a.whaleyAccount,a.whaleyOrder,b.whaleyProduct) as membership_order_id,
         |  a.sn as product_sn,
         |  a.whaleyAccount as membership_account,
         |  a.whaleyOrder as order_id,
         |  a.orderStatus as deal_status,
         |  a.paymentType as payment_type,
         |  a.orderTypeId as order_type,
         |  b.whaleyProduct as product_id,
         |  b.whaleyProductName as product_name,
         |  a.goodsNo as goods_no,
         |  a.totalPrice as prime_price,
         |  a.paymentAmount as payment_amount,
         |  a.payMethod as pay_method,
         |  a.foreignKey as foreign_key,
         |  b.duration as duration,
         |  b.duration_day as duration_day,
         |  a.overTime as over_time,
         |  a.createTime as create_time,
         |  b.create_time as delivered_time,
         |  b.start_time,
         |  b.end_time,
         |  b.invalid_time
         | from
         |    account_order a
         | left join
         |    delivered_order b
         | on a.whaleyOrder=b.orderId  and a.sn = b.sn
         |
       """.stripMargin

    sqlContext.sql(sql)
  }
}
