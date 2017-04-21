package cn.whaley.datawarehouse.fact.common

/**
  * Created by Tony on 17/4/6.
  */
case class DimensionColumn(dimensionName: String,
                           joinColumnList: List[DimensionJoinCondition], //list内的是或关系
                           dimensionSkName: String
                          ) {

}
