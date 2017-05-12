package cn.whaley.datawarehouse.fact.util

import cn.whaley.datawarehouse.common.{DimensionColumn, DimensionJoinCondition}
import cn.whaley.datawarehouse.global.LogConfig

/**
  * Created by michael on 2017/5/2.
  * update by wujiulin on 2017/5/8.
  * 1.修改moretv的正则表达式
  * 2.增加解析recommend_slot_index字段功能
  */
object RecommendUtils extends LogConfig {
  private val medusaReg = ("(similar|peoplealsolike|guessyoulike)-[\\S]+-([\\S]+)\\*([\\S]+)").r
  private val moretvReg = (".*-(similar|peoplealsolike|guessyoulike)").r
  private val medusaRecommendSlotIndexRex = ("^home\\*recommendation\\*(\\d+)$").r

  def getRecommendSourceType(pathSub: String, path: String, flag: String): String = {
    var result: String = null
    flag match {
      case MEDUSA => {
        if (null != pathSub) {
          medusaReg findFirstMatchIn pathSub match {
            case Some(p) => {
              result = p.group(1)
            }
            case None =>
          }
        }
      }
      case MORETV => {
        if (null != path) {
          moretvReg findFirstMatchIn path match {
            case Some(p) => {
              result = p.group(1)
            }
            case None =>
          }
        }
      }
    }
    result
  }

  def getPreviousSid(pathSub: String): String = {
    var result: String = null
    if (null != pathSub) {
      medusaReg findFirstMatchIn pathSub match {
        case Some(p) => {
          result = p.group(2)
        }
        case None =>
      }
    }
    result
  }

  def getPreviousContentType(pathSub: String): String = {
    var result: String = null
    if (null != pathSub) {
      medusaReg findFirstMatchIn pathSub match {
        case Some(p) => {
          result = p.group(3)
        }
        case None =>
      }
    }
    result
  }

  /** get recommend_slot_index from pathMain,only for Medusa log */
  def getRecommendSlotIndex(pathMain: String): String = {
    var result: String = null
    if (pathMain == null || pathMain == "") return result
    medusaRecommendSlotIndexRex findFirstMatchIn pathMain match {
      case Some(p) => {
        result = p.group(1)
      }
      case None =>
    }
    result
  }

  /** get recommend_position_sk through recommendSourceType, recommendType, previousContentType and recommendSlotIndex
   推荐维度表，获得sk
   1.首页推荐
     事实表中字段                                     维度表字段
    recommendSlotIndex                    对应     recommend_slot_index

   2.其他推荐
    事实表中字段                                     维度表字段
 recommendSourceType                    对应     recommend_position     （guessyoulike，similar，peoplealsolike）
 previousContentType                    对应     recommend_position_type (comic,hot,mv等)
 recommendType(推荐类型，日志自带)         对应     recommend_method        (0,1)[可加可不加]
    */
  def getRecommendPositionSK(): DimensionColumn = {
    new DimensionColumn("dim_medusa_recommend_position",
      List(
        //1.首页推荐
        DimensionJoinCondition(
          Map("recommendSlotIndex" -> "recommend_slot_index"),
          "recommend_algorithm='未知' and recommend_position='portalrecommend' and recommend_slot_index>0", null, null
        ),
        //2.其他推荐
        DimensionJoinCondition(
          Map("recommendSourceType" -> "recommend_position", "previousContentType" -> "recommend_position_type"
            , "recommendType" -> "recommend_method"),
          "recommend_algorithm='未知' and recommend_slot_index=0", null, null
        )
      ),
      "recommend_position_sk")
  }



}
