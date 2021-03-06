package cn.whaley.datawarehouse.fact.moretv.util

import cn.whaley.datawarehouse.common.{DimensionColumn, DimensionJoinCondition}
import cn.whaley.datawarehouse.global.LogConfig


/**
  * Created by michael on 2017/4/24.
  * 收集所有关于专题的工具类到此类中
  */
object SubjectUtils extends LogConfig{

  /**
    * 从路径中获取专题code
    */
  def getSubjectCodeByPathETL(pathSpecial:String, path: String, flag: String)= {
    var result: String = null
    flag match {
      case MEDUSA => {
        if (SUBJECT.equalsIgnoreCase(PathParserUtils.getPathMainInfo(pathSpecial, 1, 1))) {
          result = getSubjectCode(pathSpecial)
        }
      }
      case MORETV => {
        val info = getSubjectCodeAndPath(path)
        if (info.nonEmpty) {
          val subjectCode = info(0)
          result = subjectCode._1
        }
      }
      case _ =>
    }

    result
  }

  private val regex_etl="""([a-zA-Z]+)([0-9]+)""".r
  //private val regex_etl="""(movie|tv|hot|kids|zongyi|comic|jilu|sports|xiqu|mv)([0-9]+)""".r
  //private val regex_etl="""(movie|tv|hot|kids|zongyi|comic|jilu|sports|xiqu|mv|kid)([0-9]+)""".r
  private val regexSubjectName="""subject-([a-zA-Z0-9-\u4e00-\u9fa5]+)""".r
  // 获取专题code
  def getSubjectCode(subject:String) = {
    var subjectCode: String = null
    if (subject != null){
      regex_etl findFirstMatchIn subject match {
        // 如果匹配成功，说明subject包含了专题code，直接返回专题code
        case Some(m) => {
          subjectCode = m.group(1) + m.group(2)
        }
        case None =>
      }
    }
    subjectCode
  }

  /*例子：假设pathSpecial为subject-儿歌一周热播榜,解析出 儿歌一周热播榜 */
  def getSubjectNameETL(subject:String) :String= {
    regexSubjectName findFirstMatchIn subject match {
      // 如果匹配成功，说明subject包含了专题名称，直接返回专题名称
      case Some(m) => {
        m.group(1)
      }
      case None => null
    }
  }

  /**
    * 从路径中获取专题名称,对于medusa日志，可以从pathSpecial解析出subjectName；对于moretv日志，日志里面不存在subjectName打点
    *
    * @param pathSpecial medusa play pathSpecial field
    * @return subject_name string value or null
    *         Example:
    *
    *         {{{
    *                sqlContext.sql("
    *                select pathSpecial,subjectName,subjectCode
    *                from log_data
    *                where flag='medusa' and pathSpecial is not null and size(split(pathSpecial,'-'))=2").show(100,false)
    *         }}}
    **/

  def getSubjectNameByPathETL(pathSpecial: String): String = {
    var result: String = null
    if (pathSpecial != null) {
      if (SUBJECT.equalsIgnoreCase(PathParserUtils.getPathMainInfo(pathSpecial, 1, 1))) {
        val subjectCode = SubjectUtils.getSubjectCode(pathSpecial)
        val pathLen = pathSpecial.split("-").length
        if (pathLen == 2) {
          result = PathParserUtils.getPathMainInfo(pathSpecial, 2, 1)
        } else if (pathLen > 2) {
          var tempResult = PathParserUtils.getPathMainInfo(pathSpecial, 2, 1)
          if (subjectCode != null) {
            for (i <- 2 until pathLen - 1) {
              tempResult = tempResult.concat("-").concat(PathParserUtils.getPathMainInfo(pathSpecial, i + 1, 1))
            }
            result = tempResult
          } else {
            for (i <- 2 until pathLen) {
              tempResult = tempResult.concat("-").concat(PathParserUtils.getPathMainInfo(pathSpecial, i + 1, 1))
            }
            result = tempResult
          }
        }
      }
    }
    result
  }

  /** 通过专题subject_code and subject_name获得subject_sk  */
  def getSubjectSK(): DimensionColumn = {
    new DimensionColumn("dim_medusa_subject",
      List(DimensionJoinCondition(
        Map("subjectCode" -> "subject_code"),
        null, null, "subjectCode is not null"
      ),
        DimensionJoinCondition(  //先按照名字和contentType关联，防止出现不同频道重名的专题
          Map("subjectName" -> "subject_name", "contentType" -> "subject_content_type"),
          null, null, "subjectName is not null"
        ),
        DimensionJoinCondition(
          Map("subjectName" -> "subject_name"),
          null, null, "subjectName is not null"
        )
      ),
      "subject_sk")
  }

  //匹配首页上的专题
  val regexSubjectA = "home-(hotrecommend)(-\\d+-\\d+)?-(hot\\d+|movie\\d+|zongyi\\d+|tv\\d+|comic\\d+|kids\\d+|jilu\\d+|sports\\d+|mv\\d+|xiqu\\d+)".r
  //匹配首页上的专题套专题
  val regexSubjectA2 = ("home-(hotrecommend)(-\\d+-\\d+)?-(hot\\d+|movie\\d+|zongyi\\d+|tv\\d+|comic\\d+|kids\\d+|jilu\\d+|sports\\d+|mv\\d+|xiqu\\d+)-"
    + "(actor|hot\\d+|movie\\d+|zongyi\\d+|tv\\d+|comic\\d+|kids\\d+|jilu\\d+|sports\\d+|mv\\d+|xiqu\\d+)").r
  //匹配在三级页面的专题
  val regexSubjectB = "home-(movie|zongyi|tv|comic|kids|jilu|hot|sports|mv|xiqu)-(\\w+)-(movie\\d+|zongyi\\d+|tv\\d+|comic\\d+|kids\\d+|jilu\\d+|hot\\d+|sports\\d+|mv\\d+|xiqu\\d+)".r
  //匹配在三级页面的专题套专题
  val regexSubjectB2  = ("home-(movie|zongyi|tv|comic|kids|jilu|hot|mv|xiqu)-(\\w+)-(movie\\d+|zongyi\\d+|tv\\d+|comic\\d+|kids\\d+|jilu\\d+|hot\\d+|mv\\d+|xiqu\\d+)-"
    + "(actor|movie\\d+|zongyi\\d+|tv\\d+|comic\\d+|kids\\d+|jilu\\d+|hot\\d+|mv\\d+|xiqu\\d+)").r
  //匹配第三方跳转的专题
  val regexSubjectC = "(thirdparty_\\d{1})[\\w\\-]+-(movie\\d+|zongyi\\d+|tv\\d+|comic\\d+|kids\\d+|jilu\\d+|hot\\d+|sports\\d+|mv\\d+|xiqu\\d+)".r
  //匹配少儿毛推荐的专题
  val regexSubjectD = "home-kids_home-(\\w+)-(kids\\d+)".r
  //匹配少儿三级页面中的专题
  val regexSubjectE = "home-kids_home-(\\w+)-(\\w+)-(kids\\d+)".r
  //匹配历史收藏中的专题
  val regexSubjectF = "home-(history)-[\\w\\-]+-(movie\\d+|zongyi\\d+|tv\\d+|comic\\d+|kids\\d+|jilu\\d+|jilu\\d+|hot\\d+|sports\\d+|mv\\d+|xiqu\\d+)".r
  //匹配历史收藏中的专题套专题
  val regexSubjectF2 = "home-(history)-[\\w\\-]+-(movie\\d+|zongyi\\d+|tv\\d+|comic\\d+|kids\\d+|jilu\\d+|jilu\\d+|hot\\d+|sports\\d+|mv\\d+|xiqu\\d+)-(actor|movie\\d+|zongyi\\d+|tv\\d+|comic\\d+|kids\\d+|jilu\\d+|jilu\\d+|hot\\d+|sports\\d+|mv\\d+|xiqu\\d+)".r
  //暂时不清楚是匹配哪种情况，暂且保留此匹配项
  val regexSubjectG = "home-(movie|zongyi|tv|comic|kids|jilu|hot)-(movie\\d+|zongyi\\d+|tv\\d+|comic\\d+|kids\\d+|jilu\\d+|hot\\d+|sports\\d+)".r

  def getSubjectCodeAndPath(path:String) = {
    var result: List[(String, String)] = List()
    if (path != null){
      regexSubjectA2 findFirstMatchIn path match {
        case Some(a2) => result = (a2.group(4),a2.group(1))::(a2.group(3),a2.group(1))::Nil
        case None => regexSubjectA findFirstMatchIn path match {
          case Some(a) => result = (a.group(3),a.group(1))::Nil
          case None => regexSubjectB2 findFirstMatchIn path match {
            case Some(b2) => result = (b2.group(3),b2.group(2))::(b2.group(4),b2.group(2))::Nil
            case None => regexSubjectB findFirstMatchIn path match {
              case Some(b) => result = (b.group(3),b.group(2))::Nil
              case None => regexSubjectC findFirstMatchIn path match {
                case Some(c) => result = (c.group(2),c.group(1))::Nil
                case None => regexSubjectD findFirstMatchIn path match {
                  case Some(d) => result = (d.group(2),d.group(1))::Nil
                  case None => regexSubjectE findFirstMatchIn path match {
                    case Some(e) => result = (e.group(3),e.group(2))::Nil
                    case None => regexSubjectF2 findFirstMatchIn path match {
                      case Some(f2) => result = (f2.group(2),f2.group(1))::(f2.group(3),f2.group(1))::Nil
                      case None => regexSubjectF findFirstMatchIn path match {
                        case Some(f) => result = (f.group(2),f.group(1))::Nil
                        case None => regexSubjectG findFirstMatchIn path match {
                          case Some(g) => result = (g.group(2),g.group(1))::Nil
                          case None => Nil
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    result
  }
  def getSubjectCodeAndPathWithId(path:String,userId:String) = {
    getSubjectCodeAndPath(path).map(x => (x,userId))
  }


}
