package cn.whaley.datawarehouse.util

import scopt.OptionParser

/**
  * Created by baozhiwang on 2017/3/6.
  */
object ParamsParseUtil {

  private val default = Params()

  def parse(args: Seq[String], default: Params = default): Option[Params] = {
    if (args.nonEmpty) {
      val parser = new OptionParser[Params]("ParamsParse") {
        head("ParamsParse", "1.2")
        opt[Map[String, String]]("paramMap").valueName("k1=v1,k2=v2...").action((x, c) => c.copy(paramMap = x)).
          text("param Map[String,String]")
        opt[Boolean]("isOnline").action((x, c) => c.copy(isOnline = x))
        opt[Boolean]("deleteOld").action((x, c) => c.copy(deleteOld = x))
        opt[String]("dimensionType").action((x, c) => c.copy(dimensionType = x))
      }
      parser.parse(args, default) match {
        case Some(p) =>Some(p)
        case None => throw new RuntimeException("parse error")
      }
    } else {
      throw new RuntimeException("args is empty,at least need --isOnline true")
    }
  }

}
