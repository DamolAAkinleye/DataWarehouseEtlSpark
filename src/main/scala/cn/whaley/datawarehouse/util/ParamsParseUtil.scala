package cn.whaley.datawarehouse.util

import scopt.OptionParser

/**
  * Created by baozhiwang on 2017/3/6.
  */
object ParamsParseUtil {

  private val default = Params()

  private val readFormat = DateFormatUtils.readFormat

  def parse(args: Seq[String], default: Params = default): Option[Params] = {
    if (args.nonEmpty) {
      val parser = new OptionParser[Params]("ParamsParse") {
        head("ParamsParse", "1.2")
        opt[Boolean]("isOnline").action((x, c) => c.copy(isOnline = x))
        opt[Boolean]("isBackup").action((x, c) => c.copy(isBackup = x))
        opt[String]("startDate").action((x, c) => c.copy(startDate = x)).
          validate(e => try {
            readFormat.parse(e)
            success
          } catch {
            case e: Exception => failure("wrong date format, should be 'yyyyMMdd'")
          })
      }
      parser.parse(args, default) match {
        case Some(p) => Some(p)
        case None => throw new RuntimeException("parse error")
      }
    } else {
      throw new RuntimeException("args is empty,at least need --isOnline")
    }
  }

}
