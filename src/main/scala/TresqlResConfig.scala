package org.wabase

import com.typesafe.config.{Config, ConfigFactory}
import org.tresql.{Cache, Dialect, Logging}

trait TresqlResConf {
  def functionSignatureResource: String = null
  def macrosResource: String = null
  def macros: Any = null
  def dialect: Dialect = null
  def idExpr: String => String = null
  def queryTimeout: Int = -1
  def fetchSize: Int = -1
  def maxResultSize: Int = -1
  def recursiveStackDepth: Int = -1
  def cache: Cache = null
  def bindVarLogFilter: Logging#BindVarLogFilter = null
  def logger: Logging#TresqlLogger = null
}

object TresqlResConf {

  def tresqlResConf(dbPath: String, config: Config): TresqlResConf = {
    val dbConf = config.getConfig(dbPath)
    val tresqlConf =
      if (dbConf.hasPath("tresql")) dbConf.getConfig("tresql")
      else ConfigFactory.empty
    val tresqlConfInstance =
      if (tresqlConf.hasPath("config-class"))
        Class.forName(tresqlConf.getString("config-class")).newInstance().asInstanceOf[TresqlResConf]
      else new TresqlResConf {}

    val tresqlConfFromConfig =
      new TresqlResConf {
        override val functionSignatureResource: String =
          if (tresqlConf.hasPath("function-signatures"))
            tresqlConf.getString("function.signatures")
          else null
        override val macrosResource: String =
          if (tresqlConf.hasPath("macros"))
            tresqlConf.getString("macros")
          else null
        override val macros: Any =
          if (tresqlConf.hasPath("macros-class"))
            Class.forName(tresqlConf.getString("macros-class")).newInstance()
          else null
        override val queryTimeout: Int =
          if (tresqlConf.hasPath("query-timeout"))
            tresqlConf.getInt("query-timeout")
          else -1
        override val fetchSize: Int =
          if (tresqlConf.hasPath("fetch-size"))
            tresqlConf.getInt("fetch-size")
          else -1
        override val maxResultSize: Int =
          if (tresqlConf.hasPath("max-result-size"))
            tresqlConf.getInt("max-result-size")
          else -1
        override val recursiveStackDepth: Int =
          if (tresqlConf.hasPath("max-result-size"))
            tresqlConf.getInt("max-result-size")
          else -1
      }

    new TresqlResConf {
      override val functionSignatureResource: String =
        Option(tresqlConfInstance.functionSignatureResource)
          .getOrElse(tresqlConfFromConfig.functionSignatureResource)
      override val macrosResource: String =
        Option(tresqlConfInstance.macrosResource)
          .getOrElse(tresqlConfFromConfig.macrosResource)
      override val macros: Any =
        Option(tresqlConfInstance.macros)
          .getOrElse(tresqlConfFromConfig.macros)
      override val dialect: Dialect =
        Option(tresqlConfInstance.dialect).orNull
      override val idExpr: String => String =
        Option(tresqlConfInstance.idExpr).orNull
      override val queryTimeout: Int =
        Option(tresqlConfInstance.queryTimeout)
          .getOrElse(tresqlConfFromConfig.queryTimeout)
      override val fetchSize: Int =
        Option(tresqlConfInstance.fetchSize)
          .getOrElse(tresqlConfFromConfig.fetchSize)
      override val maxResultSize: Int =
        Option(tresqlConfInstance.maxResultSize)
          .getOrElse(tresqlConfFromConfig.maxResultSize)
      override val recursiveStackDepth: Int =
        Option(tresqlConfInstance.recursiveStackDepth)
          .getOrElse(tresqlConfFromConfig.recursiveStackDepth)
      override val cache: Cache =
        Option(tresqlConfInstance.cache).orNull
      override val bindVarLogFilter: Logging#BindVarLogFilter =
        Option(tresqlConfInstance.bindVarLogFilter).orNull
      override val logger: Logging#TresqlLogger =
        Option(tresqlConfInstance.logger).orNull
    }
  }
}
