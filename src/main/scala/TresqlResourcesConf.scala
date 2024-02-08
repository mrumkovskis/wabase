package org.wabase

import com.typesafe.config.{Config, ConfigFactory, ConfigObject, ConfigValueType}
import org.mojoz.querease.TresqlMetadata
import org.tresql.{Cache, Dialect, Logging, Metadata, Resources, ResourcesTemplate, SimpleCache, dialects}

import scala.jdk.CollectionConverters._

trait TresqlResourcesConf {
  def macrosClass: Class[_] = null
  def dialect: Dialect = null
  def idExpr: String => String = null
  def queryTimeout: Int = -1
  def maxResultSize: Int = -1
  def fetchSize: Int = -1
  def recursiveStackDepth: Int = -1
  def cacheSize: Int = -1
  def cache: Cache = null
  def bindVarLogFilter: Logging#BindVarLogFilter = null
  def db: String = null
  /** This method allows to distinguish between null db value and no db value. In the last case db name
   * is taken from configuration parameter tresql.<db name>. */
  protected def isDbSet: Boolean = false
}

object TresqlResourcesConf extends Loggable {

  val config = ConfigFactory.load("tresql-resources.conf")

  lazy val confs: Map[String, TresqlResourcesConf] = {
    if (config.hasPath("tresql")) {
      val rConf = config.getConfig("tresql")
      rConf.root().asScala
        .collect { case e@(_, v) if v.valueType() == ConfigValueType.OBJECT => e }
        .map { case (dbName, confValue) =>
          val n = if (dbName == DefaultCpName) null else dbName
          n -> tresqlResourcesConf(n, confValue.asInstanceOf[ConfigObject].toConfig.withFallback(rConf))
        }.toMap match {
          case m if m.isEmpty => Map((null, new TresqlResourcesConf {}))
          case m => m
        }
    }
    else Map((null, new TresqlResourcesConf {}))
  }

  /**
   * Merges configuration from config-class instance and if value not defined
   * (null for strings or -1 for numbers), configuration parameters.
   * Following configuration parameters under tresql.<db name> can be used (primitive values):
   *  - config-class          - custom TresqlResourcesConf class name (must have no arg constructor)
   *  - macro-class           - macros implementation class name (must have no arg constructor)
   *  - query-timeout
   *  - max-result-size
   *  - fetch-size
   *  - resursive-stack-depth
   *  - cache-size
   *  - db                    - db instance name where tables are defined.
   * */
  def tresqlResourcesConf(dbName: String, tresqlConf: Config): TresqlResourcesConf = {
    val tresqlConfInstance =
      if (tresqlConf.hasPath("config-class"))
        Class.forName(tresqlConf.getString("config-class")).getDeclaredConstructor().newInstance().asInstanceOf[TresqlResourcesConf]
      else new TresqlResourcesConf {}

    val tresqlConfFromConfig =
      new TresqlResourcesConf {
        override val macrosClass: Class[_] =
          if (tresqlConf.hasPath("macros-class"))
            Class.forName(tresqlConf.getString("macros-class"))
          else null
        override val dialect: Dialect =
          if (tresqlConf.hasPath("vendor"))
            vendor_dialect(tresqlConf.getString("vendor"))
          else null
        override val idExpr: String => String =
          if (tresqlConf.hasPath("vendor"))
            vendor_id_expr(tresqlConf.getString("vendor"))
          else null
        override val queryTimeout: Int =
          if (tresqlConf.hasPath("query-timeout"))
            tresqlConf.getDuration("query-timeout").getSeconds.toInt
          else -1
        override val maxResultSize: Int =
          if (tresqlConf.hasPath("max-result-size"))
            tresqlConf.getInt("max-result-size")
          else -1
        override val fetchSize: Int =
          if (tresqlConf.hasPath("fetch-size"))
            tresqlConf.getInt("fetch-size")
          else -1
        override val recursiveStackDepth: Int =
          if (tresqlConf.hasPath("recursive-stack-depth"))
            tresqlConf.getInt("recursive-stack-depth")
          else -1
        override val cacheSize: Int =
          if (tresqlConf.hasPath("cache-size"))
            tresqlConf.getInt("cache-size")
          else -1
        override val db: String =
          if (tresqlConf.hasPath("db"))
            tresqlConf.getString("db")
          else null
        override protected val isDbSet: Boolean =
          tresqlConf.hasPathOrNull("db")
      }

    new TresqlResourcesConf {
      override val macrosClass: Class[_] =
        Option(tresqlConfInstance.macrosClass)
          .getOrElse(tresqlConfFromConfig.macrosClass)
      override val dialect: Dialect =
        Option(tresqlConfInstance.dialect)
          .getOrElse(tresqlConfFromConfig.dialect)
      override val idExpr: String => String =
        Option(tresqlConfInstance.idExpr)
          .getOrElse(tresqlConfFromConfig.idExpr)
      override val queryTimeout: Int =
        Option(tresqlConfInstance.queryTimeout).filter(_ != -1)
          .getOrElse(tresqlConfFromConfig.queryTimeout)
      override val fetchSize: Int =
        Option(tresqlConfInstance.fetchSize).filter(_ != -1)
          .getOrElse(tresqlConfFromConfig.fetchSize)
      override val maxResultSize: Int =
        Option(tresqlConfInstance.maxResultSize).filter(_ != -1)
          .getOrElse(tresqlConfFromConfig.maxResultSize)
      override val cacheSize: Int =
        Option(tresqlConfInstance.cacheSize).filter(_ != -1)
          .getOrElse(tresqlConfFromConfig.cacheSize)
      override val recursiveStackDepth: Int =
        Option(tresqlConfInstance.recursiveStackDepth).filter(_ != -1)
          .getOrElse(tresqlConfFromConfig.recursiveStackDepth)
      override val cache: Cache =
        Option(tresqlConfInstance.cache).orNull
      override val bindVarLogFilter: Logging#BindVarLogFilter =
        Option(tresqlConfInstance.bindVarLogFilter).orNull
      override val db: String =
        (if (tresqlConfInstance.isDbSet) Some(tresqlConfInstance.db) else Option(tresqlConfInstance.db))
        .orElse(if (tresqlConfFromConfig.isDbSet) Some(tresqlConfFromConfig.db) else Option(tresqlConfFromConfig.db))
        .getOrElse(dbName)
      override protected val isDbSet: Boolean = tresqlConfInstance.isDbSet || tresqlConfFromConfig.isDbSet
    }
  }

  def vendor_dialect(vendor: String): Dialect = vendor match {
    case "postgresql" =>
      TresqlResources.PostgresSqlDialect orElse dialects.PostgresqlDialect orElse dialects.VariableNameDialect
    case "oracle" => dialects.OracleDialect orElse dialects.VariableNameDialect
    case "hsqldb" => dialects.HSQLDialect orElse dialects.VariableNameDialect
    case _ => dialects.ANSISQLDialect orElse dialects.VariableNameDialect
  }

  def vendor_id_expr(vendor: String): String => String = vendor match {
    case "postgresql" => _ => "nextval('seq')"
    case "oracle" => seq => s"dual{`$seq.nextval`}"
    case "hsqldb" => _ => "nextval('seq')"
    case _ => seq => s"nextval('$seq')"
  }

  def tresqlResourcesTemplate(
    resConfs: Map[String, TresqlResourcesConf],
    tresqlMetadata: TresqlMetadata
  ): ResourcesTemplate = {
    val cpToVendor = {
      val DbVendorRegex = """jdbc:(\w+):.*""".r
      def dbVendor(jdbcUrl: String): String = {
        val DbVendorRegex(vendor) = jdbcUrl
        vendor
      }
      val c = org.wabase.config.getConfig("jdbc.cp")
      c.root().asScala.keys.map { cp =>
        val n = if (cp == DefaultCpName) null else cp
        n -> dbVendor(c.getString(s"$cp.jdbcUrl"))
      }.toMap
    }
    def resources(
      db: String,
      conf: TresqlResourcesConf,
      metadata: Metadata,
      extraResources: Map[String, Resources],
    ): ResourcesTemplate = {
      val wabaseConf = org.wabase.config
      val macros =
        if (conf.macrosClass != null) {
          try conf.macrosClass.getField("MODULE$").get(null) catch {
            case util.control.NonFatal(ex1) =>
              try conf.macrosClass.getDeclaredConstructor().newInstance() catch {
                case util.control.NonFatal(ex2) =>
                  logger.error("Failed to get macros instance, tried both object and empty constructor", ex1)
                  throw new RuntimeException("Failed to get macros instance", ex2)
              }
          }
        }
        else Macros
      val dialect: Dialect = {
        val dbVendor = cpToVendor.getOrElse(db, null)
        if (conf.dialect != null) conf.dialect orElse vendor_dialect(dbVendor)
        else vendor_dialect(dbVendor)
      }
      val idExpr: String => String =
        if (conf.idExpr != null) conf.idExpr
        else vendor_id_expr(cpToVendor.getOrElse(db, null))
      val queryTimeout =
        if (conf.queryTimeout != -1) conf.queryTimeout
        else wabaseConf.getDuration("jdbc.query-timeout").getSeconds.toInt
      val maxResultSize =
        if (conf.maxResultSize != -1) conf.maxResultSize
        else wabaseConf.getInt("tresql.max-result-size")
      val fetchSize =
        if (conf.fetchSize != -1) conf.fetchSize
        else 0
      val recursiveStackDepth =
        if (conf.recursiveStackDepth != -1) conf.recursiveStackDepth
        else 50
      val cacheSize =
        if (conf.cacheSize != -1) conf.cacheSize
        else wabaseConf.getInt("tresql.cache-size")
      val cache =
        if (conf.cache != null) conf.cache
        else new SimpleCache(cacheSize)
      val bindVarLogFilter =
        if(conf.bindVarLogFilter != null) conf.bindVarLogFilter
        else TresqlResources.bindVarLogFilter

      ResourcesTemplate(
        conn = null,
        metadata = metadata,
        dialect = dialect,
        idExpr = idExpr,
        queryTimeout = queryTimeout,
        fetchSize: Int,
        maxResultSize: Int,
        recursiveStackDepth = recursiveStackDepth,
        params = Map(),
        extraResources = extraResources,
        logger = TresqlResources.logger,
        cache = cache,
        bindVarLogFilter = bindVarLogFilter,
        macros = macros
      )
    }

    resConfs.partition(_._1 == null) match {
      case (main, extra) if main.nonEmpty =>
        val extraRes = extra.flatMap {
          case (db, extraConf) =>
            if (tresqlMetadata.extraDbToMetadata.contains(db))
              List(db -> resources(db, extraConf, tresqlMetadata.extraDbToMetadata(db), Map()))
            else Nil
        }
        resources(null, main(null), tresqlMetadata, extraRes)
      case (_, extra) if extra.isEmpty => null //return null if no resources are configured
      case _ => sys.error(s"no main database found in (${resConfs.keys.mkString(",")})")
    }
  }
}
