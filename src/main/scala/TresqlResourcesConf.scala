package org.wabase

import com.typesafe.config.{Config, ConfigFactory, ConfigObject, ConfigResolveOptions, ConfigValueType}
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

  val config = ConfigFactory.parseResources("tresql-resources.conf").resolve(ConfigResolveOptions.noSystem())

  val tunedConfig = ConfigFactory.defaultOverrides()
      .withFallback(ConfigFactory.defaultApplication())
      .withFallback(ConfigFactory.parseResources("tresql-resources.conf"))
      .withFallback(ConfigFactory.defaultReferenceUnresolved())
      .resolve()

  val wabaseConf = ConfigFactory.load

  lazy val DefaultCpName: String =
    Option("tresql.default")
      .filter(config.hasPathOrNull)
      .map(n => if (config.getIsNull(n)) null else config.getString(n))
      .getOrElse("main")

  lazy val confs: Map[String, TresqlResourcesConf] = {
      val plainCfgR = if (     config.hasPath("tresql"))      config.getConfig("tresql") else ConfigFactory.empty
      val tunedCfgR = if (tunedConfig.hasPath("tresql")) tunedConfig.getConfig("tresql") else ConfigFactory.empty
      val cpConfs =
       wabaseConf.getConfig("jdbc.cp").root().asScala.keys.map { cpName =>
          val n = if (cpName == DefaultCpName) null else cpName
          cpName ->
            // force db name here because plugin does not read application.conf when initializing aliasToDb
            ConfigFactory.parseString(s"db = ${Option(n).map("\"" + _ + "\"").orNull}")
        }.toMap

      val resConfs = plainCfgR.root().asScala
        .collect { case e@(_, v) if v.valueType() == ConfigValueType.OBJECT => e }
        .map { case (cpName, confValue) =>
          cpName -> confValue.asInstanceOf[ConfigObject].toConfig
        }.toMap

      (cpConfs ++ resConfs)
        .map { case (cpName, cpOrResConf) =>
          val tunedConfForCp =
            if (tunedCfgR.hasPath(cpName)) tunedCfgR.getConfig(cpName).withFallback(tunedCfgR) else tunedCfgR
          val n = if (cpName == DefaultCpName) null else cpName
          n -> tresqlResourcesConf(n, tunedConfForCp, cpOrResConf.withFallback(plainCfgR))
        }.toMap match {
          case m if m.isEmpty => Map((null, new TresqlResourcesConf {}))
          case m => m
        }
  }

  /**
   * Merges configuration from config-class instance and if value not defined
   * (null for strings or -1 for numbers), configuration parameters.
   * Settings for specific db are prioritized over [root] settings for any db.
   * 1. Tunable settings (sizes, timeouts) from props and confs (application.*, tresql-resources.conf, reference.conf)
   * 3. Settings in configuration class
   * 4. Settings in tresql-resources.conf
   * 5. Settings in props and application confs (application.*, reference.conf)
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
  def tresqlResourcesConf(
      cpName: String, tunedConfForCp: Config, plainConfForCp: Config): TresqlResourcesConf = {
    val tresqlConfInstance =
      if (plainConfForCp.hasPath("config-class"))
        getObjectOrNewInstance(plainConfForCp.getString("config-class"), "tresql resources config").asInstanceOf[TresqlResourcesConf]
      else new TresqlResourcesConf {}

    def tresqlConfFromConfig(cConf: Config, tunableOnly: Boolean) = {
      def getStringOpt(parameterName: String): Option[String] =
        Option(parameterName).filter(cConf.hasPath).filterNot(_ => tunableOnly).map(cConf.getString)
      def getStringSetOpt(parameterName: String): Option[Set[String]] =
        Option(parameterName).filter(cConf.hasPath).filterNot(_ => tunableOnly).map(cConf.getStringList).map(_.asScala.toSet)
      def getSeconds(parameterName: String): Int =
        Option(parameterName).filter(cConf.hasPath).map(cConf.getDuration).map(_.getSeconds.toInt).getOrElse(-1)
      def getInt(parameterName: String): Int =
        Option(parameterName).filter(cConf.hasPath).map(cConf.getInt).getOrElse(-1)
      new TresqlResourcesConf {
        override val cacheSize:             Int = getInt("cache-size")
        override val db:                 String = getStringOpt("db").orNull
        override val dialect:           Dialect = getStringOpt("vendor").map(vendor_dialect).orNull
        override val fetchSize:             Int = getInt("fetch-size")
        override val idExpr:   String => String = getStringOpt("vendor").map(vendor_id_expr).orNull
        override val macrosClass:      Class[_] = getStringOpt("macros-class").map(Class.forName).orNull
        override val maxResultSize:         Int = getInt("max-result-size")
        override val queryTimeout:          Int = getSeconds("query-timeout")
        override val recursiveStackDepth:   Int = getInt("recursive-stack-depth")
        override val bindVarLogFilter:      Logging#BindVarLogFilter =
          getStringSetOpt("confidential-value-variable-names").map { hide => {
            case (fullName, _) if hide.contains(fullName) || hide.exists(h => fullName startsWith s"$h.") => "***"
          }: Logging#BindVarLogFilter}.orNull
        override protected val isDbSet: Boolean = cConf.hasPathOrNull("db") && !tunableOnly
      }
    }

    val tresqlConfs = Seq(
      tresqlConfFromConfig(tunedConfForCp, tunableOnly = true),
      tresqlConfInstance,
      tresqlConfFromConfig(plainConfForCp, tunableOnly = false),
      tresqlConfFromConfig(tunedConfForCp, tunableOnly = false),
    )

    def getInt(getIntValue: TresqlResourcesConf => Int) = tresqlConfs.map(getIntValue).find(_ != -1).getOrElse(-1)
    def getValue[T >: Null](getValue: TresqlResourcesConf => T): T = tresqlConfs.map(getValue).find(_ != null).orNull

    new TresqlResourcesConf {
      override val bindVarLogFilter: Logging#BindVarLogFilter = getValue(_.bindVarLogFilter)
      override val cache:               Cache = getValue(_.cache)
      override val cacheSize:             Int = getInt(_.cacheSize)
      override val db:                 String = tresqlConfs.filter(_.isDbSet).headOption.map(_.db).getOrElse(cpName)
      override val dialect:           Dialect = getValue(_.dialect)
      override val fetchSize:             Int = getInt(_.fetchSize)
      override val idExpr:   String => String = getValue(_.idExpr)
      override val macrosClass:      Class[_] = getValue(_.macrosClass)
      override val maxResultSize:         Int = getInt(_.maxResultSize)
      override val queryTimeout:          Int = getInt(_.queryTimeout)
      override val recursiveStackDepth:   Int = getInt(_.recursiveStackDepth)
      override protected val isDbSet: Boolean = tresqlConfs.exists(_.isDbSet)
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
      val c = wabaseConf.getConfig("jdbc.cp")
      c.root().asScala.keys.map { cp =>
        val n = if (cp == DefaultCpName) null else cp
        n -> dbVendor(c.getString(s"$cp.jdbcUrl"))
      }.toMap
    }
    def resources(
      cpName: String,
      conf: TresqlResourcesConf,
      metadata: Metadata,
      extraResources: Map[String, Resources],
    ): ResourcesTemplate = {
      val macros =
        if (conf.macrosClass != null)
             getObjectOrNewInstance(conf.macrosClass, "macros")
        else Macros
      val dialect: Dialect = {
        val dbVendor = cpToVendor.getOrElse(cpName, null)
        if (conf.dialect != null) conf.dialect orElse vendor_dialect(dbVendor)
        else vendor_dialect(dbVendor)
      }
      val idExpr: String => String =
        if (conf.idExpr != null) conf.idExpr
        else vendor_id_expr(cpToVendor.getOrElse(cpName, null))
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
      case (main, extra) =>
        val extraRes = extra.flatMap {
          case (cpName, extraConf) =>
            if (tresqlMetadata.extraDbToMetadata.contains(cpName))
              List(cpName -> resources(cpName, extraConf, tresqlMetadata.extraDbToMetadata(cpName), Map()))
            else Nil
        }
        if  (main.nonEmpty)
             resources(null, main(null),                 tresqlMetadata,          extraRes)
        else resources(null, new TresqlResourcesConf {}, new TresqlMetadata(Nil), extraRes)
    }
  }
}
