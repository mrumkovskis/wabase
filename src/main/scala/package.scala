package org

import java.util.concurrent.TimeUnit.MILLISECONDS
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

import java.lang.reflect.InvocationTargetException
import javax.sql.DataSource
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.jdk.CollectionConverters._

package object wabase extends Loggable {

  import scala.language.existentials
  import scala.language.implicitConversions
  import scala.language.postfixOps
  import scala.language.reflectiveCalls
  import com.typesafe.config._

  lazy val config = ConfigFactory.load

  type jBoolean = java.lang.Boolean
  type jLong = java.lang.Long
  type jDate = java.util.Date
  type sDate = java.sql.Date
  type Timestamp = java.sql.Timestamp
  val TRUE = java.lang.Boolean.TRUE
  val FALSE = java.lang.Boolean.FALSE
  def currentTime = System.currentTimeMillis

  val CommonFunctions = ValidationEngine.CustomValidationFunctions

  type AppConfig = AppBase.AppConfig
  type AppMdConventions = AppMetadata.AppMdConventions
  type AppVersion = AppServiceBase.AppVersion
  type DbDeferredStorage = DeferredControl.DbDeferredStorage
  type PostgreSqlConstraintMessage = DbConstraintMessage.PostgreSqlConstraintMessage

  type ConstantQueryTimeout = AppServiceBase.ConstantQueryTimeout
  type DefaultAppMdConventions = AppMetadata.DefaultAppMdConventions
  type DefaultAppExceptionHandler[User] = AppServiceBase.AppExceptionHandler.DefaultAppExceptionHandler[User]
  type DefaultServerStatistics = ServerStatistics.DefaultServerStatistics
  type DefaultWsInitialEventsPublisher = ServerNotifications.DefaultInitialEventsPublisher

  type NoAudit[User] = Audit.NoAudit[User]
  type NoAuthorization[User] = Authorization.NoAuthorization[User]
  type NoCustomConstraintMessage = DbConstraintMessage.NoCustomConstraintMessage
  type NoServerStatistics = ServerStatistics.NoServerStatistics
  type NoWsInitialEvents = ServerNotifications.NoInitialEvents

  type CustomValidationFunctions = ValidationEngine.CustomValidationFunctions
  type LdapAuthentication = Authentication.LdapAuthentication
  type SimpleExceptionHandler = AppServiceBase.AppExceptionHandler.SimpleExceptionHandler
  type Statistics = ServerStatistics.Statistics

  @deprecated("use reference.conf and toFiniteDuration(config.getDuration(path))", "6.0")
  def durationConfig(path: String, defaultDuration: FiniteDuration) =
    Option(path).filter(config.hasPath).map(config.getDuration).map(toFiniteDuration).getOrElse(defaultDuration)

  implicit def toFiniteDuration(d: java.time.Duration): FiniteDuration = Duration.fromNanos(d.toNanos)

  /** Timeout is wrapped into case class so it can be used as implicit parameter */
  case class QueryTimeout(timeoutSeconds: Int)

  /** Default query timeout based on "jdbc.query-timeout" configuration setting */
  lazy val DefaultQueryTimeout: QueryTimeout =
    QueryTimeout(config.getDuration("jdbc.query-timeout").toSeconds.toInt)

  //db connection pool configuration
  def createConnectionPool(config: Config): HikariDataSource = {
    val props = new java.util.Properties(System.getProperties)
    for (e <- config.entrySet.asScala) {
      val key = e.getKey
      if (key.toLowerCase.contains("time") || key == "leakDetectionThreshold")
        props.setProperty(key, "" + config.getDuration(key, MILLISECONDS))
      else
        props.setProperty(key, config.getString(key))
    }
    val hikariConfig = new HikariConfig(props)
    new HikariDataSource(hikariConfig)
  }

  def getObjectOrNewInstance(className: String, description: String): AnyRef = {
    if (className endsWith "$")
      getObjectOrNewInstance(Class.forName(className), description)
    else try Class.forName(className).getDeclaredConstructor().newInstance().asInstanceOf[AnyRef] catch {
      case util.control.NonFatal(ex1) =>
        try Class.forName(className + "$").getField("MODULE$").get(null) catch {
          case util.control.NonFatal(ex2) =>
            logger.error(s"Failed to get $description instance, tried both empty constructor and object", ex2)
            throw new RuntimeException(s"Failed to get $description instance", ex1)
        }
      }
  }

  def getObjectOrNewInstance(clazz: Class[_], description: String): AnyRef = {
    try clazz.getField("MODULE$").get(null) catch {
      case util.control.NonFatal(ex1) =>
        try clazz.getDeclaredConstructor().newInstance().asInstanceOf[AnyRef] catch {
          case util.control.NonFatal(ex2) =>
            logger.error(s"Failed to get $description instance, tried both object and empty constructor", ex1)
            throw new RuntimeException(s"Failed to get $description instance", ex2)
        }
    }
  }

  def invocationParameter(availableParameters: Seq[(Class[_], Class[_] => Any)])(parameterClass: Class[_]): Any =
    availableParameters.collectFirst {
      case (c, f) if parameterClass.isAssignableFrom(c) || c.isAssignableFrom(parameterClass) => f(parameterClass)
    }.getOrElse(sys.error(s"Cannot find value for function parameter. Unsupported parameter type: $parameterClass"))

  def invokeFunction(className: String, function: String, getParameter: Class[_] => Any): Any = {
    val obj = getObjectOrNewInstance(className, s"function $function")
    val clazz = obj.getClass
    clazz.getMethods.filter(_.getName == function) match {
      case Array(method) =>
        try method.invoke(obj, (method.getParameterTypes map getParameter).asInstanceOf[Array[Object]]: _*) // cast is needed for scala 2.12.x
        catch {
          case e: InvocationTargetException if e.getCause != null => throw e.getCause
        }
      case Array() => sys.error(s"Method $function not found in class $className")
      case m => sys.error(s"Multiple methods '$function' found: (${m.toList}) in class $className")
    }
  }

  def invokeFunction(className: String, function: String, availableParameters: Seq[(Class[_], Class[_] => Any)]): Any =
    invokeFunction(className, function, invocationParameter(availableParameters)(_))

  case class PoolName(connectionPoolName: String)
  lazy val DEFAULT_CP = {
    val dcp = PoolName(TresqlResourcesConf.DefaultCpName)
    if (dcp.connectionPoolName == null)
      logger.debug("Default JDBC connection pool disabled")
    else if (!config.hasPath(s"jdbc.cp.${dcp.connectionPoolName}"))
      logger.warn(s"Default JDBC connection pool configuration missing (key jdbc.cp.${dcp.connectionPoolName}).")
    dcp
  }

  object ConnectionPools {
    private lazy val cps = {
      val c = config.getConfig("jdbc.cp")
      val s: Seq[(PoolName, DataSource)] =
        c.root().asScala.keys.map(v => (PoolName(v), createConnectionPool(c.getConfig(v)))).toSeq ++
          Seq(PoolName(null) -> DisabledDataSource)
      scala.collection.concurrent.TrieMap(s: _*)
    }
    def key(poolName: String): PoolName =
      if (poolName != null) PoolName(poolName) else DEFAULT_CP

    def apply(poolName: String): DataSource =
      apply(key(poolName))
    def apply(pool: PoolName): DataSource = {
      cps.getOrElse(pool, {
        require(pool == null || pool.connectionPoolName == null,
          s"""Unable to find connection pool "${pool.connectionPoolName}"""")
        cps(DEFAULT_CP)
      })
    }

    def apply(pool: PoolName, factoryFun: () => DataSource): DataSource = {
      cps.getOrElse(pool, {
        val ds = factoryFun()
        cps.put(pool, ds)
        ds
      })
    }
  }

  object DisabledDataSource extends DataSource {
    // Members declared in javax.sql.CommonDataSource
    override def getParentLogger(): java.util.logging.Logger = ???

    // Members declared in javax.sql.DataSource
    override def getConnection(username: String, password: String): java.sql.Connection = null
    override def getConnection(): java.sql.Connection = null
    override def getLogWriter(): java.io.PrintWriter = ???
    override def getLoginTimeout(): Int = ???
    override def setLogWriter(out: java.io.PrintWriter): Unit = ???
    override def setLoginTimeout(seconds: Int): Unit = ???

    // Members declared in java.sql.Wrapper
    override def isWrapperFor(iface: Class[_]): Boolean = false
    override def unwrap[T](iface: Class[T]): T = ???
  }
}
