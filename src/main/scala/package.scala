package org

import java.util.concurrent.TimeUnit.MILLISECONDS
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.jdk.CollectionConverters._

package object wabase extends Loggable {

  import scala.language.existentials
  import scala.language.implicitConversions
  import scala.language.postfixOps
  import scala.language.reflectiveCalls
  import com.typesafe.config._

  val config = ConfigFactory.load

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

  def durationConfig(path: String, defaultDuration: FiniteDuration) =
    Option(path).filter(config.hasPath).map(config.getDuration).map(toFiniteDuration).getOrElse(defaultDuration)

  implicit def toFiniteDuration(d: java.time.Duration): FiniteDuration = Duration.fromNanos(d.toNanos)

  /** Timeout is wrapped into case class so it can be used as implicit parameter */
  case class QueryTimeout(timeoutSeconds: Int)

  /** Default query timeout based on "jdbc.query-timeout" configuration setting */
  val DefaultQueryTimeout: Option[QueryTimeout] = // FIXME DefaultQueryTimeout must not be optional
    if (config.hasPath("jdbc.query-timeout"))     // FIXME do not test for config.hasPath - provide reference.conf instead
      Some(QueryTimeout(Duration(config.getString("jdbc.query-timeout")).toSeconds.toInt))
    else
      None

  val MaxResultSize: Option[Int] =
    if (config.hasPath("tresql.max-result-size")) Some(config.getInt("tresql.max-result-size"))
    else None

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

  case class PoolName(connectionPoolName: String)
  val DEFAULT_CP = PoolName(if (config.hasPath("jdbc.default")) config.getString("jdbc.default") else "main")
  if (!config.hasPath(s"jdbc.cp.${DEFAULT_CP.connectionPoolName}"))
    logger.warn(s"""Default connection pool configuration missing (key jdbc.cp.${DEFAULT_CP.connectionPoolName}), or jdbc.default not set to correct key (default value = "main"). \nThere will be errors if You rely on JDBC connections""")

  object ConnectionPools {
    private lazy val cps: Map[PoolName, HikariDataSource] = {
      val c = config.getConfig("jdbc.cp")
      c.root().asScala.keys.map(v => (PoolName(v), createConnectionPool(c.getConfig(v)))).toMap
    }

    def apply(pool: PoolName) = {
      cps.getOrElse(pool, {
        logger.warn(s"""Unable to find connection pool "${pool.connectionPoolName}". Using default pool "$DEFAULT_CP"""")
        cps(DEFAULT_CP)
      })
    }
  }
}
