package org.wabase.ds

import com.typesafe.config.Config
import org.wabase.{Loggable, OpParser, TresqlResourcesConf, config, invokeFunction}

import javax.sql.DataSource
import scala.util.control.NonFatal
import scala.jdk.CollectionConverters._

case class PoolName(connectionPoolName: String)
/** Timeout is wrapped into case class so it can be used as implicit parameter */
case class QueryTimeout(timeoutSeconds: Int)

object ConnectionPools extends Loggable {
  lazy val DEFAULT_CP = {
    val dcp = PoolName(TresqlResourcesConf.DefaultCpName)
    if (dcp.connectionPoolName == null)
      logger.debug("Default JDBC connection pool disabled")
    else if (!config.hasPath(s"jdbc.cp.${dcp.connectionPoolName}"))
      logger.warn(s"Default JDBC connection pool configuration missing (key jdbc.cp.${dcp.connectionPoolName}).")
    dcp
  }

  /** Default query timeout based on "jdbc.query-timeout" configuration setting */
  lazy val DefaultQueryTimeout: QueryTimeout =
    QueryTimeout(config.getDuration("jdbc.query-timeout").toSeconds.toInt)

  private val factory_class_function = config.getString("jdbc.data-source-factory")
  private lazy val cps = {
    val c = config.getConfig("jdbc.cp")
    val s: Seq[(PoolName, DataSource)] =
      c.root().asScala.keys.map(v => (PoolName(v), createDataSource(c.getConfig(v)))).toSeq ++
        Seq(PoolName(null) -> DisabledDataSource)
    scala.collection.concurrent.TrieMap(s: _*)
  }

  private def createDataSourceFromFactory(config: Config): DataSource = {
    import scala.concurrent.ExecutionContext.Implicits.global
    invokeFunction(factory_class_function, Seq((classOf[Config], () => config))).asInstanceOf[DataSource]
  }

  private def createDataSource(conf: Config) = try createDataSourceFromFactory(conf) catch {
    case NonFatal(ex) =>
      logger.error(s"Error initializing Db connection pool for $conf", ex)
      null
  }

  def key(poolName: String): PoolName =
    if (poolName != null) PoolName(poolName) else DEFAULT_CP

  def apply(poolName: String): DataSource =
    apply(key(poolName))

  def apply(pool: PoolName): DataSource = {
    val ds = cps.getOrElse(pool, {
      require(pool == null || pool.connectionPoolName == null,
        s"""Unable to find connection pool "${pool.connectionPoolName}"""")
      cps(DEFAULT_CP)
    })
    if (ds != null) ds
    else apply(pool, () => createDataSourceFromFactory(config.getConfig(s"jdbc.cp.${pool.connectionPoolName}")))
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
