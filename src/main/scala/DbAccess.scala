package org.wabase

import com.typesafe.scalalogging.Logger
import org.mojoz.querease.{QuereaseMacros, TresqlMetadata}

import java.sql.Connection
import javax.sql.DataSource
import org.slf4j.LoggerFactory
import org.tresql.{Cache, Dialect, Expr, LogTopic, Logging, QueryBuilder, Resources, ResourcesTemplate, SimpleCache, ThreadLocalResources}
import org.wabase.AppMetadata.DbAccessKey

import scala.language.postfixOps
import scala.util.control.NonFatal

trait DbAccessProvider {
  def dbAccess: DbAccess
}

trait DbAccess { this: Loggable =>

  val DefaultCp: PoolName = WabaseAppConfig.DefaultCp

  protected lazy val resourcesTemplate: ResourcesTemplate =
    TresqlResourcesConf.tresqlResourcesTemplate(TresqlResourcesConf.confs, tresqlMetadata)
  protected lazy val threadLocalResources = new ThreadLocalResources {
    override def initResourcesTemplate: ResourcesTemplate = DbAccess.this.resourcesTemplate
  }
  protected def tresqlMetadata: TresqlMetadata
  implicit def tresqlResources: ThreadLocalResources = threadLocalResources

  protected def defaultQueryTimeout: QueryTimeout = DefaultQueryTimeout

  private def setenv(pool: DataSource, timeout: QueryTimeout, extraDb: Seq[DbAccessKey]): Unit = {
    tresqlResources.initFromTemplate
    tresqlResources.conn = pool.getConnection
    tresqlResources.queryTimeout = timeout.timeoutSeconds
    if (extraDb.nonEmpty) tresqlResources.extraResources =
      extraDb.map { case DbAccessKey(db) =>
        db -> (
         if (db == DefaultCp.connectionPoolName)
          tresqlResources
         else
          tresqlResources
            .extraResources(db)
            .withConn(dataSource(ConnectionPools.key(db)).getConnection)
        )
      }.toMap
  }
  private def clearEnv(rollback: Boolean = false) = {
    def close(c: Connection) = {
      if (c != null) if (rollback) rollbackAndCloseConnection(c) else commitAndCloseConnection(c)
    }
    val conn = tresqlResources.conn
    tresqlResources.conn = null
    close(conn)
    val extraConns = tresqlResources.extraResources.collect { case (_, r) if r.conn != null => r.conn }
    if (extraConns.nonEmpty) tresqlResources.extraResources =
      tresqlResources.extraResources.transform((_, r) => r.withConn(null))
    extraConns foreach close
    tresqlResources.queryTimeout = 0
  }

  def commitAndCloseConnection: Connection => Unit = DbAccess.commitAndCloseConnection
  def rollbackAndCloseConnection: Connection => Unit = DbAccess.rollbackAndCloseConnection
  def closeConns: (Connection => Unit) => Resources => Unit = DbAccess.closeConns
  def initResources: Resources => (PoolName, Seq[DbAccessKey]) => Resources = DbAccess.initResources
  def closeResources: (Resources, Boolean, Option[Throwable]) => Unit = DbAccess.closeResources

  def extraDb(keys: Seq[DbAccessKey]): Seq[DbAccessKey] = keys.filter(_.db != null)

  def withDbAccessLogger(rt: ResourcesTemplate, loggerPrefix: String): ResourcesTemplate =
    DbAccess.withLogger(rt, loggerPrefix)

  private val currentPool = new ThreadLocal[PoolName]
  // TODO do not call nested dbUse with extraDb parameter set to avoid connection leaks
  def dbUse[A](a: => A)(implicit timeout: QueryTimeout = defaultQueryTimeout,
                        pool: PoolName = DEFAULT_CP,
                        extraDb: Seq[DbAccessKey] = Nil): A = {
    require(pool != null, "Connection pool must not be null")
    val oldConn = tresqlResources.conn
    val oldPool = currentPool.get()
    val oldTimeout = tresqlResources.queryTimeout
    val oldExtraResources = tresqlResources.extraResources
    val poolChanges = oldPool != pool
    if (poolChanges) {
      logger.debug(s"""Using connection pool "${pool.connectionPoolName}"""")
      setenv(dataSource(pool), timeout, extraDb)
      currentPool.set(pool)
    }
    try {
      a
    } finally {
      if (poolChanges) {
        clearEnv(true)
        tresqlResources.conn = oldConn
        tresqlResources.queryTimeout = oldTimeout
        tresqlResources.extraResources = oldExtraResources
        currentPool.set(oldPool)
      }
    }
  }

  def withConn[A](
    template: Resources = tresqlResources.resourcesTemplate,
    poolName: PoolName = DEFAULT_CP,
    extraDb:  Seq[DbAccessKey] = Nil,
  )(f: Resources => A): A =
    DbAccess.withConn(template, poolName, extraDb)(f)

  def withRollbackConn[A](
    template: Resources = tresqlResources.resourcesTemplate,
    poolName: PoolName = DEFAULT_CP,
    extraDb:  Seq[DbAccessKey] = Nil,
  )(f: Resources => A): A =
    DbAccess.withRollbackConn(template, poolName, extraDb)(f)

  def transaction[A](
    template: Resources = tresqlResources.resourcesTemplate,
    poolName: PoolName = DEFAULT_CP,
    extraDb:  Seq[DbAccessKey] = Nil,
  )(f: Resources => A): A =
    DbAccess.transaction(template, poolName, extraDb)(f)

  val transaction: Transaction = new Transaction

  // TODO do not call nested transaction with extraDb parameter set to avoid connection leaks
  class Transaction {
    def apply[A](a: => A)(implicit timeout: QueryTimeout = defaultQueryTimeout,
                          pool: PoolName = DEFAULT_CP,
                          extraDb: Seq[DbAccessKey] = Nil): A =
      transactionInternal(forceNewConnection = false, a)
    def foreach(f: Unit => Unit)(implicit timeout: QueryTimeout = defaultQueryTimeout,
                                 pool: PoolName = DEFAULT_CP,
                                 extraDb: Seq[DbAccessKey] = Nil): Unit =
      transactionInternal(forceNewConnection = false, f(()))
  }

  val transactionNew: TransactionNew = new TransactionNew

  // TODO do not call nested transactionNew with extraDb parameter set to avoid connection leaks
  class TransactionNew {
    def apply[A](a: => A)(implicit timeout: QueryTimeout = defaultQueryTimeout,
                          pool: PoolName = DEFAULT_CP,
                          extraDb: Seq[DbAccessKey] = Nil): A =
      transactionInternal(forceNewConnection = true, a)
    def foreach(f: Unit => Unit)(implicit timeout: QueryTimeout = defaultQueryTimeout,
                                 pool: PoolName = DEFAULT_CP,
                                 extraDb: Seq[DbAccessKey] = Nil): Unit =
      transactionInternal(forceNewConnection = true, f(()))
  }

  protected def transactionInternal[A](forceNewConnection: Boolean, a: => A)(implicit timeout: QueryTimeout,
                                                                             pool: PoolName,
                                                                             extraDb: Seq[DbAccessKey]): A = {
    require(pool != null, "Connection pool must not be null")
    val oldConn = tresqlResources.conn
    val oldPool = currentPool.get()
    val oldTimeout = tresqlResources.queryTimeout
    val oldExtraResources = tresqlResources.extraResources
    val poolChanges = oldPool != pool
    if (forceNewConnection || poolChanges) {
      logger.debug(s"""Using connection pool "${pool.connectionPoolName}"""")
      setenv(dataSource(pool), timeout, extraDb)
      currentPool.set(pool)
    }
    try {
      val res = a
      if (forceNewConnection || poolChanges) {
        if (tresqlResources.conn != null) tresqlResources.conn.commit()
        tresqlResources.extraResources.foreach { case (_, r) => if (r.conn != null) r.conn.commit() }
      }
      res
    } catch {
      case ex: Exception =>
        def rollbackConn(c: Connection) = try if (c != null) c.rollback() catch {
          case NonFatal(e) => logger.warn(s"Error rolling back connection $c", e)
        }
        if (forceNewConnection || poolChanges) {
          rollbackConn(tresqlResources.conn)
          tresqlResources.extraResources.foreach { case (_, r) => rollbackConn(r.conn) }
        }
        throw ex
    } finally {
      if (forceNewConnection || poolChanges) {
        clearEnv()
        tresqlResources.conn = oldConn
        tresqlResources.queryTimeout = oldTimeout
        tresqlResources.extraResources = oldExtraResources
        currentPool.set(oldPool)
      }
    }
  }

  def dataSource(pool: PoolName): DataSource = {
    ConnectionPools(pool)
  }
}

object TresqlResources {
  def sqlWithParams(sql: String, params: Seq[(String, Any)]) = params.foldLeft(sql) {
    case (sql, (name, value)) => sql.replace(s"?/*$name*/", value match {
      case _: Int | _: Long | _: Double | _: BigDecimal | _: BigInt | _: Boolean => value.toString
      case _: String | _: java.sql.Date | _: java.sql.Timestamp => s"'$value'"
      case null => "null"
      case _ => value.toString
    })
  }
  val infoLogger = Logger(LoggerFactory.getLogger("org.wabase.tresql"))
  val tresqlLogger = Logger(LoggerFactory.getLogger("org.wabase.tresql.tql"))
  val ortLogger = Logger(LoggerFactory.getLogger("org.wabase.tresql.ort"))
  val sqlLogger = Logger(LoggerFactory.getLogger("org.wabase.tresql.sql"))
  val varsLogger = Logger(LoggerFactory.getLogger("org.wabase.tresql.params"))
  val sqlWithParamsLogger = Logger(LoggerFactory.getLogger("org.wabase.tresql.sql_wp"))

  val logger: Logging#TresqlLogger = (m, params, topic) => topic match {
    case LogTopic.sql => sqlLogger.debug(m)
    case LogTopic.tresql => tresqlLogger.debug(m)
    case LogTopic.params => varsLogger.debug(m)
    case LogTopic.sql_with_params => sqlWithParamsLogger.debug(sqlWithParams(m, params))
    case LogTopic.ort => ortLogger.debug(m)
    case LogTopic.info => infoLogger.debug(m)
    case _ => infoLogger.debug(m)
  }

  def withLogger(loggerPrefix: String): Logging#TresqlLogger = {
    val infoLogger = Logger(LoggerFactory.getLogger(s"$loggerPrefix.tresql"))
    val tresqlLogger = Logger(LoggerFactory.getLogger(s"$loggerPrefix.tresql.tql"))
    val ortLogger = Logger(LoggerFactory.getLogger(s"$loggerPrefix.tresql.ort"))
    val sqlLogger = Logger(LoggerFactory.getLogger(s"$loggerPrefix.tresql.sql"))
    val varsLogger = Logger(LoggerFactory.getLogger(s"$loggerPrefix.tresql.vars"))
    val sqlWithParamsLogger = Logger(LoggerFactory.getLogger(s"$loggerPrefix.tresql.sql_wp"))

    (msg, params, topic) => {
      topic match {
        case LogTopic.sql =>
          sqlLogger.debug(msg)
          if(!sqlLogger.underlying.isDebugEnabled) this.sqlLogger.debug(msg)
        case LogTopic.tresql =>
          tresqlLogger.debug(msg)
          if(!tresqlLogger.underlying.isDebugEnabled) this.tresqlLogger.debug(msg)
        case LogTopic.params =>
          varsLogger.debug(msg)
          if(!varsLogger.underlying.isDebugEnabled) this.varsLogger.debug(msg)
        case LogTopic.sql_with_params =>
          sqlWithParamsLogger.debug(sqlWithParams(msg, params))
          if(!sqlWithParamsLogger.underlying.isDebugEnabled) this.sqlWithParamsLogger.debug(sqlWithParams(msg, params))
        case LogTopic.ort =>
          ortLogger.debug(msg)
          if(!ortLogger.underlying.isDebugEnabled) this.ortLogger.debug(msg)
        case LogTopic.info =>
          infoLogger.debug(msg)
          if(!infoLogger.underlying.isDebugEnabled) this.infoLogger.debug(msg)
      }
    }
  }

  val cache: Cache = new SimpleCache(config.getInt("tresql.cache-size"))

  val PostgresSqlDialect: Dialect = {
    case f: QueryBuilder#FunExpr if f.name == "unaccent" => s"f_unaccent(${f.params map (_.sql) mkString ", "})"
  }

  val bindVarLogFilter: Logging#BindVarLogFilter = {
    case (fullName, _) if fullName == "password" || fullName.startsWith("password.") => "***"
  }
}

object DbAccess extends Loggable {
  def commitAndCloseConnection(dbConn: Connection): Unit = {
    try if (dbConn != null && !dbConn.isClosed) dbConn.commit() catch {
      case NonFatal(ex) => logger.warn(s"Failed to commit db connection $dbConn", ex)
    }
    closeConnection(dbConn)
  }
  def rollbackAndCloseConnection(dbConn: Connection): Unit = {
    try if (dbConn != null && !dbConn.isClosed) dbConn.rollback catch {
      case NonFatal(ex) => logger.warn(s"Failed to rollback db transaction $dbConn", ex)
    }
    closeConnection(dbConn)
  }
  def closeConnection(dbConn: Connection): Unit = {
    try if (dbConn != null && !dbConn.isClosed) dbConn.close catch {
      case NonFatal(ex) => logger.warn(s"Failed to close db connection $dbConn", ex)
    }
  }
  def closeConns(connCloser: Connection => Unit)(resources: Resources) = {
    (resources.conn :: resources
      .extraResources.collect { case (_, r) if r.conn != null => r.conn }.toList) foreach connCloser
  }
  def initResources(initialResources: Resources)(poolName: PoolName, extraDb: Seq[DbAccessKey]): Resources = {
    val dsFactory = () => ConnectionPools(poolName)
    val dsExtraFactories = extraDb.map { case DbAccessKey(db) =>
      (db, () => ConnectionPools(db))
    }.toMap
    initConns(initialResources, poolName)(dsFactory, dsExtraFactories)
  }
  def initConns(initialResources: Resources, poolName: PoolName)(
    dsFactory: () => DataSource,
    dsExtraFactories: Map[String, () => DataSource]
  ): Resources = {
    val dbConn = dsFactory().getConnection
    var extraConns = List[Connection]()
    try {
      val initRes = initialResources.withConn(dbConn)
      if (dsExtraFactories.isEmpty) initRes
      else dsExtraFactories.foldLeft(initRes) { case (res, (db, fac)) =>
        if (res.extraResources.contains(db)) {
          val extraConn =
            if (db == poolName.connectionPoolName)
              dbConn
            else
              fac().getConnection
          extraConns ::= extraConn
          res.withUpdatedExtra(db)(_.withConn(extraConns.head))
        } else res
      }
    } catch {
      case NonFatal(ex) =>
        (dbConn :: extraConns) foreach rollbackAndCloseConnection
        throw ex
    }
  }
  def closeResources(res: Resources, doRollback: Boolean, err: Option[Throwable]): Unit = {
    if (err.isEmpty && !doRollback) closeConns(commitAndCloseConnection)(res)
    else closeConns(rollbackAndCloseConnection)(res)
  }
  def withConn[A](
    template: Resources,
    poolName: PoolName = DEFAULT_CP,
    extraDb:  Seq[DbAccessKey] = Nil,
  )(f: Resources => A): A = {
    val res = initResources(template)(poolName, extraDb)
    try f(res) finally closeConns(closeConnection)(res)
  }
  def withRollbackConn[A](
    template: Resources,
    poolName: PoolName = DEFAULT_CP,
    extraDb:  Seq[DbAccessKey] = Nil,
  )(f: Resources => A): A = {
    val res = initResources(template)(poolName, extraDb)
    try f(res) finally closeConns(rollbackAndCloseConnection)(res)
  }
  def transaction[A](
    template: Resources,
    poolName: PoolName = DEFAULT_CP,
    extraDb:  Seq[DbAccessKey] = Nil,
  )(f: Resources => A): A = {
    val res = initResources(template)(poolName, extraDb)
    try {
      val result = f(res)
      closeConns(commitAndCloseConnection)(res)
      result
    } catch {
      case util.control.NonFatal(ex) =>
        closeConns(rollbackAndCloseConnection)(res)
        throw ex
    }
  }

  def withLogger(rt: ResourcesTemplate, loggerPrefix: String): ResourcesTemplate = {
    val logger = TresqlResources.withLogger(loggerPrefix)
    rt.copy(
      logger = logger,
      extraResources = rt.extraResources.transform((_, rt) => rt.withLogger(logger))
    )
  }
}

trait DbAccessDelegate extends DbAccess { this: Loggable =>

  def dbAccessDelegate: DbAccess

  override implicit val tresqlResources: ThreadLocalResources = dbAccessDelegate.tresqlResources

  override def commitAndCloseConnection: Connection => Unit = dbAccessDelegate.commitAndCloseConnection
  override def rollbackAndCloseConnection: Connection => Unit = dbAccessDelegate.rollbackAndCloseConnection
  override def closeConns: (Connection => Unit) => Resources => Unit = dbAccessDelegate.closeConns
  override def initResources: Resources => (PoolName, Seq[DbAccessKey]) => Resources = dbAccessDelegate.initResources
  override def closeResources: (Resources, Boolean, Option[Throwable]) => Unit = dbAccessDelegate.closeResources
  override def extraDb(keys: Seq[DbAccessKey]): Seq[DbAccessKey] = dbAccessDelegate.extraDb(keys)

  override def withConn[A](template: Resources, poolName: PoolName, extraDb: Seq[DbAccessKey])(f: Resources => A): A =
    dbAccessDelegate.withConn(template, poolName, extraDb)(f)
  override def withRollbackConn[A](template: Resources, poolName: PoolName, extraDb: Seq[DbAccessKey])(f: Resources => A): A =
    dbAccessDelegate.withRollbackConn(template, poolName, extraDb)(f)
  override def transaction[A](template: Resources, poolName: PoolName, extraDb: Seq[DbAccessKey])(f: Resources => A): A =
    dbAccessDelegate.transaction(template, poolName, extraDb)(f)

  override def dbUse[A](a: => A)(implicit timeout: QueryTimeout = defaultQueryTimeout,
                                 pool: PoolName = DEFAULT_CP,
                                 extraDb: Seq[DbAccessKey] = Nil): A = {
    dbAccessDelegate.dbUse(a)
  }

  override val transaction: Transaction = new TransactionDelegate(dbAccessDelegate.transaction)

  class TransactionDelegate(tr: DbAccess#Transaction) extends Transaction {
    override def apply[A](a: => A)(implicit timeout: QueryTimeout = defaultQueryTimeout,
                                   pool: PoolName = DEFAULT_CP,
                                   extraDb: Seq[DbAccessKey] = Nil): A =
      tr(a)
    override def foreach(f: Unit => Unit)(implicit timeout: QueryTimeout = defaultQueryTimeout,
                                          pool: PoolName = DEFAULT_CP,
                                          extraDb: Seq[DbAccessKey] = Nil): Unit =
      tr.foreach(f)
  }

  override val transactionNew: TransactionNew = new TransactionNewDelegate(dbAccessDelegate.transactionNew)

  class TransactionNewDelegate(tr: DbAccess#TransactionNew) extends TransactionNew {
    override def apply[A](a: => A)(implicit timeout: QueryTimeout = defaultQueryTimeout,
                                   pool: PoolName = DEFAULT_CP,
                                   extraDb: Seq[DbAccessKey] = Nil): A =
      tr(a)
    override def foreach(f: Unit => Unit)(implicit timeout: QueryTimeout = defaultQueryTimeout,
                                          pool: PoolName = DEFAULT_CP,
                                          extraDb: Seq[DbAccessKey] = Nil): Unit =
      tr.foreach(f)
  }
}

object Macros extends Macros

class Macros extends TresqlComparisonMacros {
  /**
    * Dumb regexp to find bind variables (tresql syntax) in sql string.
    * Expects [not double] colon, identifier, optional question mark.
    * Double colon is excluded to ignore postgresql typecasts.
    */
  protected val varRegex = new scala.util.matching.Regex(
    """(?U)(?<!:)(?::)([_\p{L}]\w*)(\?)?""", "name", "opt")
  override def sql(b: QueryBuilder, const: QueryBuilder#ConstExpr): b.SQLExpr = {
    val value = String.valueOf(const.value)
    val vars = varRegex.findAllMatchIn(value).map(m => b.VarExpr(
      m.group("name"), Nil, m.group("opt") == "?", false)).toList
    val sqlSnippet = varRegex.replaceAllIn(value, "?")
    if (vars.exists(v => v.opt && !(b.env contains v.name)))
      b.SQLExpr("null", Nil)
    else b.SQLExpr(sqlSnippet, vars)
  }

  override def shouldUnaccent(s: String) = true
  override def shouldIgnoreCase(s: String) = true
}

class TresqlComparisonMacros extends QuereaseMacros {

  val hasNonAscii = """[^\p{ASCII}]"""r
  val hasUpper = """\p{javaUpperCase}"""r

  override def bin_op_function(b: QueryBuilder, op: QueryBuilder#ConstExpr, lop: Expr, rop: Expr): Expr = {
    if (lop == null || rop == null) null else {
      def resolve(lop: Expr, rop: Expr, not: Boolean, prefix: Boolean, suffix: Boolean) = {
        val (lopt, ropt) = maybeTransform(lop, rop)
        matchMode(lopt, ropt, false, not, prefix, suffix)
      }

      def matchMode(lop: Expr, rop: Expr, ilike: Boolean, not: Boolean, prefix: Boolean, suffix: Boolean) = {
        val ropp = if (prefix) b.BinExpr("||", b.ConstExpr("%"), rop) else rop
        val rops = if (suffix) b.BinExpr("||", ropp, b.ConstExpr("%")) else ropp
        if (ilike)
          if (not) super.bin_op_function(b, b.ConstExpr("!~~"), lop, rops)
          else super.bin_op_function(b, b.ConstExpr("~~"), lop, rops)
        else b.BinExpr(if (not) "!~" else "~", lop, rops)
      }

      def maybeTransform(lop: Expr, rop: Expr) = {
        val (unnacentL, lowerL) = valueProps(lop)
        val (unnacentR, lowerR) = valueProps(rop)
        val (lopu, ropu) =
          if (unnacentL || unnacentR) (b.FunExpr("unaccent", List(lop)), b.FunExpr("unaccent", List(rop)))
          else (lop, rop)
        val (lopl, ropl) =
          if (lowerL || lowerR) (b.FunExpr("lower", List(lopu)), b.FunExpr("lower", List(ropu)))
          else (lopu, ropu)
        (lopl, ropl)
      }

      def valueProps(e: Expr) = {
        var hasVar = false
        var una = true
        var low = true
        b.transform(e, {
          case v: b.VarExpr if b.env contains v.name => v() match {
            case x =>
              hasVar = true
              if (una) una = shouldUnaccent(String.valueOf(x))
              if (low) low = shouldIgnoreCase(String.valueOf(x))
              v
          }
        })
        (una && hasVar, low && hasVar)
      }

      String.valueOf(op.value) match {
        case "~%"       => matchMode(lop, rop, false, false, false, true)
        case "%~"       => matchMode(lop, rop, false, false, true, false)
        case "%~%"      => matchMode(lop, rop, false, false, true, true)
        case "!~%"      => matchMode(lop, rop, false, true, false, true)
        case "!%~"      => matchMode(lop, rop, false, true, true, false)
        case "!%~%"     => matchMode(lop, rop, false, true, true, true)
        case "~~%"      => matchMode(lop, rop, true, false, false, true)
        case "%~~"      => matchMode(lop, rop, true, false, true, false)
        case "%~~%"     => matchMode(lop, rop, true, false, true, true)
        case "!~~%"     => matchMode(lop, rop, true, true, false, true)
        case "!%~~"     => matchMode(lop, rop, true, true, true, false)
        case "!%~~%"    => matchMode(lop, rop, true, true, true, true)
        case "~~~"      => resolve(lop, rop, false, false, false)
        case "~~~%"     => resolve(lop, rop, false, false, true)
        case "%~~~"     => resolve(lop, rop, false, true, false)
        case "%~~~%"    => resolve(lop, rop, false, true, true)
        case "!~~~"     => resolve(lop, rop, true, false, false)
        case "!~~~%"    => resolve(lop, rop, true, false, true)
        case "!%~~~"    => resolve(lop, rop, true, true, false)
        case "!%~~~%"   => resolve(lop, rop, true, true, true)
        case _          => super.bin_op_function(b, op, lop, rop)
      }
    }
  }

  protected def shouldUnaccent(s: String) = hasNonAscii.findFirstIn(s).isEmpty
  protected def shouldIgnoreCase(s: String) = hasUpper.findFirstIn(s).isEmpty
}
