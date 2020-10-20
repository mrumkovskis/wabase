package org.wabase

import com.typesafe.scalalogging.Logger
import javax.sql.DataSource
import org.slf4j.LoggerFactory
import org.tresql.{Dialect, Expr, LogTopic, QueryBuilder, SimpleCache, ThreadLocalResources, dialects}

import scala.language.postfixOps

trait DbAccessProvider {
  def dbAccess: DbAccess
}

trait DbAccess { this: Loggable =>
  implicit def tresqlResources: ThreadLocalResources

  protected def defaultQueryTimeout: QueryTimeout = DefaultQueryTimeout.getOrElse(QueryTimeout(5))

  private def setenv(pool: DataSource, timeout: QueryTimeout) {
    tresqlResources.initFromTemplate
    tresqlResources.conn = pool.getConnection
    tresqlResources.queryTimeout = timeout.timeoutSeconds
  }
  private def clearEnv(rollback: Boolean = false) = {
    val conn = tresqlResources.conn
    tresqlResources.conn = null
    tresqlResources.queryTimeout = 0
    if (rollback && conn != null) try conn.rollback catch { case e: Exception => e.printStackTrace }
    if (conn != null) try if (!conn.isClosed) conn.close catch { case e: Exception => e.printStackTrace }
  }

  private val currentPool = new ThreadLocal[PoolName]
  // db use
  def dbUse[A](a: => A)(implicit timeout: QueryTimeout = defaultQueryTimeout, pool: PoolName = DEFAULT_CP): A = {
    val oldConn = tresqlResources.conn
    val oldPool = currentPool.get()
    val oldTimeout = tresqlResources.queryTimeout
    val poolChanges = oldPool != pool
    if (poolChanges) {
      logger.debug(s"""Using connection pool "$pool"""")
      setenv(ConnectionPools(pool), timeout)
      currentPool.set(pool)
    }
    try {
      a
    } finally {
      if (poolChanges) {
        clearEnv(true)
        tresqlResources.conn = oldConn
        tresqlResources.queryTimeout = oldTimeout
        currentPool.set(oldPool)
      }
    }
  }

  val transaction: Transaction = new Transaction

  class Transaction {
    def apply[A](a: => A)(implicit timeout: QueryTimeout = defaultQueryTimeout, pool: PoolName = DEFAULT_CP): A =
      transactionInternal(forceNewConnection = false, a)
    def foreach(f: Unit => Unit)(implicit timeout: QueryTimeout = defaultQueryTimeout, pool: PoolName = DEFAULT_CP): Unit =
      transactionInternal(forceNewConnection = false, f(()))
  }

  val transactionNew: TransactionNew = new TransactionNew

  class TransactionNew {
    def apply[A](a: => A)(implicit timeout: QueryTimeout = defaultQueryTimeout, pool: PoolName = DEFAULT_CP): A =
      transactionInternal(forceNewConnection = true, a)
    def foreach(f: Unit => Unit)(implicit timeout: QueryTimeout = defaultQueryTimeout, pool: PoolName = DEFAULT_CP): Unit =
      transactionInternal(forceNewConnection = true, f(()))
  }

  protected def transactionInternal[A](forceNewConnection: Boolean, a: => A)(
    implicit timeout: QueryTimeout, pool: PoolName): A = {
    val oldConn = tresqlResources.conn
    val oldPool = currentPool.get()
    val oldTimeout = tresqlResources.queryTimeout
    val poolChanges = oldPool != pool
    if (forceNewConnection || poolChanges) {
      logger.debug(s"""Using connection pool "$pool"""")
      setenv(ConnectionPools(pool), timeout)
      currentPool.set(pool)
    }
    try {
      val res = a
      if (forceNewConnection || poolChanges) tresqlResources.conn.commit()
      res
    } catch {
      case ex: Exception =>
        if (forceNewConnection || poolChanges){
          try tresqlResources.conn.rollback catch {
            case e: Exception => e.printStackTrace
          }
        }
        throw ex
    } finally {
      if (forceNewConnection || poolChanges) {
        clearEnv()
        tresqlResources.conn = oldConn
        tresqlResources.queryTimeout = oldTimeout
        currentPool.set(oldPool)
      }
    }
  }
}

trait PostgresDbAccess extends DbAccess { this: QuereaseProvider with Loggable =>
  override lazy val tresqlResources: ThreadLocalResources = new PostgresSqlTresqlResources(qe)
}

trait TresqlResources extends ThreadLocalResources {
  override def resourcesTemplate = MaxResultSize.map { maxSize =>
    super.resourcesTemplate.copy(maxResultSize = maxSize, macros = Macros)
  }.getOrElse(super.resourcesTemplate.copy(macros = Macros))

  val infoLogger = Logger(LoggerFactory.getLogger("org.wabase.tresql"))
  val tresqlLogger = Logger(LoggerFactory.getLogger("org.wabase.tresql.tresql"))
  val sqlLogger = Logger(LoggerFactory.getLogger("org.wabase.tresql.db.sql"))
  val varsLogger = Logger(LoggerFactory.getLogger("org.wabase.tresql.db.vars"))
  val sqlWithParamsLogger = Logger(LoggerFactory.getLogger("org.wabase.tresql.sql_with_params"))

  override val logger = (m, params, topic) => topic match {
    case LogTopic.sql => sqlLogger.debug(m)
    case LogTopic.tresql => tresqlLogger.debug(m)
    case LogTopic.params => varsLogger.debug(m)
    case LogTopic.sql_with_params => sqlWithParamsLogger.debug(sqlWithParams(m, params))
    case LogTopic.info => infoLogger.debug(m)
    case _ => infoLogger.debug(m)
  }

  override val cache = new SimpleCache(4096)

  def sqlWithParams(sql: String, params: Map[String, Any]) = params.foldLeft(sql) {
    case (sql, (name, value)) => sql.replaceAllLiterally(s"?/*$name*/", value match {
      case _: Int | _: Long | _: Double | _: BigDecimal | _: BigInt | _: Boolean => value.toString
      case _: String | _: java.sql.Date | _: java.sql.Timestamp => s"'$value'"
      case null => "null"
      case _ => value.toString
    })
  }
}

class PostgresSqlTresqlResources(qe: AppQuerease) extends TresqlResources {
  protected lazy val typeDefs = qe.typeDefs
  protected lazy val YamlToPgTypeMap =
    typeDefs.map(t => t.name -> t.name).toMap ++
      typeDefs.filter(_.sqlWrite contains "sql")
        .map(t => t.name -> t.sqlWrite.get("sql")
          .map(_.map(_.targetNamePattern).min).getOrElse(t.name)).toMap ++
      typeDefs.filter(_.sqlWrite contains "postgresql")
        .map(t => t.name -> t.sqlWrite.get("postgresql")
          .map(_.map(_.targetNamePattern).min).getOrElse(t.name)).toMap

  /*
  protected val YamlToPgTypeMap = Map("string" -> "varchar", "long" -> "bigint",
    "date" -> "date", "dateTime" -> "timestamp", "int" -> "integer",
    "boolean" -> "boolean", "decimal" -> "decimal")
  */
  protected def yamlToPgTypeMap = YamlToPgTypeMap

  override def resourcesTemplate: ResourcesTemplate =
    super.resourcesTemplate
      .copy(metadata = qe.tresqlMetadata)
      .copy(dialect = AppPostgreSqlDialect orElse dialects.ANSISQLDialect orElse dialects.VariableNameDialect)
      .copy(idExpr = _ => "nextval(\"seq\")")

  object AppPostgreSqlDialect extends Dialect {
    private val dialect: Dialect = {
      case f: QueryBuilder#FunExpr if f.name == "unaccent" => s"f_unaccent(${f.params map (_.sql) mkString ", "})"
      case f: QueryBuilder#FunExpr if f.name == "cast" && f.params.size == 2 => s"cast(${f.params(0).sql} as ${
        f.params(1) match {
          case c: QueryBuilder#ConstExpr => String.valueOf(c.value)
          case x => x.sql
        }
      })"
      case f: QueryBuilder#FunExpr if f.name == "decode" && f.params.size > 2 =>
        f.params.tail.grouped(2).map { g =>
          if (g.size == 2) s"when ${g(0).sql} then ${g(1).sql}"
          else s"else ${g(0).sql}"
        }.mkString(s"case ${f.params(0).sql} ", " ", " end")
      case c: QueryBuilder#ColExpr if c.alias != null => Option(c.col).map(_.sql).getOrElse("null") + " as " + c.alias
      case i: QueryBuilder#InsertExpr =>
        //pg insert as select needs column cast if bind variables are from 'from' clause select
        val b = i.builder
        i.vals match {
          case ivals@b.SelectExpr(
          List(valstable@b.Table(b.BracesExpr(vals: b.SelectExpr), _, _, _, _)),
          _, _, _, _, _, _, _, _, _) =>
            val table = i.table.name.last
            //insertable column names
            val colNames = i.cols.collect { case b.ColExpr(b.IdentExpr(name), _, _, _) => name.last } toSet
            //second level query which needs column casts matching insertable column names
            val colsWithCasts =
              vals.cols.cols.map {
                case c@b.ColExpr(e, a, _, _) if colNames(a) =>
                  qe.tableMetadata.col(table, a).flatMap(n => yamlToPgTypeMap.get(n.type_.name))
                    .map(typ => c.copy(col = b.CastExpr(e, typ)))
                    .getOrElse(c)
                case x => x
              }
            //copy modified cols to second level query cols
            val colsExprWithCasts = vals.cols.copy(cols = colsWithCasts)
            new b.InsertExpr(i.table.asInstanceOf[b.IdentExpr], i.alias, i.cols, ivals
              .copy(tables = List(valstable.copy(
                table = b.BracesExpr(vals.copy(cols = colsExprWithCasts))))),
              i.returning.asInstanceOf[Option[b.ColsExpr]]
            ).defaultSQL
          case _ => i.defaultSQL
        }
    }

    override def isDefinedAt(e: Expr) = dialect.isDefinedAt(e)

    override def apply(e: Expr) = dialect(e)
  }
}

trait DbAccessDelegate extends DbAccess { this: Loggable =>

  def dbAccessDelegate: DbAccess

  implicit val tresqlResources: ThreadLocalResources = dbAccessDelegate.tresqlResources

  override def dbUse[A](a: => A)(implicit timeout: QueryTimeout = defaultQueryTimeout, pool: PoolName = DEFAULT_CP): A = {
    dbAccessDelegate.dbUse(a)
  }

  override val transaction: Transaction = new TransactionDelegate(dbAccessDelegate.transaction)

  class TransactionDelegate(tr: DbAccess#Transaction) extends Transaction {
    override def apply[A](a: => A)(implicit timeout: QueryTimeout = defaultQueryTimeout, pool: PoolName = DEFAULT_CP): A =
      tr(a)
    override def foreach(f: Unit => Unit)(implicit timeout: QueryTimeout = defaultQueryTimeout, pool: PoolName = DEFAULT_CP): Unit =
      tr.foreach(f)
  }

  override val transactionNew: TransactionNew = new TransactionNewDelegate(dbAccessDelegate.transactionNew)

  class TransactionNewDelegate(tr: DbAccess#TransactionNew) extends TransactionNew {
    override def apply[A](a: => A)(implicit timeout: QueryTimeout = defaultQueryTimeout, pool: PoolName = DEFAULT_CP): A =
      tr(a)
    override def foreach(f: Unit => Unit)(implicit timeout: QueryTimeout = defaultQueryTimeout, pool: PoolName = DEFAULT_CP): Unit =
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
  private val varRegex = new scala.util.matching.Regex(
    """(?<!:)(?::)([_a-zA-Z]\w*)(\?)?""", "name", "opt")
  override def sql(b: QueryBuilder, const: QueryBuilder#ConstExpr): b.SQLExpr = {
    val value = String.valueOf(const.value)
    val vars = varRegex.findAllMatchIn(value).map(m => b.VarExpr(
      m.group("name"), Nil, m.group("opt") == "?")).toList
    val sqlSnippet = varRegex.replaceAllIn(value, "?")
    if (vars.exists(v => v.opt && !(b.env contains v.name)))
      b.SQLExpr("null", Nil)
    else b.SQLExpr(sqlSnippet, vars)
  }

  override def shouldUnaccent(s: String) = true
  override def shouldIgnoreCase(s: String) = true
}

class TresqlComparisonMacros extends org.mojoz.querease.QuereaseMacros {

  val hasNonAscii = """[^\p{ASCII}]"""r
  val hasUpper = """\p{javaUpperCase}"""r

  def ~%(b: QueryBuilder, lop: Expr, rop: Expr) = matchMode(b, lop, rop, false, false, false, true)
  def %~(b: QueryBuilder, lop: Expr, rop: Expr) = matchMode(b, lop, rop, false, false, true, false)
  def %~%(b: QueryBuilder, lop: Expr, rop: Expr) = matchMode(b, lop, rop, false, false, true, true)
  def !~%(b: QueryBuilder, lop: Expr, rop: Expr) = matchMode(b, lop, rop, false, true, false, true)
  def !%~(b: QueryBuilder, lop: Expr, rop: Expr) = matchMode(b, lop, rop, false, true, true, false)
  def !%~%(b: QueryBuilder, lop: Expr, rop: Expr) = matchMode(b, lop, rop, false, true, true, true)

  def ~~%(b: QueryBuilder, lop: Expr, rop: Expr) = matchMode(b, lop, rop, true, false, false, true)
  def %~~(b: QueryBuilder, lop: Expr, rop: Expr) = matchMode(b, lop, rop, true, false, true, false)
  def %~~%(b: QueryBuilder, lop: Expr, rop: Expr) = matchMode(b, lop, rop, true, false, true, true)
  def !~~%(b: QueryBuilder, lop: Expr, rop: Expr) = matchMode(b, lop, rop, true, true, false, true)
  def !%~~(b: QueryBuilder, lop: Expr, rop: Expr) = matchMode(b, lop, rop, true, true, true, false)
  def !%~~%(b: QueryBuilder, lop: Expr, rop: Expr) = matchMode(b, lop, rop, true, true, true, true)

  def ~~~(b: QueryBuilder, lop: Expr, rop: Expr) = resolve(b, lop, rop, false, false, false)
  def ~~~%(b: QueryBuilder, lop: Expr, rop: Expr) = resolve(b, lop, rop, false, false, true)
  def %~~~(b: QueryBuilder, lop: Expr, rop: Expr) = resolve(b, lop, rop, false, true, false)
  def %~~~%(b: QueryBuilder, lop: Expr, rop: Expr) = resolve(b, lop, rop, false, true, true)
  def !~~~(b: QueryBuilder, lop: Expr, rop: Expr) = resolve(b, lop, rop, true, false, false)
  def !~~~%(b: QueryBuilder, lop: Expr, rop: Expr) = resolve(b, lop, rop, true, false, true)
  def !%~~~(b: QueryBuilder, lop: Expr, rop: Expr) = resolve(b, lop, rop, true, true, false)
  def !%~~~%(b: QueryBuilder, lop: Expr, rop: Expr) = resolve(b, lop, rop, true, true, true)

  private def resolve(b: QueryBuilder, lop: Expr, rop: Expr, not: Boolean, prefix: Boolean, suffix: Boolean) = {
    val (lopt, ropt) = maybeTransform(b, lop, rop)
    matchMode(b, lopt, ropt, false, not, prefix, suffix)
  }

  private def matchMode(b: QueryBuilder, lop: Expr, rop: Expr, ilike: Boolean, not: Boolean, prefix: Boolean, suffix: Boolean) = {
    val ropp = if (prefix) b.BinExpr("||", b.ConstExpr("%"), rop) else rop
    val rops = if (suffix) b.BinExpr("||", ropp, b.ConstExpr("%")) else ropp
    if (ilike) if (not) !~~(b, lop, rops) else ~~(b, lop, rops)
    else b.BinExpr(if (not) "!~" else "~", lop, rops)
  }

  private def maybeTransform(b: QueryBuilder, lop: Expr, rop: Expr) = {
    val (unnacentL, lowerL) = valueProps(b, lop)
    val (unnacentR, lowerR) = valueProps(b, rop)
    val (lopu, ropu) =
      if (unnacentL || unnacentR) (b.FunExpr("unaccent", List(lop)), b.FunExpr("unaccent", List(rop)))
      else (lop, rop)
    val (lopl, ropl) =
      if (lowerL || lowerR) (b.FunExpr("lower", List(lopu)), b.FunExpr("lower", List(ropu)))
      else (lopu, ropu)
    (lopl, ropl)
  }

  private def valueProps(b: QueryBuilder, e: Expr) = {
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

  protected def shouldUnaccent(s: String) = hasNonAscii.findFirstIn(s).isEmpty
  protected def shouldIgnoreCase(s: String) = hasUpper.findFirstIn(s).isEmpty
}
