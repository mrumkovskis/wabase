package org.wabase

import java.sql.{Connection, DriverManager}
import org.mojoz.metadata.in.YamlMd
import org.mojoz.metadata.out.SqlGenerator
import org.mojoz.querease.TresqlMetadata
import org.scalatest.{BeforeAndAfterAll, Suite}
import org.tresql.dialects.HSQLDialect
import org.tresql.{LogTopic, Logging, QueryBuilder, Resources}

import java.text.SimpleDateFormat
import java.util.Date

class TestQuerease(metadataFile: String) extends AppQuerease {
  override type DTO = org.wabase.Dto
  override type DWI = org.wabase.DtoWithId
  override lazy val yamlMetadata = YamlMd.fromResource(metadataFile)
  override lazy val viewNameToClassMap = Map[String, Class[_ <: Dto]]()
  def persistenceMetadata(viewName: String) = nameToPersistenceMetadata(viewName)
}

trait TestQuereaseInitializer extends BeforeAndAfterAll with Loggable { this: Suite =>

  protected var tresqlThreadLocalResources: TresqlResources = _
  protected var querease: TestQuerease = _

  protected def dbNamePrefix: String = getClass.getName

  override def beforeAll(): Unit = {
    super.beforeAll()
    class ThreadLocalDateFormat(val pattern: String) extends ThreadLocal[SimpleDateFormat] {
      override def initialValue = { val f = new SimpleDateFormat(pattern); f.setLenient(false); f }
      def apply(date: Date) = get.format(date)
      def format(date: Date) = get.format(date)
    }
    val Timestamp = new ThreadLocalDateFormat("yyyy.MM.dd HH:mm:ss.SSS")

    val TresqlLogger: Logging#TresqlLogger = { (msg, _, topic) =>
      val topicName = topic match {
        case LogTopic.info   => "info  "
        case LogTopic.params => "params"
        case LogTopic.sql    => "sql --"
        case LogTopic.tresql => "tresql"
        case LogTopic.ort => "ort"
        case LogTopic.sql_with_params => null
      }
      if (topicName != null) logger.debug(/*Timestamp(new Date()) + */s"  [$topicName]  $msg")
    }

    DbDrivers.loadDrivers

    def init_db(db: String): (String, Connection) = {
      val url = s"jdbc:hsqldb:mem:$dbNamePrefix${ if(db != null) "_" + db else ""}"
      val db_conn = DriverManager.getConnection(url)
      logger.debug(s"Creating database $url ...\n")
      SqlGenerator.hsqldb().schema(querease.tableMetadata.dbToTableDefs(db))
        .split(";\\s+").map(_ + ";")
        .++(customStatements)
        .foreach { sql =>
          logger.debug(sql)
          val st = db_conn.createStatement
          st.execute(sql)
          st.close
        }
      val st = db_conn.createStatement
      st.execute("create sequence seq")
      st.close
      logger.debug("Database created successfully.")
      (db, db_conn)
    }
    def create_resources(md: TresqlMetadata, extra: Map[String, Resources]) = {
      new TresqlResources {
        override def logger = TresqlLogger
        override val resourcesTemplate =
          super.resourcesTemplate.copy(
            metadata = md,
            dialect = HSQLDialect orElse {
              case c: QueryBuilder#CastExpr => c.typ match {
                case "bigint" | "long" | "int" => s"convert(${c.exp.sql}, BIGINT)"
                case _ => c.exp.sql
              }
            },
            idExpr = _ => "nextval('seq')",
            extraResources = extra
          )
      }
    }
    this.tresqlThreadLocalResources =
      querease.tableMetadata.dbToTableDefs.keys.map(init_db).toList.partition(_._1 == null) match {
        case (List((null, conn)), extra) =>
          val extra_res = extra.map { case (db, _) =>
            db -> create_resources(querease.tresqlMetadata.extraDbToMetadata(db), Map())
          }.toMap
          val res = create_resources(querease.tresqlMetadata, extra_res)
          res.conn = conn
          res.extraResources = extra.map { case (db, extraConn) =>
            ( db
            , res.extraResources(db).withConn(extraConn)
            )
          }.toMap
          res
        case x => sys.error(s"No main database found - null name: $x")
      }
  }

  protected def customStatements: Seq[String] = {
    List(
      """create function array_length(sql_array bigint array) returns int
       language java deterministic no sql
       external name 'CLASSPATH:test.HsqldbCustomFunctions.array_length'""",
      """create function array_length(sql_array char varying(1024) array) returns int
       language java deterministic no sql
       external name 'CLASSPATH:test.HsqldbCustomFunctions.array_length'""",
      """create function checked_resolve(
         resolvable char varying(1024), resolved bigint array, error_message char varying(1024)
       ) returns bigint
         if array_length(resolved) > 1 or resolvable is not null and (array_length(resolved) = 0 or resolved[1] is null) then
           signal sqlstate '45000' set message_text = error_message;
         elseif array_length(resolved) = 1 then
           return resolved[1];
         else
           return null;
         end if""",
      """create function checked_resolve(
         resolvable char varying(1024), resolved char varying(1024) array, error_message char varying(1024)
       ) returns  char varying(1024)
         if array_length(resolved) > 1 or resolvable is not null and (array_length(resolved) = 0 or resolved[1] is null) then
           signal sqlstate '45000' set message_text = error_message;
         elseif array_length(resolved) = 1 then
           return resolved[1];
         else
           return null;
         end if""",
      "set database collation \"Latvian\""
    )
  }

  def removeKeys(result: Any, keys: Set[String]): Any = {
    def rk(v: Any): Any = v match {
      case m: Map[String@unchecked, _] => rm(m)
      case i: Iterable[_] => i map rk
      case x => x
    }
    def rm(m: Map[String, Any]): Map[String, Any] = m.flatMap {
      case (k, v) if !keys.contains(k) => List(k -> rk(v))
      case _ => Nil
    }
    rk(result)
  }

  def removeIds(result: Any): Any = removeKeys(result, Set("id"))
}
