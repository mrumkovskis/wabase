package org.wabase

import java.sql.{Connection, DriverManager}
import org.mojoz.metadata.in.YamlMd
import org.mojoz.metadata.out.SqlGenerator
import org.mojoz.querease.TresqlMetadata
import org.scalatest.{BeforeAndAfterAll, Suite}
import org.scalatest.matchers.should.Matchers
import org.tresql.dialects.HSQLDialect
import org.tresql.{LogTopic, Logging, QueryBuilder, Resources}

import java.text.SimpleDateFormat
import java.util.Date

class QuereaseBase(metadataFile: String) extends AppQuerease {
  override type DTO = org.wabase.Dto
  override type DWI = org.wabase.DtoWithId
  override lazy val yamlMetadata = YamlMd.fromResource(metadataFile)
  override lazy val viewNameToClassMap = Map[String, Class[_ <: Dto]]()
}

trait QuereaseBaseSpecs extends Matchers with BeforeAndAfterAll with Loggable { this: Suite =>

  protected implicit var tresqlResources: TresqlResources = _
  protected var querease: AppQuerease = _

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
    def create_resources(db_conn: Connection, md: TresqlMetadata, extra: Map[String, Resources]) = {
      new TresqlResources {
        override val resourcesTemplate =
          super.resourcesTemplate.copy(
            conn = db_conn,
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
        override val logger = TresqlLogger
      }
    }
    this.tresqlResources = querease.tableMetadata.dbToTableDefs.keys.map(init_db).toList.partition(_._1 == null) match {
      case (List((null, conn)), extra) =>
        val extra_res = extra.map { case (db, conn) =>
          db -> create_resources(conn, querease.tresqlMetadata.extraDbToMetadata(db), Map())
        }.toMap
        create_resources(conn, querease.tresqlMetadata, extra_res)
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

  def removeKeys(map: Map[String, Any], keys: Set[String]): Map[String, Any] = {
    def rk(v: Any): Any = v match {
      case m: Map[String@unchecked, _] => rm(m)
      case i: Iterable[_] => i map rk
      case x => x
    }
    def rm(m: Map[String, Any]): Map[String, Any] = m.flatMap {
      case (k, v) if !keys.contains(k) => List(k -> rk(v))
      case _ => Nil
    }
    rm(map)
  }

  def removeIds(map: Map[String, Any]): Map[String, Any] = removeKeys(map, Set("id"))
}
