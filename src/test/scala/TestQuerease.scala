package org.wabase

import java.sql.{Connection, DriverManager}
import org.mojoz.metadata.in.YamlMd
import org.mojoz.metadata.out.DdlGenerator
import org.mojoz.querease.TresqlMetadata
import org.scalatest.{BeforeAndAfterAll, Suite}
import org.tresql.dialects.HSQLDialect
import org.tresql.{LogTopic, Logging, QueryBuilder, Resources, ResourcesTemplate, ThreadLocalResources}

import java.text.SimpleDateFormat
import java.util.Date

class TestQuerease(val metadataFile: String, mdFilter: YamlMd => Boolean = _ => true) extends AppQuerease {
  override lazy val yamlMetadata = YamlMd.fromResource(metadataFile).filter(mdFilter)
  override lazy val viewNameToClassMap = Map[String, Class[_ <: Dto]]()
  def persistenceMetadata(viewName: String) = nameToPersistenceMetadata(viewName)
}

class TestTresqlConf extends TresqlResourcesConf {
  override val dialect = HSQLDialect orElse {
    case c: QueryBuilder#CastExpr => c.typ match {
      case "bigint" | "long" | "int" => s"convert(${c.exp.sql}, BIGINT)"
      case "decimal" => s"convert(${c.exp.sql}, NUMERIC(10,2))"
      case "date" => s"convert(${c.exp.sql}, DATE)"
      case _ => c.exp.sql
    }
  }
  override val idExpr = _ => "nextval('seq')"
}

trait TestQuereaseInitializer extends BeforeAndAfterAll with Loggable { this: Suite =>

  protected var tresqlThreadLocalResources: ThreadLocalResources = _
  protected var querease: TestQuerease = _
  protected implicit var qio: AppQuereaseIo[Dto] = _

  protected def dbNamePrefix: String = getClass.getName

  override def beforeAll(): Unit = {
    super.beforeAll()
    class ThreadLocalDateFormat(val pattern: String) extends ThreadLocal[SimpleDateFormat] {
      override def initialValue = { val f = new SimpleDateFormat(pattern); f.setLenient(false); f }
      def apply(date: Date) = get.format(date)
      def format(date: Date) = get.format(date)
    }
    val Timestamp = new ThreadLocalDateFormat("yyyy.MM.dd HH:mm:ss.SSS")
    DbDrivers.loadDrivers
    System.setProperty(
      "hsqldb.method_class_names",
      "test.HsqldbCustomFunctions.*"// allow access to our custom java functions
    )
    Thread.sleep(50) // allow property to be set for sure (fix unstable hsqldb tests)

    this.tresqlThreadLocalResources = {
      def init_db(db: String): (String, Connection) = {
        val url = s"jdbc:hsqldb:mem:$dbNamePrefix${if (db != null) "_" + db else ""}"
        val db_conn = DriverManager.getConnection(url)
        logger.debug(s"Creating database $url ...\n")
        DdlGenerator.hsqldb().schema(querease.tableMetadata.dbToTableDefs(db))
          .split(";\\s+").map(_ + ";")
          .++(customStatements)
          .foreach { sql =>
            logger.debug(sql)
            val st = db_conn.createStatement
            st.execute(sql)
            st.close
          }
        val st = db_conn.createStatement
        st.execute("create sequence seq start with 1")
        st.close
        logger.debug("Database created successfully.")
        (db, db_conn)
      }
      val dbs = querease.tableMetadata.dbToTableDefs.keys.toSet
      val templ = {
        val confs = TresqlResourcesConf.confs.filter(d => dbs(d._1))
        TresqlResourcesConf.tresqlResourcesTemplate(confs, querease.tresqlMetadata)
      }
      if (templ == null) null
      else {
        val res =
          new ThreadLocalResources {
            override def initResourcesTemplate: ResourcesTemplate = templ
          }
        val dbcons = dbs map init_db
        def findConn(n: String) = dbcons.find(_._1 == n).map(_._2).orNull
        res.conn = findConn(null)
        res.extraResources = res.extraResources.transform((db, r) => r.withConn(findConn(db)))
        res
      }
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
