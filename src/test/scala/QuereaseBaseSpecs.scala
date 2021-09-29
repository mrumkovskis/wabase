package org.wabase

import java.sql.{Connection, DriverManager}
import org.mojoz.metadata.in.YamlMd
import org.mojoz.metadata.out.SqlGenerator
import org.scalatest.{BeforeAndAfterAll, Suite}
import org.scalatest.matchers.should.Matchers
import org.tresql.dialects.HSQLDialect
import org.tresql.{LogTopic, Logging, QueryBuilder}

import java.text.SimpleDateFormat
import java.util.Date

class QuereaseBase(metadataFile: String) extends AppQuerease {
  override type DTO = org.wabase.Dto
  override type DWI = org.wabase.DtoWithId
  override lazy val yamlMetadata = YamlMd.fromResource(metadataFile)
  override lazy val viewNameToClassMap = Map[String, Class[_ <: Dto]]()
}

trait QuereaseBaseSpecs extends Matchers with BeforeAndAfterAll with Loggable { this: Suite =>

  protected var conn: Connection = _
  protected implicit var tresqlResources: TresqlResources = _
  protected var querease: AppQuerease = _

  protected def customStatements: Seq[String] = Nil

  override def beforeAll(): Unit = {
    super.beforeAll()
    Class.forName("org.hsqldb.jdbc.JDBCDriver")
    this.conn = DriverManager.getConnection(s"jdbc:hsqldb:mem:${getClass.getName}_db")
    logger.debug(s"Creating database for ${getClass.getName} tests ...\n")
    SqlGenerator.hsqldb().schema(querease.tableMetadata.tableDefs)
      .split(";\\s+").map(_ + ";")
      .++(customStatements)
      .foreach { sql =>
        logger.debug(sql)
        val st = conn.createStatement
        st.execute(sql)
        st.close
      }
    val st = conn.createStatement
    st.execute("create sequence seq")
    st.close
    logger.debug("Database created successfully.")

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

    this.tresqlResources  = new TresqlResources {
      override val resourcesTemplate =
        super.resourcesTemplate.copy(
          conn = QuereaseBaseSpecs.this.conn,
          metadata = querease.tresqlMetadata,
          dialect = HSQLDialect orElse {
            case c: QueryBuilder#CastExpr => c.typ match {
              case "bigint" | "long" | "int" => s"convert(${c.exp.sql}, BIGINT)"
              case _ => c.exp.sql
            }
          },
          idExpr = _ => "nextval('seq')"
        )
      override val logger = TresqlLogger
    }
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
