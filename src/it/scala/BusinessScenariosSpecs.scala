package wabase.app

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.{HttpRequest, HttpResponse, Uri}
import org.apache.pekko.http.scaladsl.unmarshalling.Unmarshal
import org.apache.pekko.util.ByteString
import org.mojoz.metadata.out.DdlGenerator
import org.wabase._
import org.wabase.WabaseUnmarshallers.mapUnmarshaller

import java.io.File
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.language.reflectiveCalls
import scala.util.Try

object BusinessScenariosSpecs {
  def executeStatements(statements: String*): Unit = {
    val conn = ConnectionPools(TresqlResourcesConf.DefaultCpName).getConnection()
    try {
      val statement = conn.createStatement
      try statements foreach { statement.execute } finally statement.close()
    } finally conn.close()
  }

  def requestInfo(req: HttpRequest)(implicit as: ActorSystem, ec: ExecutionContext): Future[Map[String, Any]] = {
    Unmarshal(req.entity).to[Map[String, Any]].map { map =>
      val contentType     = req.entity.contentType
      val contentTypeName = s"${contentType.mediaType.mainType}/${contentType.mediaType.subType}"
      def transformFileContentToString(map: Map[String, Any]) =
        MapUtils.transform("file/content", _.asInstanceOf[ByteString].utf8String, map)
      val contentTypeParameters =
        Try(contentType.toString.drop(contentTypeName.length + 1).trim).toOption.filter(_ != "").orNull
      Map(
        "content-type" -> contentTypeName,
        "content-type-parameters" -> contentTypeParameters,
        "data" -> transformFileContentToString(map),
      ).filter(_._2 != null).toMap
    }
  }

  def sleep(millis: String, response: HttpResponse)(implicit ec: ExecutionContext): Future[HttpResponse] = Future {
    Thread.sleep(millis.toLong)
    response
  }
}

class BusinessScenariosSpecs extends BusinessScenariosBaseSpecs("http_tests") {
  import BusinessScenariosSpecs._
  lazy val server = new RunningServer
  override def resourcePath = "resources/"
  override def initHttpClient = server
  override def beforeAll() = {
  }
  override def afterAll() = {
    server.unbind() // unbind for cross-scala tests
  }

  override def scenariosAutoLogin  = false
  override def scenariosAutoLogout = false

  override def checkTestCase(
    scenario: File, testCase: File, context: Map[String, Any], map: Map[String, Any], retriesLeft: Int
  ): Map[String, Any] = {
    val path   = map.s("path")
    val method = map.sd("method", "GET")
    if (path.startsWith("/backdoor/create-sequence/")) {
      val seqName   = path.substring("/backdoor/create-sequence/".length)
      val statement = s"create sequence $seqName;"
      executeStatements(statement)
      context
    } else if (path.startsWith("/backdoor/create-table/")) {
      val tableName = path.substring("/backdoor/create-table/".length)
      val tableDef  = qe.tableMetadata.tableDef(tableName, null)
      val generator = DdlGenerator.hsqldb()
      val statement = generator.table(tableDef)
      executeStatements(statement)
      context
    } else if (path.startsWith("/backdoor/drop-sequence/")) {
      val seqName   = path.substring("/backdoor/drop-sequence/".length)
      val statement = s"drop sequence $seqName;"
      executeStatements(statement)
      context
    } else if (path.startsWith("/backdoor/drop-table/")) {
      val tableName = path.substring("/backdoor/drop-table/".length)
      executeStatements(s"drop table $tableName;")
      context
    } else {
      super.checkTestCase(scenario, testCase, context, map, retriesLeft)
    }
  }
}
