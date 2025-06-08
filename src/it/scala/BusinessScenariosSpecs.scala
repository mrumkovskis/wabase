package wabase.app

import org.apache.pekko.http.scaladsl.model.Uri
import org.mojoz.metadata.out.DdlGenerator
import org.wabase._

import java.io.File
import scala.language.reflectiveCalls
import scala.util.Random

object BusinessScenariosSpecs {
  def executeStatements(statements: String*): Unit = {
    val conn = ConnectionPools(TresqlResourcesConf.DefaultCpName).getConnection()
    try {
      val statement = conn.createStatement
      try statements foreach { statement.execute } finally statement.close()
    } finally conn.close()
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
    if (path.startsWith("/backdoor/create-table/")) {
      val tableName = path.substring("/backdoor/create-table/".length)
      val tableDef  = qe.tableMetadata.tableDef(tableName, null)
      val generator = DdlGenerator.hsqldb()
      val statement = generator.table(tableDef)
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
