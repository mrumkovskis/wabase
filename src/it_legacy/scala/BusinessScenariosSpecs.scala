package wabase.app

import org.mojoz.metadata.out.DdlGenerator
import org.wabase._
import org.wabase.ds.ConnectionPools

object BusinessScenariosSpecs extends Loggable {
  def executeStatements(statements: String*): Unit = {
    val conn = ConnectionPools(TresqlResourcesConf.DefaultCpName).getConnection()
    try {
      val statement = conn.createStatement
      try statements foreach { st =>
        logger.debug(st)
        try statement.execute(st) catch {
          case util.control.NonFatal(ex) =>
            logger.error(s"Failed to execute statement: $st")
            throw ex
        }
      } finally statement.close()
    } finally conn.close()
  }
}

class BusinessScenariosSpecs extends BusinessScenariosBaseSpecs("http_tests") {
  import BusinessScenariosSpecs.executeStatements
  lazy val server = new RunningServer
  override def resourcePath = "resources/"
  override def initHttpClient = server
  override def beforeAll() = {
    server
  }
  override def afterAll() = {
    server.unbind() // unbind for cross-scala tests
  }

  override def scenariosAutoLogin  = false
  override def scenariosAutoLogout = false

  override def backdoorAction(requestInfo: RequestInfo, context: Map[String, Any], map: Map[String, Any]): Any = {
    import requestInfo._
    if (path.startsWith("/backdoor/create-sequences/")) {
      val seqNames   = path.substring("/backdoor/create-sequences/".length).split(",").toSeq
      val statements = seqNames.map { seqName => s"create sequence $seqName start with 1;" }
      executeStatements(statements: _*)
    } else if (path.startsWith("/backdoor/create-tables/")) {
      val tableNames = path.substring("/backdoor/create-tables/".length).split(",").toSeq
      val tableDefs  = tableNames.map { tableName => qe.tableMetadata.tableDef(tableName, null) }.toVector
      val generator  = DdlGenerator.hsqldb()
      val statements = generator.schema(tableDefs).split(";[\r\n]+").toSeq
      executeStatements(statements: _*)
    } else if (path.startsWith("/backdoor/drop-sequences/")) {
      val seqNames   = path.substring("/backdoor/drop-sequences/".length).split(",").toSeq
      val statements = seqNames.map { seqName => s"drop sequence $seqName;" }
      executeStatements(statements: _*)
    } else if (path.startsWith("/backdoor/drop-tables/")) {
      val tableNames = path.substring("/backdoor/drop-tables/".length).split(",").toSeq
      val statements = tableNames.map { tableName => s"drop table $tableName;" }
      executeStatements(statements: _*)
    } else {
      throw new IllegalArgumentException(s"Unexpected path: $path")
    }
  }
}
