package wabase.app

import org.wabase.{BusinessScenariosBaseSpecs, WabaseServer}

import scala.util.Random

class BusinessScenariosSpecs extends BusinessScenariosBaseSpecs("http_tests") {
  lazy val server = new RunningServer
  override def initHttpClient = server
  override def beforeAll() = {
  }
  override def afterAll() = {
    server.unbind() // unbind for cross-scala tests
  }
}
