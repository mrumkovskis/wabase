package wabase.app

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.server.Directives.{complete, handleExceptions, onSuccess, pathPrefix, provide}
import org.apache.pekko.http.scaladsl.server.Route
import org.wabase._

object TestDbAccess extends DbAccess with QuereaseProvider with Loggable {
  override protected def tresqlMetadata = DefaultAppQuerease.tresqlMetadata
}

case class TestUser()

object TestApp extends AppBase[TestUser]
    with AppFileStreamer[TestUser]
    with DbAccessDelegate
    with NoAudit[TestUser]
    with PostgreSqlConstraintMessage
{
  override protected def initQuerease = DefaultAppQuerease
  override def dbAccessDelegate: DbAccess = TestDbAccess
  override def check[C <: RequestContext[_]](ctx: C, clazz: Class[_]): Unit = {}
  override def relevant[C <: RequestContext[_]](ctx: C, clazz: Class[_]) = ctx
  override def hasRole(user: TestUser, roles: Set[String]): Boolean = true
}

class Service(system: ActorSystem) extends ExecutionImpl()(system)
    with AppServiceBase[TestUser]
    with AppFileServiceBase[TestUser]
    with ConstantQueryTimeout
    with NoServerStatistics
    with SimpleExceptionHandler
{
  override type App = TestApp.type
  override def initApp: App = TestApp
  override def initFileStreamer: AppFileStreamer[TestUser] = TestApp
  implicit val user: TestUser = TestUser()
  implicit val state: ApplicationState = ApplicationState(Map.empty)
  val route = Route.seal {
    handleExceptions(appExceptionHandler) {
      (pathPrefix("upload") & provide(None)) { filenameOpt =>
        val ufd = extractFileDirective(filenameOpt)
          .andThen(uploadFileDirective _)
          .flatMap(onSuccess(_))

        ufd { fileInfo =>
          complete(fileInfo.sha_256)
        }
      }
    }
  }
}
