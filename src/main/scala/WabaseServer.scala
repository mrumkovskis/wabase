package org.wabase

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.{HttpRequest, HttpResponse}

import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}
import scala.io.StdIn

class WabaseServer(wabase: WabaseService.Wabase) {
  val port = WabaseServer.port
  private val deferredControl = new WabaseDeferredControl(wabase)(wabase.system)
  private val service         = new WabaseService

  def handle(req: HttpRequest): Future[HttpResponse] =
    service.handle(wabase, deferredControl)(req)(wabase.system)
}

object WabaseServer {

  class App(exec: Execution) extends WabaseApp[WabaseUser]
    with Execution
    with AppBase[WabaseUser]
    with NoAudit[WabaseUser]
    with DbAccess
    with NoAuthorization[WabaseUser]
    with NoCustomConstraintMessage
    with NoValidation
    with Marshalling
    with AppProvider[WabaseUser]
    with JsonConverterProvider
    {
      // Members declared in org.wabase.Execution
      override protected def execution: org.wabase.Execution = exec

      // Members declared in org.wabase.AppProvider
      override type App = AppBase[WabaseUser]
      override protected def initApp: App = this

      // Members declared in org.wabase.JsonConverterProvider
      override protected def initJsonConverter: org.wabase.JsonConverter[?] = qio
    }

  val port = config.getInt("port")

  lazy val shutdownOnKeyPressEnter = config.getBoolean("app.server.shutdown-on-keypress-enter")
  lazy val shutdownOnBindFailed    = config.getBoolean("app.server.shutdown-on-bind-failed")

  lazy val (app, server, bindingFuture) = {
    implicit val serverSystem: ActorSystem  = ActorSystem("wabase-server")
    implicit val ec: ExecutionContext = serverSystem.dispatcher
    val executionImpl = new ExecutionImpl()(serverSystem)
    val app = new App(executionImpl)
    val server = new WabaseServer(app)
    val hostPortString = s"http://localhost:${server.port}"
    // TODO support TLS if configured
    val bindingFuture = Http().newServerAt("0.0.0.0", server.port).bind(server.handle)
    bindingFuture.onComplete {
      case Success(_) =>
        println(
          "\n" +
          s"Server now online at $hostPortString" +
          (if (shutdownOnKeyPressEnter) "\nPress ENTER to stop..." else "") +
          "\n\n"
        )
      case Failure(ex) =>
        println(
          "\n" +
          s"FAILED to start server at $hostPortString because of: ${ex.getMessage}" +
          "\n\n"
        )
        app.system.terminate()
        if (shutdownOnBindFailed) {
          System.exit(1)
        }
    }
    (app, server, bindingFuture)
  }

  def apply(): WabaseServer = server

  def unbind(): Unit = {
    implicit val ec: ExecutionContext = app.executor
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete { _ =>   // and terminate actor system when done
        app.system.terminate()
      }
  }

  def shutdown(): Unit = {
    implicit val ec: ExecutionContext = app.executor
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete { _ =>   // and shutdown when done
        app.system.terminate()
        System.exit(0)
      }
  }

  def main(args: Array[String]): Unit = {
    apply()
    if (shutdownOnKeyPressEnter) {
      StdIn.readLine() // let it run until user presses enter
      shutdown()
    }
  }

  def hello(ctx: WabaseRequestContext): String = "Hello from wabase!"
}
