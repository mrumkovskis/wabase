package org.wabase

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.{HttpRequest, HttpResponse}

import scala.collection.immutable.Seq
import scala.concurrent.ExecutionContext
import scala.io.StdIn

class WabaseServer(wabase: WabaseService.Wabase) {
  val port = config.getInt("port")
  private val deferredControl = new WabaseDeferredControl(wabase)(wabase.system)
  private val service         = new WabaseService

  def handle(req: HttpRequest) = service.handle(wabase, deferredControl)(req)(wabase.system)
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

  def main(args: Array[String]): Unit = {
    implicit val serverSystem: ActorSystem  = ActorSystem("wabase-server")
    implicit val ec: ExecutionContext = serverSystem.dispatcher
    val executionImpl = new ExecutionImpl()(serverSystem)
    val server = new WabaseServer(new App(executionImpl))
    // TODO support TLS if configured
    val bindingFuture = Http().newServerAt("0.0.0.0", server.port).bind(server.handle)

    println(s"Server now online. Please navigate to http://localhost:8080/hi\nPress RETURN to stop...")
    StdIn.readLine() // let it run until user presses return
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ => serverSystem.terminate()) // and shutdown when done
  }

  def hello(ctx: WabaseRequestContext): HttpResponse = HttpResponse(entity = "Hello from wabase!")
}
