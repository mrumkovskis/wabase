package org.wabase

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.mojoz.metadata.in.YamlMd
import org.wabase.config

import scala.concurrent.ExecutionContext
import scala.io.StdIn

object WabaseServer {
  def main(args: Array[String]): Unit = {
    val port = config.getInt("port")

    implicit val serverSystem: ActorSystem  = ActorSystem("wabase-server")
    implicit val ec: ExecutionContext = serverSystem.dispatcher
    val executionImpl = new ExecutionImpl()(serverSystem)

    val wabase: WabaseService.Wabase = new WabaseApp[WabaseUser]
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
      override protected def execution: org.wabase.Execution = executionImpl

      // Members declared in org.wabase.AppProvider
      override type App = AppBase[WabaseUser]
      override protected def initApp: App = this

      // Members declared in org.wabase.JsonConverterProvider
      override protected def initJsonConverter: org.wabase.JsonConverter[?] = qio

      // Members declared in org.wabase.QuereaseProvider
      override protected def initQuerease: AppQuerease = new AppQuerease {
        override lazy val yamlMetadata = YamlMd.fromResource("/routes.yaml")
      }
    }

    val deferredControl = new WabaseDeferredControl(wabase)
    val service         = new WabaseService

    // TODO support TLS if configured
    val bindingFuture = Http().newServerAt("0.0.0.0", port).bind(service.handle(wabase, deferredControl))

    println(s"Server now online. Please navigate to http://localhost:8080/hi\nPress RETURN to stop...")
    StdIn.readLine() // let it run until user presses return
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ => serverSystem.terminate()) // and shutdown when done
  }

  /* Request mapper */
  def setViewName(ctx: WabaseRequestContext): WabaseRequestContext = ctx.copy(viewName = "fakeViewName")
}
