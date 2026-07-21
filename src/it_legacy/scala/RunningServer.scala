package wabase.app

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.{HttpRequest, HttpResponse}
import org.wabase.{AppQuerease, DefaultAppQuerease}
import org.wabase.client.{HttpClient, WabaseHttpClient}

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.control.NonFatal

class RunningServer extends WabaseHttpClient()(ActorSystem("legacy-it-http-client")) {

  override protected def initQuerease: AppQuerease = DefaultAppQuerease

  override def login(username: String = null, password: String = null) = {
    ""
  }

  private val readyF = ServerState.synchronized {
    if (!ServerState.started) {
      ServerState.started = true
      Server.main(Array.empty)
      ServerState.ready = Server.bindingFuture
    }
    ServerState.ready
  }
  try Await.result(readyF, 30.seconds)
  catch {
    case NonFatal(e) =>
      ServerState.synchronized {
        ServerState.started = false
        ServerState.ready = null
      }
      throw e
  }

  def unbind(): Unit = {
    implicit val ec: scala.concurrent.ExecutionContext = Server.service.executionContext
    Await.result(Server.unbindFuture, 30.seconds)
  }
}

private object ServerState {
  var started = false
  var ready: Future[_] = _
}
