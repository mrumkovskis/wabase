package wabase.app

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.{HttpRequest, HttpResponse}
import org.wabase.{AppQuerease, DefaultAppQuerease}
import org.wabase.client.{HttpClient, WabaseHttpClient}

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration._

class RunningServer extends WabaseHttpClient()(ActorSystem("legacy-it-http-client")) {

  override protected def initQuerease: AppQuerease = DefaultAppQuerease

  override def login(username: String = null, password: String = null) = {
    ""
  }

  ServerState.synchronized {
    if (!ServerState.is_running) {
      Server.main(Array.empty)
      ServerState.is_running = true
    }
  }

  def unbind(): Unit = {
    implicit val ec: scala.concurrent.ExecutionContext = Server.service.executor
    Await.result(Server.unbindFuture, 30.seconds)
  }
}

private object ServerState {
  var is_running = false
}
