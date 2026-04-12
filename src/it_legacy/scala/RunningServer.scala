package wabase.app

import org.apache.pekko.http.scaladsl.model.{HttpRequest, HttpResponse}
import org.wabase.{AppQuerease, DefaultAppQuerease}
import org.wabase.client.{HttpClient, WabaseHttpClient}

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

class RunningServer extends WabaseHttpClient {

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

  def unbind(): Unit = Server.unbind()
}

private object ServerState {
  var is_running = false
}
