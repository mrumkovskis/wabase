package wabase.app

import org.apache.pekko.http.scaladsl.model.HttpMethods
import org.wabase.{AppQuerease, DefaultAppQuerease, WabaseServer}
import org.wabase.client.WabaseHttpClient
import org.scalatest.matchers.should.Matchers

import scala.language.reflectiveCalls

class RunningServer extends WabaseHttpClient {

  override protected def initQuerease: AppQuerease = DefaultAppQuerease
  override lazy val port = WabaseServer.port

  override def login(username: String = null, password: String = null) = {
    ""
  }

  ServerState.synchronized {
    if (!ServerState.is_running) {
      WabaseServer.main(Array.empty)
      ServerState.is_running = true
    }
  }

  def unbind() = WabaseServer.unbind()
}

private object ServerState {
  var is_running = false
}
