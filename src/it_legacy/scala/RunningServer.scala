package wabase.app

import org.wabase.client.WabaseHttpClient
import org.wabase.{AppQuerease, DefaultAppQuerease}

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
