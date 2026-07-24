package wabase.app

import org.wabase.{AppQuerease, DefaultAppQuerease, WabaseServer}
import org.wabase.client.WabaseHttpClient

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.control.NonFatal

class RunningServer extends WabaseHttpClient()(WabaseServer.app.actorSystem) {

  override protected def initQuerease: AppQuerease = DefaultAppQuerease

  override def login(username: String = null, password: String = null) = {
    ""
  }

  private val readyF = ServerState.synchronized {
    if (!ServerState.started) {
      ServerState.started = true
      WabaseServer.main(Array.empty)
      ServerState.ready = WabaseServer.bindingFuture
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
    Await.result(WabaseServer.unbindFuture, 30.seconds)
  }
}

private object ServerState {
  var started = false
  var ready: Future[_] = _
}
