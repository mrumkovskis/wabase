package wabase.app

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.{HttpRequest, HttpResponse}
import org.wabase.{AppQuerease, DefaultAppQuerease, WabaseServer}
import org.wabase.client.{HttpClient, WabaseHttpClient}

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.control.NonFatal

class RunningServer extends WabaseHttpClient()(ActorSystem("it-http-client")) {

  override protected def initQuerease: AppQuerease = DefaultAppQuerease

  override def login(username: String = null, password: String = null) = {
    ""
  }

  override protected def doRequest(req: HttpRequest, cookieStorage: CookieMap, timeout: FiniteDuration, maxRedirects: Int): Future[HttpResponse] =
    super.doRequest(req.addAttribute(HttpClient.ModeKey, HttpClient.ProxyMode), cookieStorage, timeout, maxRedirects)

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
    implicit val ec: scala.concurrent.ExecutionContext = WabaseServer.app.executor
    Await.result(WabaseServer.unbindFuture, 30.seconds)
  }
}

private object ServerState {
  var started = false
  var ready: Future[_] = _
}
