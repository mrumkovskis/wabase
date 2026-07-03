package wabase.app

import org.apache.pekko.actor.{ActorSystem, Terminated}
import org.apache.pekko.http.scaladsl.Http
import org.wabase._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object Server extends scala.App with Loggable {
  val bindAddress     = config.getString("app.server.bind-address")
  val port            = config.getInt("port")
  val hostPortString  = s"http://localhost:$port"

  val service = new Service(ActorSystem("legacy-service-test"))
  implicit val ec: ExecutionContext = service.executor
  implicit val ss: ActorSystem      = service.system

  val bindingFuture = {
    Http().newServerAt(bindAddress, port).bindFlow(service.route)
  }
  bindingFuture.onComplete {
    case Success(_) =>
      println(
        "\n" +
        s"Server now online at $hostPortString" +
        "\n\n"
      )
    case Failure(ex) =>
      println(
        "\n" +
        s"FAILED to start server at $hostPortString because of: ${ex.getMessage}" +
        "\n\n"
      )
      service.system.terminate()
  }
  def unbind(): Unit = {
    val _ = unbindFuture
  }

  def unbindFuture(implicit ec: ExecutionContext): Future[Terminated] =
    bindingFuture
      .flatMap(_.unbind())  // trigger unbinding from the port
      .recover { case _ => null }
      .flatMap { _ =>       // and terminate actor system when done
       service.system.terminate()
      }
}
