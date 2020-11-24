package org.wabase

import java.util.concurrent.TimeoutException

import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.StatusCode
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.scalatest.matchers.should.Matchers
import org.wabase.client.RestClient
import org.wabase.client.ClientException

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

class RestClientTest  extends FlatSpec with Matchers with ScalatestRouteTest with BeforeAndAfterAll with Loggable{
  behavior of "RestClient"
  class TestClient extends RestClient {
    def handleError(e: Throwable): Nothing = e match{
      case error if error.getCause != null => handleError(error.getCause)
      case error: TimeoutException =>
        logger.error(error.getMessage, error)
        throw new TimeoutException(error.getMessage)
      case error: ClientException => throw new ClientException(error.getMessage, error, error.status, error.responseContent, error.request)
      case error => throw ClientException(error)
    }
    override def requestFailed(
        message: String, cause: Throwable,
        status: StatusCode = null, content: String = null, request: HttpRequest = null): Nothing =
      try super.requestFailed(message, cause, status, content, request) catch {
        case e: Throwable => handleError(e)
      }
  }
  val client = new TestClient{
    override val requestTimeout = 50 seconds
    override val awaitTimeout   = 55 seconds
  }
  val fastClient = new TestClient{
    override val requestTimeout = 2 seconds
  }

  val route: Route = {
    path("ok") {complete{"HELLO"}} ~
    path("timeout") {complete{Thread.sleep(5000);"HELLO"}} ~
    path("counter" / LongNumber) {num => complete{Thread.sleep(200);s"RESULT $num"}}
  }

  val binding = Await.result(Http().newServerAt("0.0.0.0", client.port).bindFlow(route), 1 minute)

  override def afterAll() = Await.result(binding.unbind(), 1 minute)

  it should "work" in {
    val resp = client.httpGetAwait[String](s"ok")
    resp should be ("HELLO")
  }

  it should "properly time out delayed response" in {
    intercept[TimeoutException] {fastClient.httpGetAwait[String]("timeout")}.getMessage should be ("The stream has not been completed in 2 seconds.")
  }

  it should "properly handle multiple requests in parallel" in {
    import scala.concurrent._
    val results = (1 to 100).map { i =>
      Future {(i, client.httpGetAwait[String](s"counter/$i"))}
        .filter { case (counter, response) => s"RESULT $counter" == response }
    }
    val res = Await.result(Future.foldLeft(results)(0){ case (c, _) => c + 1 }, 1 minute)
    res should be (100)
  }
}
