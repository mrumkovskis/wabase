package org.wabase

import java.util.concurrent.TimeoutException

import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.scalatest.matchers.should.Matchers
import org.wabase.client.RestClient

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

class RestClientTest  extends FlatSpec with Matchers with ScalatestRouteTest with BeforeAndAfterAll with Loggable{
  behavior of "RestClient"
  val client = new RestClient{
  }
  val fastClient = new RestClient{
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
    val resp = client.httpGet[String](s"ok")
    resp should be ("HELLO")
  }

  it should "properly time out delayed response" in {
    intercept[TimeoutException] {fastClient.httpGet[String]("timeout")}.getMessage should be ("The stream has not been completed in 2 seconds.")
  }

  it should "properly handle multiple requests in parallel" in {
    import scala.concurrent._
    val results = (1 to 100).map { i =>
      Future {(i, client.httpGet[String](s"counter/$i"))}
        .filter { case (counter, response) => s"RESULT $counter" == response }
    }
    val res = Await.result(Future.foldLeft(results)(0){ case (c, _) => c + 1 }, 1 minute)
    res should be (100)
  }
}
