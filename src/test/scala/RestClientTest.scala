package org.wabase

import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.scalatest.matchers.should.Matchers
import org.wabase.client.{ClientException, RestClient}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

class RestClientTest  extends FlatSpec with Matchers with ScalatestRouteTest with BeforeAndAfterAll with Loggable{
  behavior of "RestClient"
  val client = new RestClient{
    override val requestTimeout = 50 seconds
    override val awaitTimeout   = 55 seconds
  }
  val fastClient = new RestClient{
    override val requestTimeout = 2 seconds
  }

  val route: Route = {
    path("ok") {complete{"HELLO"}} ~
    path("timeout") {complete{Thread.sleep(5000);"HELLO"}} ~
    path("uri-echo") { extractUri { uri => complete(uri.toString) } } ~
    path("counter" / LongNumber) {num => complete{Thread.sleep(200);s"RESULT $num"}}
  }

  val binding = Await.result(Http().newServerAt("0.0.0.0", client.port).bindFlow(route), 1 minute)

  override def afterAll() = Await.result(binding.unbind(), 1 minute)

  it should "work" in {
    val resp = client.httpGetAwait[String](s"ok")
    resp should be ("HELLO")
  }

  it should "properly time out delayed response" in {
    val errorMessage =
      intercept[ClientException] {
        fastClient.httpGetAwait[String]("timeout")
      }.getMessage
    errorMessage should include ("Request GET http://localhost:8080/timeout failed")
    errorMessage should include ("The stream has not been completed in 2 seconds")
  }

  it should "properly handle multiple requests in parallel" in {
    import scala.concurrent._
    val results = (1 to 100).map { i =>
      Future(i)
        .flatMap(i => client.httpGet[String](s"counter/$i"))
        .map(response => (i, response))
        .filter { case (counter, response) => s"RESULT $counter" == response }
    }
    val res = Await.result(Future.foldLeft(results)(0){ case (c, _) => c + 1 }, 1 minute)
    res should be (100)
  }

  it should "allow query in path, append params" in {
    def echo(path: String, params: Map[String, Any] = Map.empty) =
      Option(Await.result(client.httpGet[String](path, params), 1 second)).map(echoed =>
        echoed.substring(echoed.indexOf(client.port.toString) + client.port.toString.length + 1)
      ).get
    val q1 = Map("q" -> 1)
    echo("uri-echo")                    shouldBe "uri-echo"
    echo("uri-echo", q1)                shouldBe "uri-echo?q=1"
    echo("uri-echo?q")                  shouldBe "uri-echo?q"
    echo("uri-echo?q", q1)              shouldBe "uri-echo?q&q=1"
    echo("uri-echo?/key1/key2")         shouldBe "uri-echo?/key1/key2"
    echo("uri-echo?/key1/key2", q1)     shouldBe "uri-echo?/key1/key2?q=1"
    echo("uri-echo?/key1/key2?q=0")     shouldBe "uri-echo?/key1/key2?q=0"
    echo("uri-echo?/key1/key2?q=0", q1) shouldBe "uri-echo?/key1/key2?q=0&q=1"
    echo("uri-echo?/spec%2Fkey1/spec%3Dkey2%3F")          shouldBe "uri-echo?/spec%2Fkey1/spec%3Dkey2%3F"
    echo("uri-echo?/spec%2Fkey1/spec%3Dkey2%3F", q1)      shouldBe "uri-echo?/spec%2Fkey1/spec%3Dkey2%3F?q=1"
    echo("uri-echo?/spec%2Fkey1/spec%3Dkey2%3F?q=0")      shouldBe "uri-echo?/spec%2Fkey1/spec%3Dkey2%3F?q=0"
    echo("uri-echo?/spec%2Fkey1/spec%3Dkey2%3F?q=0", q1)  shouldBe "uri-echo?/spec%2Fkey1/spec%3Dkey2%3F?q=0&q=1"
  }
}
