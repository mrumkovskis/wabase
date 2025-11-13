package org.wabase
package client

import com.typesafe.config.Config
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.HttpMethods.POST
import org.apache.pekko.http.scaladsl.model.{HttpEntity, HttpRequest, HttpResponse, StatusCodes}
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.http.scaladsl.testkit.ScalatestRouteTest
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.scalatest.matchers.should.Matchers
import org.wabase.AppQuerease.InjectionParametersContext

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps


object Teapot extends RestClient {
  override def doRequest(req: HttpRequest): Future[HttpResponse] =
    Future.successful(HttpResponse(StatusCodes.ImATeapot))
}

class FakeClient(clientCfg: Config = HttpClientConfig.componentConfs.root) extends RestClient(clientCfg) {
  override def doRequest(req: HttpRequest): Future[HttpResponse] =
    Future.successful(HttpResponse(entity = clientCfg.getString("fake-response")))
}

class RestClientTest  extends FlatSpec with Matchers with ScalatestRouteTest with BeforeAndAfterAll with Loggable{
  behavior of "RestClient"
  val client     = new RestClient(HttpClientConfig("slow"))
  val fastClient = new RestClient(HttpClientConfig("fast"))

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

  it should "construct extended client" in {
    val clientCfg = HttpClientConfig.configs("teapot")
    val client = getObjectOrNewInstance[HttpClient](clientCfg, "client-class", "http client")
    client shouldBe Teapot
    val request = HttpRequest(POST, entity = HttpEntity("BREW"))
    val injection = InjectionParametersContext(request)
    Await.result(
      HttpClientConfig.httpClientFactory.createHttpClients("teapot")(injection)(request),
      1 second,
    ).status shouldBe StatusCodes.ImATeapot
  }

  it should "construct extended client with config" in {
    val request = HttpRequest(POST, entity = HttpEntity("BREW"))
    val injection = InjectionParametersContext(request)
    Await.result(
      HttpClientConfig.httpClientFactory.createHttpClients("fake_1")(injection)(request)
        .flatMap(_.entity.toStrict(1.second)),
      1 second,
    ).data.utf8String shouldBe "so fake"
    Await.result(
      HttpClientConfig.httpClientFactory.createHttpClients("fake_2")(injection)(request)
        .flatMap(_.entity.toStrict(1.second)),
      1 second,
    ).data.utf8String shouldBe "fake again"
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
