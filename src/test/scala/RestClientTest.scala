package org.wabase
package client

import com.typesafe.config.Config
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.HttpMethods.{GET, POST, PUT}
import org.apache.pekko.http.scaladsl.model.Uri
import org.apache.pekko.http.scaladsl.model.headers.{Authorization, BasicHttpCredentials, Cookie, HttpCookie, Location, RawHeader, `Set-Cookie`}
import org.apache.pekko.http.scaladsl.model.{HttpEntity, HttpHeader, HttpRequest, HttpResponse, StatusCodes}
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.http.scaladsl.testkit.ScalatestRouteTest

import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.scalatest.matchers.should.Matchers
import org.wabase.AppQuerease.InjectionParametersContext

import scala.collection.immutable.{Seq => iSeq}
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps


object Teapot extends RestClient()(ActorSystem("teapot")) {
  override def doRequest(req: HttpRequest): Future[HttpResponse] =
    Future.successful(HttpResponse(StatusCodes.ImATeapot))
}

class FakeClient(clientCfg: Config = HttpClientConfig.componentConfs.root)(implicit system: ActorSystem)
    extends RestClient(clientCfg)(system) {
  override def doRequest(req: HttpRequest): Future[HttpResponse] =
    Future.successful(HttpResponse(entity = clientCfg.getString("fake-response")))
}

class RestClientTest  extends FlatSpec with Matchers with ScalatestRouteTest with BeforeAndAfterAll with Loggable{
  behavior of "RestClient"
  val client     = new RestClient(HttpClientConfig("slow"))
  val fastClient = new RestClient(HttpClientConfig("fast"))

  val server_port= HttpClientConfig("slow").getInt("server-port")

  val route: Route = {
    path("ok") {complete{"HELLO"}} ~
    path("timeout") {complete{Thread.sleep(5000);"HELLO"}} ~
    path("uri-echo") { extractUri { uri => complete(uri.toString) } } ~
    path("counter" / LongNumber) {num => complete{Thread.sleep(200);s"RESULT $num"}} ~
    path("redirect-abs-path") {
      get {
        complete(HttpResponse(status = StatusCodes.Found, headers = List(Location(Uri("/uri-echo")))))
      }
    } ~
    path("redirect-parent-relative" / Segment) { id =>
      put {
        complete(HttpResponse(
          status = StatusCodes.SeeOther,
          headers = List(Location(Uri(s"../resource?/$id")))))
      }
    } ~
    path("resource") {
      extractUri { uri => complete(uri.toString) }
    } ~
    path("redirect-drop-content-headers") {
      put {
        complete(HttpResponse(
          status = StatusCodes.SeeOther,
          headers = List(Location(Uri("/echo-content-headers")))))
      }
    } ~
    path("echo-content-headers") {
      extractRequest { req =>
        val names = req.headers
          .map(_.lowercaseName)
          .filter(RestClient.contentHeaderNames.contains)
          .sorted
          .mkString(",")
        // also surface entity content-type if present (not Empty)
        val ct =
          if (req.entity.isKnownEmpty) ""
          else req.entity.contentType.toString
        complete(if (names.isEmpty && ct.isEmpty) "none" else s"$names|$ct")
      }
    } ~
    path("redirect-same-origin-auth") {
      get {
        complete(HttpResponse(
          status = StatusCodes.Found,
          headers = List(Location(Uri("/echo-auth")))))
      }
    } ~
    path("echo-auth") {
      extractRequest { req =>
        val auth = req.header[Authorization].map(_.value).getOrElse("no-auth")
        complete(auth)
      }
    } ~
    path("redirect-cross-origin-auth") {
      get {
        complete(HttpResponse(
          status = StatusCodes.Found,
          // Same host, different port — different origin
          headers = List(Location(Uri(s"http://127.0.0.1:$server_port/echo-auth")))))
      }
    } ~
    path("set-host-cookie") {
      get {
        complete(HttpResponse(
          status = StatusCodes.OK,
          headers = List(`Set-Cookie`(HttpCookie("sid", "secret-session"))),
          entity = "ok"))
      }
    } ~
    path("redirect-cross-origin-cookie") {
      get {
        complete(HttpResponse(
          status = StatusCodes.Found,
          headers = List(Location(Uri(s"http://127.0.0.1:$server_port/echo-cookie")))))
      }
    } ~
    path("echo-cookie") {
      extractRequest { req =>
        val cookie = req.header[Cookie].map(_.value).getOrElse("no-cookie")
        complete(cookie)
      }
    }
  }

  val binding = Await.result(Http().newServerAt("0.0.0.0", server_port).bindFlow(route), 1 minute)

  override def afterAll() = Await.result(binding.unbind(), 1 minute)

  it should "work" in {
    val resp = client.httpGetAwait[String](s"ok")
    resp should be ("HELLO")
  }

  it should "construct extended client" in {
    val clientCfg = HttpClientConfig.configs("teapot")
    @annotation.nowarn("msg=Manifest")
    val client = getObjectOrNewInstance[HttpClient](clientCfg, "client-class", "http client")
    client shouldBe Teapot
    val request = HttpRequest(POST, entity = HttpEntity("BREW"))
    val injection = InjectionParametersContext(request)
    val httpClients = HttpClientConfig.httpClientFactory.createHttpClients
    Await.result(
      httpClients("teapot")(injection)(request),
      1 second,
    ).status shouldBe StatusCodes.ImATeapot
  }

  it should "construct extended client with config" in {
    val request = HttpRequest(POST, entity = HttpEntity("BREW"))
    val injection = InjectionParametersContext(request)
    val httpClients = HttpClientConfig.httpClientFactory.createHttpClients
    Await.result(
      httpClients("fake_1")(injection)(request)
        .flatMap(_.entity.toStrict(1.second)),
      1 second,
    ).data.utf8String shouldBe "so fake"
    Await.result(
      httpClients("fake_2")(injection)(request)
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

  it should "resolve relative Location per RFC 3986" in {
    val base = Uri(s"http://localhost:$server_port/name/42")
    RestClient.resolveRedirectUri(base, Uri("name?/42")).toString shouldBe
      s"http://localhost:$server_port/name/name?/42"
    RestClient.resolveRedirectUri(base, Uri("/name?/42")).toString shouldBe
      s"http://localhost:$server_port/name?/42"
    RestClient.resolveRedirectUri(base, Uri("../name?/42")).toString shouldBe
      s"http://localhost:$server_port/name?/42"
  }

  it should "follow redirect with absolute-path Location" in {
    val resp = client.httpGetAwait[String]("redirect-abs-path")
    resp should include ("uri-echo")
  }

  it should "follow 303 redirect with GET and parent-relative Location" in {
    val response = Await.result(
      client.doRequest(HttpRequest(PUT, uri = s"http://localhost:$server_port/redirect-parent-relative/42")),
      1.second)
    val body = Await.result(response.entity.toStrict(1.second).map(_.data.utf8String.trim), 1.second)
    body shouldBe s"http://localhost:$server_port/resource?/42"
  }

  it should "detect same origin by scheme host and port" in {
    val a = Uri(s"http://localhost:$server_port/a")
    RestClient.isSameOrigin(a, Uri(s"http://localhost:$server_port/b")) shouldBe true
    RestClient.isSameOrigin(a, Uri(s"https://localhost:$server_port/b")) shouldBe false
    RestClient.isSameOrigin(a, Uri(s"http://127.0.0.1:$server_port/b")) shouldBe false
    RestClient.isSameOrigin(a, Uri(s"http://localhost:${server_port + 1}/b")) shouldBe false
  }

  it should "strip Authorization Cookie and Host on cross-origin redirect headers" in {
    val from = Uri("https://api.example.com/v1")
    val to = Uri("https://other.example.com/v1")
    val headers: iSeq[HttpHeader] = iSeq(
      Authorization(BasicHttpCredentials("u", "p")),
      Cookie("sid", "1"),
      RawHeader("X-Custom", "keep"),
      RawHeader("Host", "api.example.com"),
    )
    val same = RestClient.redirectRequestHeaders(from, Uri("https://api.example.com/other"), headers)
    same should have size 4
    val cross = RestClient.redirectRequestHeaders(from, to, headers)
    cross.map(_.lowercaseName).toSet shouldBe Set("x-custom")
  }

  it should "strip content-related headers when method becomes GET on redirect" in {
    val from = Uri("https://api.example.com/v1")
    val sameOrigin = Uri("https://api.example.com/other")
    val headers: iSeq[HttpHeader] = iSeq(
      RawHeader("X-Custom", "keep"),
      RawHeader("Content-Type", "application/json"),
      RawHeader("Content-Length", "12"),
      RawHeader("Content-Encoding", "gzip"),
      RawHeader("Content-Language", "en"),
      RawHeader("Content-Location", "https://api.example.com/body"),
      RawHeader("Digest", "sha-256=abc"),
      RawHeader("Last-Modified", "Mon, 01 Jan 2020 00:00:00 GMT"),
      Authorization(BasicHttpCredentials("u", "p")),
    )
    val dropped = RestClient.redirectRequestHeaders(from, sameOrigin, headers, dropContentHeaders = true)
    dropped.map(_.lowercaseName).toSet shouldBe Set("x-custom", "authorization")
    val kept = RestClient.redirectRequestHeaders(from, sameOrigin, headers, dropContentHeaders = false)
    kept.map(_.lowercaseName).toSet should contain allOf (
      "content-type", "content-length", "content-encoding", "content-language",
      "content-location", "digest", "last-modified", "x-custom", "authorization",
    )
  }

  it should "not send content headers after 303 redirect to GET" in {
    val response = Await.result(
      client.doRequest(HttpRequest(
        PUT,
        uri = s"http://localhost:$server_port/redirect-drop-content-headers",
        entity = HttpEntity("""{"a":1}"""),
        headers = iSeq(
          RawHeader("Content-Language", "en"),
          RawHeader("Content-Encoding", "identity"),
          RawHeader("X-Custom", "keep"),
        ),
      )),
      2.seconds)
    val body = Await.result(response.entity.toStrict(1.second).map(_.data.utf8String.trim), 1.second)
    body shouldBe "none"
  }

  it should "keep Authorization on same-origin redirect" in {
    val auth = Authorization(BasicHttpCredentials("u", "p"))
    val resp = client.httpGetAwait[String](
      "redirect-same-origin-auth",
      headers = iSeq(auth),
    )
    resp should include ("Basic")
  }

  it should "strip Authorization on cross-origin redirect" in {
    val auth = Authorization(BasicHttpCredentials("u", "p"))
    val response = Await.result(
      client.doRequest(HttpRequest(
        GET,
        uri = s"http://localhost:$server_port/redirect-cross-origin-auth",
        headers = iSeq(auth),
      )),
      2.seconds)
    val body = Await.result(response.entity.toStrict(1.second).map(_.data.utf8String.trim), 1.second)
    body shouldBe "no-auth"
  }

  it should "scope host-only cookies to the host that set them" in {
    val cookies = new client.CookieMap
    val local = Uri(s"http://localhost:$server_port/set-host-cookie")
    val other = Uri(s"http://127.0.0.1:$server_port/echo-cookie")
    cookies.setCookiesFromHeaders(
      iSeq(`Set-Cookie`(HttpCookie("sid", "secret-session"))),
      local,
    )
    cookies.map.keySet should contain ("sid")
    cookies.getCookies(local).flatMap(_.cookies.map(_.name)) should contain ("sid")
    cookies.getCookies(other) shouldBe empty
  }

  it should "be thread-safe under concurrent jar updates and reads" in {
    val cookies = new client.CookieMap
    val uri = Uri(s"http://localhost:$server_port/")
    val writers = (1 to 8).map { t =>
      Future {
        (1 to 200).foreach { i =>
          val name = s"c${t}_$i"
          cookies.setCookiesFromHeaders(
            iSeq(`Set-Cookie`(HttpCookie(name, s"v$i"))),
            uri,
          )
          cookies.getCookies(uri)
          cookies.map
        }
      }
    }
    val reader = Future {
      (1 to 500).foreach { _ =>
        cookies.getCookies
        cookies.getCookies(uri)
        cookies.map.keySet
      }
    }
    Await.result(Future.sequence(writers :+ reader), 10.seconds)
    cookies.map.size shouldBe 8 * 200
  }

  it should "not send host-only cookies on cross-origin redirect" in {
    // Warm cookie jar via a client that shares getCookieStorage
    val jarClient = new RestClient(HttpClientConfig("slow")) {
      override def doRequest(req: HttpRequest): Future[HttpResponse] =
        doRequest(req, getCookieStorage, requestTimeout)
    }
    Await.result(
      jarClient.doRequest(HttpRequest(GET, uri = s"http://localhost:$server_port/set-host-cookie")),
      2.seconds)
    jarClient.getCookieStorage.map.keySet should contain ("sid")
    val response = Await.result(
      jarClient.doRequest(HttpRequest(
        GET,
        uri = s"http://localhost:$server_port/redirect-cross-origin-cookie",
      )),
      2.seconds)
    val body = Await.result(response.entity.toStrict(1.second).map(_.data.utf8String.trim), 1.second)
    body shouldBe "no-cookie"
  }

  it should "allow query in path, append params" in {
    def echo(path: String, params: Map[String, Any] = Map.empty) =
      Option(Await.result(client.httpGet[String](path, params), 1 second)).map(echoed =>
        echoed.substring(echoed.indexOf(server_port.toString) + server_port.toString.length + 1)
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
