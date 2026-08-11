package org.wabase
package client

import com.typesafe.config.Config
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.HttpMethods.{GET, POST, PUT}
import org.apache.pekko.http.scaladsl.model.Uri
import org.apache.pekko.http.scaladsl.model.headers.{Authorization, BasicHttpCredentials, Cookie, HttpCookie, Location, RawHeader, `Set-Cookie`}
import org.apache.pekko.http.scaladsl.model.{DateTime, HttpEntity, HttpHeader, HttpRequest, HttpResponse, StatusCodes}
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
    } ~
    path("redirect-to-error") {
      get {
        complete(HttpResponse(
          status = StatusCodes.Found,
          headers = List(Location(Uri("/error-endpoint")))))
      }
    } ~
    path("error-endpoint") {
      get {
        complete(HttpResponse(status = StatusCodes.NotFound, entity = "missing"))
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

  it should "preserve final response status and content in ClientException after redirect" in {
    val ex = intercept[ClientException] {
      client.httpGetAwait[String]("redirect-to-error")
    }
    ex.status shouldBe StatusCodes.NotFound
    Option(ex.responseContent).getOrElse("") should include ("missing")
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
    cookies.map.keys.map(_.name) should contain ("sid")
    cookies.getCookies(local).flatMap(_.cookies.map(_.name)) should contain ("sid")
    cookies.getCookies(other) shouldBe empty
  }

  it should "key cookies by name domain and path so same name does not interfere" in {
    val cookies = new client.CookieMap
    val hostA = Uri("http://a.example.com/app/x")
    val hostB = Uri("http://b.example.com/app/x")
    cookies.setCookiesFromHeaders(
      iSeq(`Set-Cookie`(HttpCookie("sid", "from-a"))),
      hostA,
    )
    cookies.setCookiesFromHeaders(
      iSeq(`Set-Cookie`(HttpCookie("sid", "from-b"))),
      hostB,
    )
    cookies.map.size shouldBe 2
    // default-path of /app/x is /app
    cookies.map.keySet should contain (RestClient.CookieKey("sid", "a.example.com", "/app"))
    cookies.map.keySet should contain (RestClient.CookieKey("sid", "b.example.com", "/app"))
    val aVal = cookies.getCookies(Uri("http://a.example.com/app/y")).flatMap(_.cookies.map(_.value))
    val bVal = cookies.getCookies(Uri("http://b.example.com/app/y")).flatMap(_.cookies.map(_.value))
    aVal should contain ("from-a")
    bVal should contain ("from-b")
  }

  it should "keep distinct path cookies with the same name and domain" in {
    val cookies = new client.CookieMap
    val base = Uri("http://example.com/")
    cookies.setCookiesFromHeaders(
      iSeq(
        `Set-Cookie`(HttpCookie("sid", "root", path = Some("/"))),
        `Set-Cookie`(HttpCookie("sid", "api", path = Some("/api"))),
      ),
      base,
    )
    cookies.map.size shouldBe 2
    cookies.getCookies(Uri("http://example.com/")).flatMap(_.cookies.map(_.value)) should contain ("root")
    cookies.getCookies(Uri("http://example.com/")).flatMap(_.cookies.map(_.value)) should not contain "api"
    cookies.getCookies(Uri("http://example.com/api/v1")).flatMap(_.cookies.map(_.value)).toSet shouldBe Set("root", "api")
  }

  it should "order Cookie header by longer path first then creation time (RFC 6265 §5.4)" in {
    val cookies = new client.CookieMap
    val base = Uri("http://example.com/")
    // Shorter path stored first; longer path must still appear first in Cookie header
    cookies.setCookiesFromHeaders(
      iSeq(`Set-Cookie`(HttpCookie("sid", "root", path = Some("/")))),
      base,
    )
    cookies.setCookiesFromHeaders(
      iSeq(`Set-Cookie`(HttpCookie("sid", "api", path = Some("/api")))),
      base,
    )
    cookies.getCookies(Uri("http://example.com/api/v1")).flatMap(_.cookies.map(_.value)) shouldBe
      List("api", "root")

    // Equal path length: earlier creation-time first
    val equalPath = new client.CookieMap
    equalPath.setCookiesFromHeaders(
      iSeq(`Set-Cookie`(HttpCookie("first", "1", path = Some("/")))),
      base,
    )
    equalPath.setCookiesFromHeaders(
      iSeq(`Set-Cookie`(HttpCookie("second", "2", path = Some("/")))),
      base,
    )
    equalPath.getCookies(Uri("http://example.com/")).flatMap(_.cookies.map(_.name)) shouldBe
      List("first", "second")

    // Replacement of same (name, domain, path) keeps original creation-time ordering
    equalPath.setCookiesFromHeaders(
      iSeq(`Set-Cookie`(HttpCookie("first", "1b", path = Some("/")))),
      base,
    )
    equalPath.getCookies(Uri("http://example.com/")).flatMap(_.cookies.map(c => c.name -> c.value)) shouldBe
      List("first" -> "1b", "second" -> "2")
  }

  it should "use default-path when Path is absent, empty, or does not start with / (RFC 6265 §5.2.4)" in {
    val cookies = new client.CookieMap
    // default-path of /app/page is /app
    val from = Uri("http://example.com/app/page")
    cookies.setCookiesFromHeaders(
      iSeq(
        `Set-Cookie`(HttpCookie("no_path", "1")),
        `Set-Cookie`(HttpCookie("empty_path", "2", path = Some(""))),
        `Set-Cookie`(HttpCookie("relative_path", "3", path = Some("foo"))),
        `Set-Cookie`(HttpCookie("absolute_path", "4", path = Some("/api"))),
      ),
      from,
    )
    cookies.map.keySet should contain (RestClient.CookieKey("no_path", "example.com", "/app"))
    cookies.map.keySet should contain (RestClient.CookieKey("empty_path", "example.com", "/app"))
    cookies.map.keySet should contain (RestClient.CookieKey("relative_path", "example.com", "/app"))
    cookies.map.keySet should contain (RestClient.CookieKey("absolute_path", "example.com", "/api"))
    // Stored cookie path attributes are normalized
    cookies.map.values.filter(_.name != "absolute_path").flatMap(_.path).toSet shouldBe Set("/app")
    cookies.map.values.find(_.name == "absolute_path").flatMap(_.path) shouldBe Some("/api")
    // Scope follows default-path /app
    cookies.getCookies(Uri("http://example.com/app/other")).flatMap(_.cookies.map(_.name)).toSet should
      contain allOf ("no_path", "empty_path", "relative_path")
    cookies.getCookies(Uri("http://example.com/")).flatMap(_.cookies.map(_.name)) should not contain "no_path"
    cookies.getCookies(Uri("http://example.com/api/x")).flatMap(_.cookies.map(_.name)) should contain ("absolute_path")
  }

  it should "use defaultCookiePath for empty path in setCookies" in {
    val cookies = new client.CookieMap
    cookies.setCookies(Map("a" -> "1"), path = Some(""))
    cookies.setCookies(Map("b" -> "2"), path = Some("relative"))
    cookies.map.keySet.map(_.path).foreach(_ shouldBe client.defaultCookiePath)
  }

  it should "respect cookie Max-Age and Expires (RFC 6265 §5.3)" in {
    val cookies = new client.CookieMap
    val base = Uri("http://example.com/")
    val uri = Uri("http://example.com/")

    // Max-Age=0 deletes / does not store
    cookies.setCookiesFromHeaders(
      iSeq(`Set-Cookie`(HttpCookie("gone", "x", maxAge = Some(0L), path = Some("/")))),
      base,
    )
    cookies.map.keys.map(_.name) should not contain "gone"

    // Past Expires does not store
    val past = DateTime(System.currentTimeMillis() - 1 * 60 * 1000L)
    cookies.setCookiesFromHeaders(
      iSeq(`Set-Cookie`(HttpCookie("past", "x", expires = Some(past), path = Some("/")))),
      base,
    )
    cookies.map.keys.map(_.name) should not contain "past"

    // Future Expires is stored and sent
    val future = DateTime(System.currentTimeMillis() + 60 * 60 * 1000L)
    cookies.setCookiesFromHeaders(
      iSeq(`Set-Cookie`(HttpCookie("live", "1", expires = Some(future), path = Some("/")))),
      base,
    )
    cookies.getCookies(uri).flatMap(_.cookies.map(_.name)) should contain ("live")

    // Max-Age takes precedence over Expires: Max-Age=0 wins over future Expires
    cookies.setCookiesFromHeaders(
      iSeq(`Set-Cookie`(HttpCookie("live", "2", maxAge = Some(0L), expires = Some(future), path = Some("/")))),
      base,
    )
    cookies.map.keys.map(_.name) should not contain "live"

    // Max-Age takes precedence: positive Max-Age keeps cookie even if Expires is in the past
    cookies.setCookiesFromHeaders(
      iSeq(`Set-Cookie`(HttpCookie("maxage", "1", maxAge = Some(3600L), expires = Some(past), path = Some("/")))),
      base,
    )
    cookies.getCookies(uri).flatMap(_.cookies.map(_.name)) should contain ("maxage")

    // Max-Age relative expiry: short-lived cookie is evicted after it expires
    cookies.setCookiesFromHeaders(
      iSeq(`Set-Cookie`(HttpCookie("brief", "1", maxAge = Some(1L), path = Some("/")))),
      base,
    )
    cookies.getCookies(uri).flatMap(_.cookies.map(_.name)) should contain ("brief")
    Thread.sleep(1100)
    cookies.getCookies(uri).flatMap(_.cookies.map(_.name)) should not contain "brief"
    cookies.map.keys.map(_.name) should not contain "brief"

    // Session cookie (no Max-Age / Expires) remains
    cookies.setCookiesFromHeaders(
      iSeq(`Set-Cookie`(HttpCookie("session", "1", path = Some("/")))),
      base,
    )
    cookies.getCookies(uri).flatMap(_.cookies.map(_.name)) should contain ("session")
  }

  it should "compute cookie expiry with Max-Age precedence over Expires" in {
    val now = 1000 * 1000 * 1000 * 1000L
    val past = DateTime(now - 10 * 1000L)
    val future = DateTime(now + 10 * 1000L)
    RestClient.cookieExpiryMillis(HttpCookie("a", "1", maxAge = Some(30L)), now) shouldBe Some(now + 30 * 1000L)
    RestClient.cookieExpiryMillis(HttpCookie("a", "1", expires = Some(future)), now) shouldBe Some(future.clicks)
    RestClient.cookieExpiryMillis(
      HttpCookie("a", "1", maxAge = Some(5L), expires = Some(past)), now
    ) shouldBe Some(now + 5 * 1000L)
    RestClient.cookieExpiryMillis(HttpCookie("a", "1"), now) shouldBe None
    RestClient.isCookieExpired(Some(now), now) shouldBe true
    RestClient.isCookieExpired(Some(now + 1), now) shouldBe false
    RestClient.isCookieExpired(None, now) shouldBe false
  }

  it should "send Secure cookies only over https or wss" in {
    val cookies = new client.CookieMap
    val httpsUri = Uri("https://example.com/app")
    cookies.setCookiesFromHeaders(
      iSeq(`Set-Cookie`(HttpCookie("sid", "secret", secure = true, path = Some("/")))),
      httpsUri,
    )
    cookies.getCookies(Uri("https://example.com/app")).flatMap(_.cookies.map(_.name)) should contain ("sid")
    cookies.getCookies(Uri("wss://example.com/app")).flatMap(_.cookies.map(_.name)) should contain ("sid")
    cookies.getCookies(Uri("http://example.com/app")) shouldBe empty
    cookies.getCookies(Uri("ws://example.com/app")) shouldBe empty
  }

  it should "still send non-Secure cookies over http" in {
    val cookies = new client.CookieMap
    cookies.setCookiesFromHeaders(
      iSeq(`Set-Cookie`(HttpCookie("sid", "plain", secure = false, path = Some("/")))),
      Uri("http://example.com/"),
    )
    cookies.getCookies(Uri("http://example.com/")).flatMap(_.cookies.map(_.name)) should contain ("sid")
    cookies.getCookies(Uri("https://example.com/")).flatMap(_.cookies.map(_.name)) should contain ("sid")
  }

  it should "accept Domain that the request-host domain-matches (RFC 6265 §5.3)" in {
    val cookies = new client.CookieMap
    val from = Uri("http://www.example.com/app")
    cookies.setCookiesFromHeaders(
      iSeq(
        `Set-Cookie`(HttpCookie("a", "1", domain = Some("example.com"), path = Some("/"))),
        `Set-Cookie`(HttpCookie("b", "2", domain = Some(".example.com"), path = Some("/"))),
        `Set-Cookie`(HttpCookie("c", "3", domain = Some("www.example.com"), path = Some("/"))),
      ),
      from,
    )
    cookies.map.keySet.map(_.name) should contain allOf ("a", "b", "c")
    cookies.map.values.flatMap(_.domain).toSet shouldBe Set("example.com", "www.example.com")
    // Domain cookie is sent to host and subdomains
    cookies.getCookies(Uri("http://example.com/")).flatMap(_.cookies.map(_.name)).toSet should contain ("a")
    cookies.getCookies(Uri("http://www.example.com/")).flatMap(_.cookies.map(_.name)).toSet should
      contain allOf ("a", "b", "c")
    cookies.getCookies(Uri("http://api.example.com/")).flatMap(_.cookies.map(_.name)).toSet should
      contain allOf ("a", "b")
    cookies.getCookies(Uri("http://api.example.com/")).flatMap(_.cookies.map(_.name)) should not contain "c"
  }

  it should "reject Domain that the request-host does not domain-match" in {
    val cookies = new client.CookieMap
    val from = Uri("http://www.example.com/")
    cookies.setCookiesFromHeaders(
      iSeq(
        `Set-Cookie`(HttpCookie("evil", "x", domain = Some("evil.com"), path = Some("/"))),
        `Set-Cookie`(HttpCookie("sibling", "y", domain = Some("other.example.com"), path = Some("/"))),
        `Set-Cookie`(HttpCookie("deeper", "z", domain = Some("baz.www.example.com"), path = Some("/"))),
        `Set-Cookie`(HttpCookie("ok", "1", domain = Some("example.com"), path = Some("/"))),
      ),
      from,
    )
    cookies.map.keys.map(_.name).toSet shouldBe Set("ok")
  }

  it should "not treat host-only cookies as domain cookies for subdomains" in {
    val cookies = new client.CookieMap
    cookies.setCookiesFromHeaders(
      iSeq(`Set-Cookie`(HttpCookie("sid", "host-only", path = Some("/")))),
      Uri("http://example.com/"),
    )
    cookies.getCookies(Uri("http://example.com/x")).flatMap(_.cookies.map(_.name)) should contain ("sid")
    cookies.getCookies(Uri("http://www.example.com/x")) shouldBe empty
  }

  it should "domain-match only exact hosts for IP addresses (RFC 6265 §5.1.3)" in {
    RestClient.cookieDomainMatches("192.0.2.1", "192.0.2.1") shouldBe true
    RestClient.cookieDomainMatches("192.0.2.1", "2.1") shouldBe false
    RestClient.cookieDomainMatches("2001:db8::1", "2001:db8::1") shouldBe true
    RestClient.cookieDomainMatches("2001:db8::1", "db8::1") shouldBe false

    val cookies = new client.CookieMap
    cookies.setCookiesFromHeaders(
      iSeq(`Set-Cookie`(HttpCookie("sid", "ip", domain = Some("192.0.2.1"), path = Some("/")))),
      Uri("http://192.0.2.1/"),
    )
    cookies.map.keys.map(_.name) should contain ("sid")
    // Domain=partial suffix must not be accepted from an IP request-host
    cookies.setCookiesFromHeaders(
      iSeq(`Set-Cookie`(HttpCookie("bad", "x", domain = Some("0.2.1"), path = Some("/")))),
      Uri("http://192.0.2.1/"),
    )
    cookies.map.keys.map(_.name) should not contain "bad"
  }

  it should "not domain-match when only a suffix without a dot boundary" in {
    RestClient.cookieDomainMatches("notexample.com", "example.com") shouldBe false
    RestClient.cookieDomainMatches("www.example.com", "example.com") shouldBe true
    RestClient.cookieDomainMatches("example.com", "example.com") shouldBe true
  }

  it should "use defaultCookieHost and defaultCookiePath for setCookies when domain/path omitted" in {
    val cookies = new client.CookieMap
    cookies.setCookies(Map("lang" -> "en"))
    cookies.map.keySet should contain (
      RestClient.CookieKey("lang", client.defaultCookieHost, client.defaultCookiePath)
    )
    cookies.map.values.forall(_.domain.isEmpty) shouldBe true
    cookies.getCookies(Uri(client.serverPath)).flatMap(_.cookies.map(_.name)) should contain ("lang")
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
        cookies.map
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
    jarClient.getCookieStorage.map.keys.map(_.name) should contain ("sid")
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
