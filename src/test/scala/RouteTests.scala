package org.wabase

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{HttpRequest, StatusCodes}
import akka.http.scaladsl.model.headers.HttpOrigin
import akka.http.scaladsl.testkit.ScalatestRouteTest
import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.scalatest.matchers.should.Matchers
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route

class RouteTests extends FlatSpec with Matchers with ScalatestRouteTest{
  class RouteTestsAppService(system: ActorSystem) extends TestAppService(system) {
    override protected def logCsrfRejection(
        request: HttpRequest, sourceOrigins: Seq[HttpOrigin], targetOrigins: Seq[HttpOrigin]) =
      logger.debug(csrfRejectionMessage(request, sourceOrigins, targetOrigins))
  }
  val service = new RouteTestsAppService(system)

  behavior of "Core routes"

  it should "handle get by id" in {
    val route = (service.crudPath & service.getByIdPath) { (path, id) =>
      complete("OK")
    }

    Get("/") ~> route ~> check(handled shouldBe false)
    Get("/r") ~> route ~> check(handled shouldBe false)
    Get("/data") ~> route ~> check(handled shouldBe false)
    Get("/data/") ~> route ~> check(handled shouldBe false)
    Get("/data/view") ~> route ~> check(handled shouldBe false)
    Get("/data/view/") ~> route ~> check(handled shouldBe false)
    Get("/data/view/a") ~> route ~> check(handled shouldBe false)

    Post("/data/view/1") ~> route ~> check(handled shouldBe false)
    Put("/data/view/1") ~> route ~> check(handled shouldBe false)
    Delete("/data/view/1") ~> route ~> check(handled shouldBe false)

    Get("/data/view/1") ~> route ~> check(handled shouldBe true)
  }

  it should "handle delete" in {
    val route = (service.crudPath & service.deletePath) { (path, id) =>
      complete("OK")
    }

    Delete("/") ~> route ~> check(handled shouldBe false)
    Delete("/r") ~> route ~> check(handled shouldBe false)
    Delete("/data") ~> route ~> check(handled shouldBe false)
    Delete("/data/") ~> route ~> check(handled shouldBe false)
    Delete("/data/view") ~> route ~> check(handled shouldBe false)
    Delete("/data/view/") ~> route ~> check(handled shouldBe false)
    Delete("/data/view/a") ~> route ~> check(handled shouldBe false)

    Get("/data/view/1") ~> route ~> check(handled shouldBe false)
    Post("/data/view/1") ~> route ~> check(handled shouldBe false)
    Put("/data/view/1") ~> route ~> check(handled shouldBe false)

    Delete("/data/view/1") ~> route ~> check(handled shouldBe true)
  }

  it should "handle update" in {
    val route = (service.crudPath & service.updatePath) { (path, id) =>
      complete("OK")
    }

    Put("/") ~> route ~> check(handled shouldBe false)
    Put("/r") ~> route ~> check(handled shouldBe false)
    Put("/data") ~> route ~> check(handled shouldBe false)
    Put("/data/") ~> route ~> check(handled shouldBe false)
    Put("/data/view") ~> route ~> check(handled shouldBe false)
    Put("/data/view/") ~> route ~> check(handled shouldBe false)
    Put("/data/view/a") ~> route ~> check(handled shouldBe false)

    Get("/data/view/1") ~> route ~> check(handled shouldBe false)
    Post("/data/view/1") ~> route ~> check(handled shouldBe false)
    Delete("/data/view/1") ~> route ~> check(handled shouldBe false)

    Put("/data/view/1") ~> route ~> check(handled shouldBe true)
  }


  it should "handle insert" in {
    val route = (service.crudPath & service.insertPath) { path =>
      complete("OK")
    }


    Post("/") ~> route ~> check(handled shouldBe false)
    Post("/r") ~> route ~> check(handled shouldBe false)
    Post("/data") ~> route ~> check(handled shouldBe false)
    Post("/data/") ~> route ~> check(handled shouldBe false)
    Post("/data/view/a") ~> route ~> check(handled shouldBe false)
    Post("/data/view/1") ~> route ~> check(handled shouldBe false)

    Get("/data/view") ~> route ~> check(handled shouldBe false)
    Put("/data/view") ~> route ~> check(handled shouldBe false)
    Delete("/data/view") ~> route ~> check(handled shouldBe false)

    Post("/data/view") ~> route ~> check(handled shouldBe true)
    Post("/data/view/") ~> route ~> check(handled shouldBe true)

  }

  it should "handle list" in {
    val route = (service.crudPath & service.listOrGetPath) { path =>
        complete("OK")
    }

    Get("/") ~> route ~> check(handled shouldBe false)
    Get("/r") ~> route ~> check(handled shouldBe false)
    Get("/data") ~> route ~> check(handled shouldBe false)
    Get("/data/") ~> route ~> check(handled shouldBe false)
    Get("/data/view/a") ~> route ~> check(handled shouldBe false)
    Get("/data/view/1") ~> route ~> check(handled shouldBe false)

    Post("/data/view") ~> route ~> check(handled shouldBe false)
    Put("/data/view") ~> route ~> check(handled shouldBe false)
    Delete("/data/view") ~> route ~> check(handled shouldBe false)

    Get("/data/view") ~> route ~> check(handled shouldBe true)
    Get("/data/view/") ~> route ~> check(handled shouldBe true)

  }

  it should "fail properly" in {
    implicit val exceptionHandler = service.appExceptionHandler
    implicit val user: TestUsr = null
    val route = Route.seal((service.crudPath) {
      service.crudAction
    })
    Post("/data/view", "{bad json}") ~> route ~> check(status shouldEqual StatusCodes.UnprocessableEntity)
  }

  it should "pass csrf defence" in {
    //sameorigin check
    val route = service.checkSameOrigin {
      complete("OK")
    }
    import akka.http.scaladsl.model.headers.{Host, Referer, Origin, RawHeader, HttpOrigin, Cookie}
    import akka.http.scaladsl.server.InvalidOriginRejection
    import akka.http.scaladsl.model.Uri

    Get("/") ~> Origin(List(HttpOrigin("http", Host("localhost", 80)))) ~> route ~> check (handled shouldBe false)
    Get("/") ~> Host("localhost", 80) ~> route ~> check (handled shouldBe false)
    Get("/") ~> route ~> check (handled shouldBe false)

    Get("/") ~> Origin(List(HttpOrigin("http", Host("localhost", 80)))) ~>
      Host("localhost", 80) ~> route ~> check (handled shouldBe true)
    Get("/") ~> Origin(List(HttpOrigin("http", Host("localhost", 80)))) ~>
      Host("localhost", 90) ~> route ~> check (handled shouldBe false)

    Get("/") ~> Origin(List(HttpOrigin("http", Host("localhost", 80)))) ~>
      RawHeader("X-Forwarded-Host", "localhost:80") ~> route ~> check (handled shouldBe true)
    Get("/") ~> Origin(List(HttpOrigin("http", Host("localhost", 80)))) ~>
      RawHeader("X-Forwarded-Host", "localhost") ~> route ~> check (handled shouldBe true)
    Get("/") ~> Origin(List(HttpOrigin("http", Host("localhost", 0)))) ~>
      RawHeader("X-Forwarded-Host", "localhost") ~> route ~> check (handled shouldBe true)
    Get("/") ~> Origin(List(HttpOrigin("http", Host("localhost", 80)))) ~>
      RawHeader("X-Forwarded-Host", "localhost:90") ~> route ~> check (handled shouldBe false)

    Get("/") ~> Referer(Uri("https://localhost")) ~> route ~> check (handled shouldBe false)

    Get("/") ~> Referer(Uri("https://localhost")) ~>
      Host("localhost", 0) ~> route ~> check (handled shouldBe true)
    Get("/") ~> Referer(Uri("https://localhost")) ~>
      Host("localhost", 443) ~> route ~> check (handled shouldBe true)
    Get("/") ~> Referer(Uri("https://localhost")) ~>
      Host("localhost", 444) ~> route ~> check (handled shouldBe false)

    Get("/") ~> Referer(Uri("https://localhost:443")) ~>
      Host("localhost", 0) ~> route ~> check (handled shouldBe true)
    Get("/") ~> Referer(Uri("https://localhost:443")) ~>
      Host("localhost", 443) ~> route ~> check (handled shouldBe true)
    Get("/") ~> Referer(Uri("https://localhost:443")) ~>
      Host("localhost", 444) ~> route ~> check (handled shouldBe false)

    Get("/") ~> Referer(Uri("https://localhost:444")) ~>
      Host("localhost", 0) ~> route ~> check (handled shouldBe false)
    Get("/") ~> Referer(Uri("https://localhost:444")) ~>
      Host("localhost", 443) ~> route ~> check (handled shouldBe false)
    Get("/") ~> Referer(Uri("https://localhost:444")) ~>
      Host("localhost", 444) ~> route ~> check (handled shouldBe true)
    Get("/") ~> Referer(Uri("https://localhost:444")) ~>
      Host("localhost", 440) ~> route ~> check (handled shouldBe false)

    Get("/") ~> Referer(Uri("http://localhost")) ~>
      Host("localhost", 0) ~> route ~> check (handled shouldBe true)
    Get("/") ~> Referer(Uri("http://localhost")) ~>
      Host("localhost", 80) ~> route ~> check (handled shouldBe true)
    Get("/") ~> Referer(Uri("http://localhost")) ~>
      Host("localhost", 88) ~> route ~> check (handled shouldBe false)

    Get("/") ~> Referer(Uri("http://localhost:80")) ~>
      Host("localhost", 0) ~> route ~> check (handled shouldBe true)
    Get("/") ~> Referer(Uri("http://localhost:80")) ~>
      Host("localhost", 80) ~> route ~> check (handled shouldBe true)
    Get("/") ~> Referer(Uri("http://localhost:80")) ~>
      Host("localhost", 88) ~> route ~> check (handled shouldBe false)

    Get("/") ~> Referer(Uri("http://localhost:88")) ~>
      Host("localhost", 0) ~> route ~> check (handled shouldBe false)
    Get("/") ~> Referer(Uri("http://localhost:88")) ~>
      Host("localhost", 80) ~> route ~> check (handled shouldBe false)
    Get("/") ~> Referer(Uri("http://localhost:88")) ~>
      Host("localhost", 88) ~> route ~> check (handled shouldBe true)
    Get("/") ~> Referer(Uri("http://localhost:88")) ~>
      Host("localhost", 90) ~> route ~> check (handled shouldBe false)

    Get("/") ~> Referer(Uri("https://localhost")) ~>
      RawHeader("X-Forwarded-Host", "localhost") ~> route ~> check (handled shouldBe true)
    Get("/") ~> Referer(Uri("https://localhost:90")) ~>
      RawHeader("X-Forwarded-Host", "localhost:90") ~> route ~> check (handled shouldBe true)
    Get("/") ~> Referer(Uri("https://localhost:90")) ~>
      RawHeader("X-Forwarded-Host", "localhost:91") ~> route ~> check (handled shouldBe false)

    //route with provided appConfig
    val route1 = (new RouteTestsAppService(system) {
      override lazy val appConfig = com.typesafe.config.ConfigFactory.parseString("""{"host": "http://localhost"}""")
    }).checkSameOrigin { complete("OK") }
    Get("/") ~> Origin(List(HttpOrigin("http", Host("localhost", 0))))  ~>
      route1 ~> check (handled shouldBe true)
    Get("/") ~> Origin(List(HttpOrigin("http", Host("localhost", 80))))  ~>
      route1 ~> check (handled shouldBe true)

    Get("/") ~> Referer(Uri("http://localhost"))  ~>
      route1 ~> check (handled shouldBe true)
    Get("/") ~> Referer(Uri("http://localhost:80"))  ~>
      route1 ~> check (handled shouldBe true)
    Get("/") ~> Referer(Uri("xxx://localhost"))  ~>
      route1 ~> check (handled shouldBe false)

    //check token
    val route2 = service.checkCSRFToken {
      complete("OK")
    }
    Get("/") ~> Cookie(service.CSRFCookieName -> "abc") ~> RawHeader(service.CSRFHeaderName, "abc") ~>
      route2 ~> check(handled shouldBe true)
    Get("/") ~> Cookie(service.CSRFCookieName -> "abc") ~> RawHeader(service.CSRFHeaderName, "123") ~>
      route2 ~> check(handled shouldBe false)
    Get("/") ~> Cookie(service.CSRFCookieName -> "abc") ~>
      route2 ~> check(handled shouldBe false)
    Get("/") ~> RawHeader(service.CSRFHeaderName, "123") ~>
      route2 ~> check(handled shouldBe false)
  }
}
