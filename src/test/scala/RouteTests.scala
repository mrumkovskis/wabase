package org.wabase

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.scalatest.matchers.should.Matchers
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import org.scalatest.Inspectors.forAll

class RouteTests extends FlatSpec with Matchers with ScalatestRouteTest{
  val service = new TestAppService(system)

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
  it should "pass csrf defence" in {
    implicit val csrfExceptionHandler: ExceptionHandler = ExceptionHandler {
      case e: CSRFException => complete(StatusCodes.BadRequest, e.getMessage)
    }
    //sameorigin check with csrf exception handler
    val route = Route.seal(service.checkSameOrigin {
      complete("OK")
    })
    import akka.http.scaladsl.model.headers.{Host, Referer, Origin, RawHeader, HttpOrigin, Cookie}
    import akka.http.scaladsl.server.InvalidOriginRejection
    import akka.http.scaladsl.model.Uri

    def csrfErr(strings: Seq[String]) = {
      response.status shouldBe StatusCodes.BadRequest
      val resp = entityAs[String]
      resp should include("(url -")
      forAll(strings) { resp should include(_) }
    }

    def csrfOk = response.status shouldBe StatusCodes.OK

    Get("/") ~> Origin(List(HttpOrigin("http", Host("localhost", 80)))) ~> route ~> check {
      csrfErr(List("Host", "X-Forwarded-Host"))
    }
    Get("/") ~> Host("localhost", 80) ~> route ~> check (csrfErr(List("Origin", "Referer")))
    Get("/") ~> route ~> check (csrfErr(List("Host", "X-Forwarded-Host")))

    Get("/") ~> Origin(List(HttpOrigin("http", Host("localhost", 80)))) ~>
      Host("localhost", 80) ~> route ~> check (csrfOk)
    Get("/") ~> Origin(List(HttpOrigin("http", Host("localhost", 80)))) ~>
      Host("localhost", 90) ~> route ~> check (csrfErr(List("CSRF")))

    Get("/") ~> Origin(List(HttpOrigin("http", Host("localhost", 80)))) ~>
      RawHeader("X-Forwarded-Host", "localhost:80") ~> route ~> check (csrfOk)
    Get("/") ~> Origin(List(HttpOrigin("http", Host("localhost", 80)))) ~>
      RawHeader("X-Forwarded-Host", "localhost") ~> route ~> check (csrfOk)
    Get("/") ~> Origin(List(HttpOrigin("http", Host("localhost", 0)))) ~>
      RawHeader("X-Forwarded-Host", "localhost") ~> route ~> check (csrfOk)
    Get("/") ~> Origin(List(HttpOrigin("http", Host("localhost", 80)))) ~>
      RawHeader("X-Forwarded-Host", "localhost:90") ~> route ~> check (csrfErr(List("CSRF")))

    Get("/") ~> Referer(Uri("https://localhost")) ~> route ~> check (csrfErr(List("Host", "X-Forwarded-Host")))

    Get("/") ~> Referer(Uri("https://localhost")) ~>
      Host("localhost", 0) ~> route ~> check (csrfOk)
    Get("/") ~> Referer(Uri("https://localhost")) ~>
      Host("localhost", 443) ~> route ~> check (csrfOk)
    Get("/") ~> Referer(Uri("https://localhost")) ~>
      Host("localhost", 444) ~> route ~> check (csrfErr(List("CSRF")))

    Get("/") ~> Referer(Uri("https://localhost:443")) ~>
      Host("localhost", 0) ~> route ~> check (csrfOk)
    Get("/") ~> Referer(Uri("https://localhost:443")) ~>
      Host("localhost", 443) ~> route ~> check (csrfOk)
    Get("/") ~> Referer(Uri("https://localhost:443")) ~>
      Host("localhost", 444) ~> route ~> check (csrfErr(List("CSRF")))

    Get("/") ~> Referer(Uri("https://localhost:444")) ~>
      Host("localhost", 0) ~> route ~> check (csrfErr(List("CSRF")))
    Get("/") ~> Referer(Uri("https://localhost:444")) ~>
      Host("localhost", 443) ~> route ~> check (csrfErr(List("CSRF")))
    Get("/") ~> Referer(Uri("https://localhost:444")) ~>
      Host("localhost", 444) ~> route ~> check (csrfOk)
    Get("/") ~> Referer(Uri("https://localhost:444")) ~>
      Host("localhost", 440) ~> route ~> check (csrfErr(List("CSRF")))

    Get("/") ~> Referer(Uri("http://localhost")) ~>
      Host("localhost", 0) ~> route ~> check (csrfOk)
    Get("/") ~> Referer(Uri("http://localhost")) ~>
      Host("localhost", 80) ~> route ~> check (csrfOk)
    Get("/") ~> Referer(Uri("http://localhost")) ~>
      Host("localhost", 88) ~> route ~> check (csrfErr(List("CSRF")))

    Get("/") ~> Referer(Uri("http://localhost:80")) ~>
      Host("localhost", 0) ~> route ~> check (csrfOk)
    Get("/") ~> Referer(Uri("http://localhost:80")) ~>
      Host("localhost", 80) ~> route ~> check (csrfOk)
    Get("/") ~> Referer(Uri("http://localhost:80")) ~>
      Host("localhost", 88) ~> route ~> check (csrfErr(List("CSRF")))

    Get("/") ~> Referer(Uri("http://localhost:88")) ~>
      Host("localhost", 0) ~> route ~> check (csrfErr(List("CSRF")))
    Get("/") ~> Referer(Uri("http://localhost:88")) ~>
      Host("localhost", 80) ~> route ~> check (csrfErr(List("CSRF")))
    Get("/") ~> Referer(Uri("http://localhost:88")) ~>
      Host("localhost", 88) ~> route ~> check (csrfOk)
    Get("/") ~> Referer(Uri("http://localhost:88")) ~>
      Host("localhost", 90) ~> route ~> check (csrfErr(List("CSRF")))

    Get("/") ~> Referer(Uri("https://localhost")) ~>
      RawHeader("X-Forwarded-Host", "localhost") ~> route ~> check (csrfOk)
    Get("/") ~> Referer(Uri("https://localhost:90")) ~>
      RawHeader("X-Forwarded-Host", "localhost:90") ~> route ~> check (csrfOk)
    Get("/") ~> Referer(Uri("https://localhost:90")) ~>
      RawHeader("X-Forwarded-Host", "localhost:91") ~> route ~> check (csrfErr(List("CSRF")))

    //route with provided appConfig
    val route1 = Route.seal((new TestAppService(system) {
      override lazy val appConfig = com.typesafe.config.ConfigFactory.parseString("""{"host": "http://localhost"}""")
    }).checkSameOrigin { complete("OK") })
    Get("/") ~> Origin(List(HttpOrigin("http", Host("localhost", 0))))  ~>
      route1 ~> check (csrfOk)
    Get("/") ~> Origin(List(HttpOrigin("http", Host("localhost", 80))))  ~>
      route1 ~> check (csrfOk)

    Get("/") ~> Referer(Uri("http://localhost"))  ~>
      route1 ~> check (csrfOk)
    Get("/") ~> Referer(Uri("http://localhost:80"))  ~>
      route1 ~> check (csrfOk)
    Get("/") ~> Referer(Uri("xxx://localhost"))  ~>
      route1 ~> check (csrfErr(List("CSRF")))

    //check csrf token with set csrf exception handler
    val route2 = Route.seal(service.checkCSRFToken {
      complete("OK")
    })
    Get("/") ~> Cookie(service.CSRFCookieName -> "abc") ~> RawHeader(service.CSRFHeaderName, "abc") ~>
      route2 ~> check(csrfOk)
    Get("/") ~> Cookie(service.CSRFCookieName -> "abc") ~> RawHeader(service.CSRFHeaderName, "123") ~>
      route2 ~> check(csrfErr(List("does not match")))
    Get("/") ~> Cookie(service.CSRFCookieName -> "abc") ~>
      route2 ~> check(csrfErr(List("X-XSRF-TOKEN header")))
    Get("/") ~> RawHeader(service.CSRFHeaderName, "123") ~>
      route2 ~> check(csrfErr(List("XSRF-TOKEN cookie")))
  }
}
