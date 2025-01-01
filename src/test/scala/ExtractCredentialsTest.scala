package org.wabase

import org.apache.pekko.http.scaladsl.model.FormData
import org.apache.pekko.http.scaladsl.model.headers.{Authorization => AuthorizationHeader, _}
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.testkit.ScalatestRouteTest
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.{AnyWordSpec => WordSpec}

import org.apache.pekko.http.scaladsl.model._
import java.net.InetAddress

class ExtractCredentialsTest extends WordSpec with Matchers with ScalatestRouteTest {
  val service = new TestAppService(system)
  "The extractCredentials directive" should {

    val route = service.extractCredentials { cred => complete (cred match {
      case Some(BasicHttpCredentials(user, _)) => s"Basic auth: $user"
      case Some(x) => "Some credentials"
      case None => Some("No credentials")
    })}

    "return basic auth header username" in {
      Get() ~> AuthorizationHeader(BasicHttpCredentials("john", "j0hn")) ~> route ~> check {
        responseAs[String] shouldEqual "Basic auth: john"
      }
    }

    "return no credentials" in {
      // tests:
      Get("/", FormData("user" -> "john",
        "password" -> "j0hn")) ~> route ~> check {
        responseAs[String] shouldEqual "No credentials"
      }
    }
  }
}
