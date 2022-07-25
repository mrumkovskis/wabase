package org.wabase

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.{Location, `Content-Type`}
import akka.http.scaladsl.model.{ContentTypes, StatusCodes}
import akka.http.scaladsl.server.{RejectionHandler, Route}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.tresql.{DMLResult, Query, ThreadLocalResources, dialects}

class CrudTestService(system: ActorSystem, testApp: TestApp) extends TestAppService(system) {
  override def initApp          = testApp
  override def initFileStreamer = testApp
  implicit val user: TestUsr    = null
  val route =
    crudPath {
      crudAction
    }
}

class CrudServiceSpecs extends AnyFlatSpec with Matchers with TestQuereaseInitializer with ScalatestRouteTest {
  var dbAccess: DbAccess        = _
  var service:  CrudTestService = _
  var route:    Route           = _

  override def dbNamePrefix: String = "main"
  override def beforeAll(): Unit = {
    querease    = new TestQuerease("/crud-service-specs-metadata.yaml")
    super.beforeAll()
    dbAccess    = new DbAccess with Loggable {
      override implicit lazy val tresqlResources: ThreadLocalResources = new TresqlResources {
        override lazy val resourcesTemplate = super.resourcesTemplate.copy(
          dialect   = dialects.HSQLDialect,
          idExpr    = s => "nextval('seq')",
          metadata  = querease.tresqlMetadata,
        )
      }
    }
    val testApp = new TestApp with NoValidation {
      override protected def initQuerease: QE = querease
      override def dbAccessDelegate = CrudServiceSpecs.this.dbAccess
    }
    service     = new CrudTestService(system, testApp)
    route       = service.route
  }
  //----------------------------------------------------------//

  def createPerson(name: String): Long = {
    val db = dbAccess
    import db._
    transaction {
      Query(s"+person {id, name} [#person, '$name']") match {
        case r: DMLResult => r.id.get.toString.toLong
        case _ => -1
      }
    }
  }
  def hasPerson(name: String): Boolean = {
    val db = dbAccess
    import db._
    dbUse(Query(s"person[name = '$name'] {count(*)}").unique[Int]) == 1
  }
  //----------------------------------------------------------//

  it should "get by id" in {
    Get("/data/by_id_view_1/0") ~> route ~> check {
      status shouldEqual StatusCodes.NotFound
    }
    val id = createPerson("John")
    Get(s"/data/by_id_view_1/$id") ~> route ~> check {
      status shouldEqual StatusCodes.OK
      header[`Content-Type`].get.contentType shouldBe ContentTypes.`application/json`
      entityAs[String] shouldBe s"""{"id":$id,"name":"John","surname":null}"""
    }
  }

  it should "list" in {
    Get("/data/by_id_view_1") ~> route ~> check {
      status shouldEqual StatusCodes.OK
      header[`Content-Type`].get.contentType shouldBe ContentTypes.`application/json`
    }
    Get("/data/by_id_view_1?name=M") ~> route ~> check {
      status shouldEqual StatusCodes.OK
      header[`Content-Type`].get.contentType shouldBe ContentTypes.`application/json`
      entityAs[String] shouldBe s"""[]"""
    }
    val id1 = createPerson("Martin")
    Get("/data/by_id_view_1?name=M") ~> route ~> check {
      status shouldEqual StatusCodes.OK
      header[`Content-Type`].get.contentType shouldBe ContentTypes.`application/json`
      entityAs[String] shouldBe s"""[{"id":$id1,"name":"Martin","surname":null}]"""
    }
    val id2 = createPerson("Michael")
    Get("/data/by_id_view_1?name=M") ~> route ~> check {
      status shouldEqual StatusCodes.OK
      header[`Content-Type`].get.contentType shouldBe ContentTypes.`application/json`
      entityAs[String] shouldBe List(
        s"""{"id":$id1,"name":"Martin","surname":null}""",
        s"""{"id":$id2,"name":"Michael","surname":null}""",
      ).mkString("[", ",", "]")
    }
  }

  it should "count" in {
    Get("/data/count/by_id_view_1") ~> route ~> check {
      status shouldEqual StatusCodes.OK
      header[`Content-Type`].get.contentType shouldBe ContentTypes.`text/plain(UTF-8)`
    }
    Get("/data/count/by_id_view_1?name=A") ~> route ~> check {
      status shouldEqual StatusCodes.OK
      header[`Content-Type`].get.contentType shouldBe ContentTypes.`text/plain(UTF-8)`
      entityAs[String].toInt shouldBe 0
    }
    val id1 = createPerson("Anna")
    Get("/data/count/by_id_view_1?name=A") ~> route ~> check {
      status shouldEqual StatusCodes.OK
      header[`Content-Type`].get.contentType shouldBe ContentTypes.`text/plain(UTF-8)`
      entityAs[String].toInt shouldBe 1
    }
  }

  it should "create" in {
    Get("/data/create/by_id_view_1") ~> route ~> check {
      status shouldEqual StatusCodes.OK
      header[`Content-Type`].get.contentType shouldBe ContentTypes.`application/json`
      entityAs[String] shouldBe """{"id":null,"name":null,"surname":null}"""
    }
  }

  it should "insert" in {
    hasPerson("Sia") shouldBe false
    Post("/data/by_id_view_1", """{"name": "Sia"}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.path.toString
      location should startWith ("/data/by_id_view_1/")
      (location.substring(location.lastIndexOf("/") + 1).toLong > 0) shouldBe true
    }
    hasPerson("Sia") shouldBe true
  }

  it should "update by id" in {
    hasPerson("Peter") shouldBe false
    hasPerson("Pete")  shouldBe false
    val id = createPerson("Peter")
    hasPerson("Peter") shouldBe true
    hasPerson("Pete")  shouldBe false
    Put(s"/data/by_id_view_1/$id", s"""{"id": $id,"name": "Pete"}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.path.toString
      location should startWith ("/data/by_id_view_1/")
      location.substring(location.lastIndexOf("/") + 1).toLong shouldBe id
    }
    hasPerson("Peter") shouldBe false
    hasPerson("Pete")  shouldBe true
  }

  it should "delete by id" in {
    hasPerson("Scott") shouldBe false
    val id = createPerson("Scott")
    hasPerson("Scott") shouldBe true
    Delete(s"/data/by_id_view_1/$id") ~> route ~> check {
      status shouldEqual StatusCodes.OK
    }
    hasPerson("Scott") shouldBe false
  }
}
