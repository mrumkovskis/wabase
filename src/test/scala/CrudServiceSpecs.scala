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

  def createPerson(name: String, surname: String = null): Long = {
    val db = dbAccess
    import db._
    val values = Seq(name, surname)
      .map { case null => "null" case x => s"'$x'" }
      .mkString(", ")
    transaction {
      Query(s"+person {id, name, surname} [#person, $values]") match {
        case r: DMLResult => r.id.get.toString.toLong
        case _ => -1
      }
    }
  }
  def deletePerson(name: String): Unit = {
    val db = dbAccess
    import db._
    transaction { Query(s"-person[name = '$name']") }
  }
  def hasPerson(name: String): Boolean = {
    val db = dbAccess
    import db._
    dbUse(Query(s"person[name = '$name'] {count(*)}").unique[Int]) == 1
  }
  def hasPerson(name: String, surname: String = null): Boolean = {
    val db = dbAccess
    import db._
    dbUse(Query(s"person[name = '$name' & surname = '$surname'] {count(*)}").unique[Int]) == 1
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

  it should "get by key" in {
    Get(s"/data/by_key_view_1/Jar/Key") ~> route ~> check {
      status shouldEqual StatusCodes.NotFound
    }
    val id = createPerson("Jar", "Key")
    Get(s"/data/by_key_view_1/Jar/Key") ~> route ~> check {
      status shouldEqual StatusCodes.OK
      header[`Content-Type`].get.contentType shouldBe ContentTypes.`application/json`
      entityAs[String] shouldBe s"""{"name":"Jar","surname":"Key"}"""
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
    Get("/data/create/by_key_view_1") ~> route ~> check {
      status shouldEqual StatusCodes.OK
      header[`Content-Type`].get.contentType shouldBe ContentTypes.`application/json`
      entityAs[String] shouldBe """{"name":null,"surname":null}"""
    }
  }

  it should "insert by id" in {
    hasPerson("Sia") shouldBe false
    Post("/data/by_id_view_1", """{"name": "Sia"}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.path.toString
      location should startWith ("/data/by_id_view_1/")
      (location.substring(location.lastIndexOf("/") + 1).toLong > 0) shouldBe true
    }
    hasPerson("Sia") shouldBe true
  }

  it should "insert by key" in {
    hasPerson("Jane") shouldBe false
    Post("/data/by_key_view_1", """{"name": "Jane"}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.path.toString
      location shouldBe "/data/by_key_view_1/Jane/null"
    }
    hasPerson("Jane") shouldBe true
    hasPerson("Bruce") shouldBe false
    Post("/data/by_key_view_1", """{"name": "Bruce", "surname": "Fur"}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.path.toString
      location shouldBe "/data/by_key_view_1/Bruce/Fur"
    }
    hasPerson("Bruce") shouldBe true
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

  it should "update by key" in {
    hasPerson("KeyName") shouldBe false
    val id = createPerson("KeyName", "OldSurname")
    hasPerson("KeyName", "OldSurname") shouldBe true
    Put(s"/data/by_key_view_2/KeyName", s"""{"name": "KeyName", "surname": "NewSurname"}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.path.toString
      location shouldBe "/data/by_key_view_2/KeyName"
    }
    hasPerson("KeyName", "OldSurname") shouldBe false
    hasPerson("KeyName", "NewSurname") shouldBe true
  }

  it should "update key" in {
    hasPerson("Winnie") shouldBe false
    hasPerson("Bear")   shouldBe false
    val id = createPerson("Winnie", "Pooh")
    hasPerson("Winnie") shouldBe true
    hasPerson("Bear")   shouldBe false
    Put(s"/data/by_key_view_1/Winnie/Pooh", s"""{"name": "Bear", "surname": "Pooh"}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.path.toString
      location shouldBe "/data/by_key_view_1/Bear/Pooh"
    }
    hasPerson("Winnie") shouldBe false
    hasPerson("Bear")   shouldBe true
  }

  it should "manage key magically" in {
    hasPerson("MagicIns") shouldBe false
    hasPerson("MagicUpd") shouldBe false
    hasPerson("NotMagic") shouldBe false
    Post("/data/by_magic_key_view_1", """{"name": "NotMagic"}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.path.toString
      location shouldBe "/data/by_magic_key_view_1/MagicIns"
    }
    hasPerson("MagicIns") shouldBe true
    hasPerson("MagicUpd") shouldBe false
    hasPerson("NotMagic") shouldBe false
    Put(s"/data/by_magic_key_view_1/MagicIns", s"""{"name": "NotMagic"}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.path.toString
      location shouldBe "/data/by_magic_key_view_1/MagicUpd"
    }
    hasPerson("MagicIns") shouldBe false
    hasPerson("MagicUpd") shouldBe true
    hasPerson("NotMagic") shouldBe false
    deletePerson("MagicUpd")
    //
    hasPerson("MagicIns") shouldBe false
    hasPerson("MagicUpd") shouldBe false
    hasPerson("NotMagic") shouldBe false
    Post("/data/by_magic_key_view_2", """{"name": "NotMagic"}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.path.toString
      location shouldBe "/data/by_magic_key_view_2/MagicIns"
    }
    hasPerson("MagicIns") shouldBe true
    hasPerson("MagicUpd") shouldBe false
    hasPerson("NotMagic") shouldBe false
    Put(s"/data/by_magic_key_view_2/MagicIns", s"""{"name": "NotMagic"}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.path.toString
      location shouldBe "/data/by_magic_key_view_2/MagicUpd"
    }
    hasPerson("MagicIns") shouldBe false
    hasPerson("MagicUpd") shouldBe true
    hasPerson("NotMagic") shouldBe false
  }

  it should "not update readonly key" in {
    hasPerson("RoName") shouldBe false
    hasPerson("RwName") shouldBe false
    val id = createPerson("RoName")
    hasPerson("RoName") shouldBe true
    hasPerson("RwName") shouldBe false
    Put(s"/data/by_readonly_key_view_1/RoName", s"""{"name": "RwName", "surname": null}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.path.toString
      location shouldBe "/data/by_readonly_key_view_1/RoName"
    }
    hasPerson("RoName") shouldBe true
    hasPerson("RwName") shouldBe false
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

  it should "delete by key" in {
    hasPerson("Deleme") shouldBe false
    createPerson("Deleme")
    hasPerson("Deleme") shouldBe true
    Delete(s"/data/by_key_view_2/Deleme") ~> route ~> check {
      status shouldEqual StatusCodes.OK
    }
    hasPerson("Deleme") shouldBe false
  }
}
