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
  implicit val exceptionHandler = appExceptionHandler
  implicit val rejectionHandler = RejectionHandler.default
  val route =
    Route.seal {
      crudPath {
        crudAction
      }
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
    // do not redirect to helper view
    Put(s"/data/by_key_view_3/KeyName", s"""{"name": "KeyName", "surname": "NewSurname"}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.path.toString
      location shouldBe "/data/by_key_view_3/KeyName"
    }
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

  it should "redirect by key explicitly" in {
    hasPerson("RediName") shouldBe false
    // on insert redirect to this explicitly
    Post("/data/by_key_redirect_view_1", """{"name": "RediName"}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.path.toString
      location shouldBe "/data/by_key_redirect_view_1/RediName"
    }
    // on update redirect to this explicitly
    Put(s"/data/by_key_redirect_view_1/RediName", s"""{"name": "RediNameUpd"}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.path.toString
      location shouldBe "/data/by_key_redirect_view_1/RediNameUpd"
    }
    hasPerson("RediOther") shouldBe false
    // on insert redirect to another view explicitly
    Post("/data/by_key_redirect_view_2", """{"name": "RediOther", "surname": "Surnm"}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.path.toString
      location shouldBe "/data/by_key_view_1/RediOther/Surnm"
    }
    // on update redirect to another view explicitly
    Put(s"/data/by_key_redirect_view_2/RediOther",
        s"""{"name": "RediOther", "surname": "SurnmUpd"}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.path.toString
      location shouldBe "/data/by_key_view_1/RediOther/SurnmUpd"
    }
  }

  it should "support hidden key" in {
    // half key hidden
    hasPerson("Hidden-1") shouldBe false
    Post("/data/by_hidden_key_view_1", """{"surname": "MeHidden"}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.path.toString
      location shouldBe "/data/by_hidden_key_view_1/MeHidden"
    }
    hasPerson("Hidden-1") shouldBe true
    Put("/data/by_hidden_key_view_1/MeHidden", """{"surname": "MeHiddenUpd"}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.path.toString
      location shouldBe "/data/by_hidden_key_view_1/MeHiddenUpd"
    }
    Get("/data/by_hidden_key_view_1/MeHiddenUpd") ~> route ~> check {
      status shouldEqual StatusCodes.OK
      header[`Content-Type`].get.contentType shouldBe ContentTypes.`application/json`
      entityAs[String] shouldBe """{"surname":"MeHiddenUpd"}"""
    }
    Delete("/data/by_hidden_key_view_1/MeHiddenUpd") ~> route ~> check {
      status shouldEqual StatusCodes.OK
    }
    hasPerson("Hidden-1") shouldBe false

    // whole key hidden
    Get("/data/create/by_hidden_key_view_2") ~> route ~> check {
      status shouldEqual StatusCodes.OK
      header[`Content-Type`].get.contentType shouldBe ContentTypes.`application/json`
      entityAs[String] shouldBe """{"surname":"Surname"}"""
    }
    hasPerson("Hidden-2") shouldBe false
    Get("/data/count/by_hidden_key_view_2") ~> route ~> check {
      status shouldEqual StatusCodes.OK
      entityAs[String].toInt shouldBe 0
    }
    Post("/data/by_hidden_key_view_2", """{"surname": "MeHidden"}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.path.toString
      location shouldBe "/data/by_hidden_key_view_2"
    }
    hasPerson("Hidden-2", "MeHidden") shouldBe true
    deletePerson("Hidden-2")
    hasPerson("Hidden-2", "MeHidden") shouldBe false
    Post("/data/by_hidden_key_view_2/", """{"surname": "MeHidden"}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.path.toString
      location shouldBe "/data/by_hidden_key_view_2"
    }
    hasPerson("Hidden-2", "MeHidden") shouldBe true
    Get("/data/count/by_hidden_key_view_2") ~> route ~> check {
      status shouldEqual StatusCodes.OK
      entityAs[String].toInt shouldBe 1
    }
    Get("/data/count/by_hidden_key_view_2?name=IgnoreMe") ~> route ~> check {
      status shouldEqual StatusCodes.OK
      entityAs[String].toInt shouldBe 1
    }
    Put("/data/by_hidden_key_view_2/", """{"surname": "MeHiddenUpd0"}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.path.toString
      location shouldBe "/data/by_hidden_key_view_2"
    }
    hasPerson("Hidden-2", "MeHiddenUpd0") shouldBe true
    Put("/data/by_hidden_key_view_2", """{"surname": "MeHiddenUpd"}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.path.toString
      location shouldBe "/data/by_hidden_key_view_2"
    }
    hasPerson("Hidden-2", "MeHiddenUpd") shouldBe true
    Get("/data/by_hidden_key_view_2") ~> route ~> check {
      status shouldEqual StatusCodes.OK
      header[`Content-Type`].get.contentType shouldBe ContentTypes.`application/json`
      entityAs[String] shouldBe """{"surname":"MeHiddenUpd"}"""
    }
    Delete("/data/by_hidden_key_view_2") ~> route ~> check {
      status shouldEqual StatusCodes.OK
    }
    hasPerson("Hidden-2") shouldBe false
  }

  it should "support date value as key" in {
    hasPerson("DateIns") shouldBe false
    hasPerson("DateUpd") shouldBe false
    Post("/data/by_date_key_view", """{"name": "DateIns", "birthdate": "2022-08-02"}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.path.toString
      location shouldBe "/data/by_date_key_view/2022-08-02"
    }
    hasPerson("DateIns") shouldBe true
    Get("/data/by_date_key_view/2022-08-02") ~> route ~> check {
      status shouldEqual StatusCodes.OK
      header[`Content-Type`].get.contentType shouldBe ContentTypes.`application/json`
      entityAs[String] shouldBe s"""{"birthdate":"2022-08-02","name":"DateIns"}"""
    }
    Put("/data/by_date_key_view/2022-08-02", s"""{"name": "DateUpd"}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.path.toString
      location shouldBe "/data/by_date_key_view/2022-08-02"
    }
    hasPerson("DateUpd") shouldBe true
    Delete("/data/by_date_key_view/2022-08-02") ~> route ~> check {
      status shouldEqual StatusCodes.OK
    }
    hasPerson("DateIns") shouldBe false
    hasPerson("DateUpd") shouldBe false
  }

  it should "support datetime value as key" in {
    // sql timestamp
    hasPerson("DtTmIns") shouldBe false
    hasPerson("DtTmUpd") shouldBe false
    Post("/data/by_datetime_key_view",
        """{"name": "DtTmIns", "date_time": "2022-08-02 05:45:00"}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.path.toString
      location shouldBe "/data/by_datetime_key_view/2022-08-02_05:45"
    }
    hasPerson("DtTmIns") shouldBe true
    Get("/data/by_datetime_key_view/2022-08-02_05:45:00.0") ~> route ~> check {
      status shouldEqual StatusCodes.OK
      header[`Content-Type`].get.contentType shouldBe ContentTypes.`application/json`
      entityAs[String] shouldBe s"""{"date_time":"2022-08-02 05:45:00.0","name":"DtTmIns"}"""
    }
    Get("/data/by_datetime_key_view/2022-08-02_05:45:33") ~> route ~> check {
      status shouldEqual StatusCodes.NotFound
    }
    Get("/data/by_datetime_key_view/2022-08-02_05:45") ~> route ~> check {
      status shouldEqual StatusCodes.OK
    }
    Get("/data/by_datetime_key_view/2022-08-02_05:45:00") ~> route ~> check {
      status shouldEqual StatusCodes.OK
    }
    Get("/data/by_datetime_key_view/2022-08-02_05:45:00.0") ~> route ~> check {
      status shouldEqual StatusCodes.OK
    }
    Get("/data/by_datetime_key_view/2022-08-02T05:45:00") ~> route ~> check {
      status shouldEqual StatusCodes.OK
    }
    Get("/data/by_datetime_key_view/2022-08-02T05:45:00.0") ~> route ~> check {
      status shouldEqual StatusCodes.OK
    }
    Get("/data/by_datetime_key_view/2022-08-02%2005:45:00") ~> route ~> check {
      status shouldEqual StatusCodes.OK
    }
    Put("/data/by_datetime_key_view/2022-08-02_05:45:00", s"""{"name": "DtTmUpd"}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.path.toString
      location shouldBe "/data/by_datetime_key_view/2022-08-02_05:45"
    }
    hasPerson("DtTmUpd") shouldBe true
    Delete("/data/by_datetime_key_view/2022-08-02_05:45:00.0") ~> route ~> check {
      status shouldEqual StatusCodes.OK
    }
    hasPerson("DtTmIns") shouldBe false
    hasPerson("DtTmUpd") shouldBe false
    Post("/data/by_datetime_key_view",
        """{"name": "DtTmIns", "date_time": "2022-08-02 05:45:01.234"}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.path.toString
      location shouldBe "/data/by_datetime_key_view/2022-08-02_05:45:01.234"
    }
    hasPerson("DtTmIns") shouldBe true
    Delete("/data/by_datetime_key_view/2022-08-02_05:45:01.234") ~> route ~> check {
      status shouldEqual StatusCodes.OK
    }
    hasPerson("DtTmIns") shouldBe false
    // local datetime
    Post("/data/by_local_datetime_key_view",
        """{"name": "DtTmIns", "l_date_time": "2022-08-02 05:45:00"}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.path.toString
      location shouldBe "/data/by_local_datetime_key_view/2022-08-02_05:45"
    }
    hasPerson("DtTmIns") shouldBe true
    Get("/data/by_local_datetime_key_view/2022-08-02_05:45:00.0") ~> route ~> check {
      status shouldEqual StatusCodes.OK
      header[`Content-Type`].get.contentType shouldBe ContentTypes.`application/json`
      entityAs[String] shouldBe s"""{"l_date_time":"2022-08-02 05:45:00.0","name":"DtTmIns"}"""
    }
    Get("/data/by_local_datetime_key_view/2022-08-02_05:45:33") ~> route ~> check {
      status shouldEqual StatusCodes.NotFound
    }
    Get("/data/by_local_datetime_key_view/2022-08-02_05:45") ~> route ~> check {
      status shouldEqual StatusCodes.OK
    }
    Get("/data/by_local_datetime_key_view/2022-08-02_05:45:00") ~> route ~> check {
      status shouldEqual StatusCodes.OK
    }
    Get("/data/by_local_datetime_key_view/2022-08-02_05:45:00.0") ~> route ~> check {
      status shouldEqual StatusCodes.OK
    }
    Get("/data/by_local_datetime_key_view/2022-08-02T05:45:00") ~> route ~> check {
      status shouldEqual StatusCodes.OK
    }
    Get("/data/by_local_datetime_key_view/2022-08-02T05:45:00.0") ~> route ~> check {
      status shouldEqual StatusCodes.OK
    }
    Get("/data/by_local_datetime_key_view/2022-08-02%2005:45:00") ~> route ~> check {
      status shouldEqual StatusCodes.OK
    }
    Put("/data/by_local_datetime_key_view/2022-08-02_05:45:00", s"""{"name": "DtTmUpd"}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.path.toString
      location shouldBe "/data/by_local_datetime_key_view/2022-08-02_05:45"
    }
    hasPerson("DtTmUpd") shouldBe true
    Delete("/data/by_local_datetime_key_view/2022-08-02_05:45:00.0") ~> route ~> check {
      status shouldEqual StatusCodes.OK
    }
    hasPerson("DtTmIns") shouldBe false
    hasPerson("DtTmUpd") shouldBe false
    Post("/data/by_local_datetime_key_view",
        """{"name": "DtTmIns", "l_date_time": "2022-08-02 05:45:01.234"}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.path.toString
      location shouldBe "/data/by_local_datetime_key_view/2022-08-02_05:45:01.234"
    }
    hasPerson("DtTmIns") shouldBe true
    Delete("/data/by_local_datetime_key_view/2022-08-02_05:45:01.234") ~> route ~> check {
      status shouldEqual StatusCodes.OK
    }
    hasPerson("DtTmIns") shouldBe false
  }

  // exception handling --------------------------------------//
  it should "respect api" in {
    Get("/data/no_api_view/0") ~> route ~> check {
      status shouldEqual StatusCodes.BadRequest
      responseAs[String] shouldBe "no_api_view.get is not a part of this API"
    }
    Get("/data/no_api_view") ~> route ~> check {
      status shouldEqual StatusCodes.BadRequest
      responseAs[String] shouldBe "no_api_view.list is not a part of this API"
    }
    Get("/data/count/no_api_view") ~> route ~> check {
      status shouldEqual StatusCodes.BadRequest
      responseAs[String] shouldBe "no_api_view.count is not a part of this API"
    }
    Get("/data/create/no_api_view") ~> route ~> check {
      status shouldEqual StatusCodes.BadRequest
      responseAs[String] shouldBe "no_api_view.create is not a part of this API"
    }
    Post("/data/no_api_view", "{}") ~> route ~> check {
      status shouldEqual StatusCodes.BadRequest
      responseAs[String] shouldBe "no_api_view.insert is not a part of this API"
    }
    Put("/data/no_api_view/0", "{}") ~> route ~> check {
      status shouldEqual StatusCodes.BadRequest
      responseAs[String] shouldBe "no_api_view.update is not a part of this API"
    }
    Delete("/data/no_api_view/0") ~> route ~> check {
      status shouldEqual StatusCodes.BadRequest
      responseAs[String] shouldBe "no_api_view.delete is not a part of this API"
    }
  }
}
