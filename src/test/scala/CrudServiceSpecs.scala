package org.wabase

import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.model.headers.{Location, `Content-Type`}
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpRequest, MediaTypes, StatusCodes, Uri}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{ExceptionHandler, Rejection, RejectionHandler, RequestContext, Route, RouteResult}
import akka.http.scaladsl.settings.{ParserSettings, RoutingSettings}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.stream.Materializer
import akka.util.ByteString
import java.time.{LocalDate, LocalTime, LocalDateTime}
import java.time.format.DateTimeFormatter
import org.mojoz.querease.{QuereaseMetadata, ValueConverter}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.tresql.{convInt, DMLResult, Query, ThreadLocalResources, dialects}
import spray.json._

import scala.concurrent.{ExecutionContextExecutor, Future}

class CrudTestService(system: ActorSystem, testApp: TestApp) extends TestAppService(system) {
  override def initApp          = testApp
  override def initFileStreamer = testApp
  implicit val user: TestUsr    = null
  implicit val exceptionHandler: ExceptionHandler = appExceptionHandler
  implicit val rejectionHandler: RejectionHandler = RejectionHandler.default
  implicit val state:            ApplicationState = ApplicationState(Map.empty)
  val route =
    Route.seal {
      apiPath {
        apiAction
      } ~
      metadataPath {
        metadataAction
      } ~
      crudPath {
        crudAction
      }
    }
}

object CrudServiceSpecsDtos {
  class json_test_any extends DtoWithId {
    var id: java.lang.Long = null
    var value: String = null
  }
  class json_test_types extends DtoWithId {
    var id: java.lang.Long = null
    var child: json_test_types_child = null
  }
  class json_test_types_child extends DtoWithId {
    var id: java.lang.Long = null
    var long: java.lang.Long = null
    var string: String = null
    var instant:       java.time.Instant        = null
    var l_date:        java.time.LocalDate      = null
    var l_time:        java.time.LocalTime      = null
    var l_date_time:   java.time.LocalDateTime  = null
    var o_date_time:   java.time.OffsetDateTime = null
    var z_date_time:   java.time.ZonedDateTime  = null
    var sql_date:      java.sql.Date            = null
    var sql_time:      java.sql.Time            = null
    var sql_timestamp: java.sql.Timestamp       = null
    var int: java.lang.Integer = null
    var bigint: scala.math.BigInt = null
    var double: java.lang.Double = null
    var decimal: scala.math.BigDecimal = null
    var boolean: java.lang.Boolean = null
    var bytes: Array[Byte] = null
    var json: String = null
    var long_arr: List[java.lang.Long] = Nil
    var string_arr: List[String] = Nil
    var date_arr: List[java.time.LocalDate] = Nil
    var time_arr: List[java.time.LocalTime] = Nil
    var date_time_arr: List[java.time.LocalDateTime] = Nil
    var int_arr: List[java.lang.Integer] = Nil
    var bigint_arr: List[scala.math.BigInt] = Nil
    var double_arr: List[java.lang.Double] = Nil
    var decimal_arr: List[scala.math.BigDecimal] = Nil
    var boolean_arr: List[java.lang.Boolean] = Nil
    var child: types_test_child = null
    var children: List[types_test_child] = Nil
  }
  class json_test_types_legacy_flow extends json_test_types with DtoWithId
  class types_test_child extends Dto {
    var name: String = null
    var date: java.time.LocalDate = null
    var date_time: java.time.LocalDateTime = null
  }

  val viewNameToClass = Map[String, Class[_ <: Dto]](
    "json_test_any"               -> classOf[json_test_any],
    "json_test_types"             -> classOf[json_test_types],
    "json_test_types_child"       -> classOf[json_test_types_child],
    "json_test_types_legacy_flow" -> classOf[json_test_types_legacy_flow],
    "types_test_child"            -> classOf[types_test_child],
  )
}

class CrudServiceSpecs extends AnyFlatSpec with Matchers with TestQuereaseInitializer with ScalatestRouteTest {
  import CrudServiceSpecsDtos._
  var dbAccess: DbAccess        = _
  var service:  CrudTestService = _
  var route:    Route           = _

  override def dbNamePrefix: String = "main"
  override def beforeAll(): Unit = {
    querease    = new TestQuerease("/crud-service-specs-metadata.yaml") {
      override lazy val defaultCpName = "main"
      override lazy val viewNameToClassMap = CrudServiceSpecsDtos.viewNameToClass
    }
    qio         = new AppQuereaseIo[Dto](querease)
    super.beforeAll()
    dbAccess    = new DbAccess with Loggable {
      override val tresqlResources = CrudServiceSpecs.this.tresqlThreadLocalResources
      override protected def tresqlMetadata = querease.tresqlMetadata
    }
    val testApp = new TestApp with NoValidation {
      override protected def initQuerease = querease
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
    Get("/data/by_id_view_1?/0") ~> route ~> check {
      status shouldEqual StatusCodes.NotFound
    }
    Get("/data/by_id_view_1/0") ~> route ~> check {
      status shouldEqual StatusCodes.NotFound
    }
    val id = createPerson("John")
    Get(s"/data/by_id_view_1/$id") ~> route ~> check {
      status shouldEqual StatusCodes.OK
      header[`Content-Type`].get.contentType shouldBe ContentTypes.`application/json`
      entityAs[String] shouldBe s"""{"id":$id,"name":"John","surname":null}"""
    }
    Get(s"/data/by_id_view_1?/$id") ~> route ~> check {
      status shouldEqual StatusCodes.OK
      header[`Content-Type`].get.contentType shouldBe ContentTypes.`application/json`
      entityAs[String] shouldBe s"""{"id":$id,"name":"John","surname":null}"""
    }
  }

  it should "get by key" in {
    Get(s"/data/by_key_view_1/Jar/Key") ~> route ~> check {
      status shouldEqual StatusCodes.NotFound
    }
    Get(s"/data/by_key_view_1?/Jar/Key") ~> route ~> check {
      status shouldEqual StatusCodes.NotFound
    }
    val id = createPerson("Jar", "Key")
    Get(s"/data/by_key_view_1/Jar/Key") ~> route ~> check {
      status shouldEqual StatusCodes.OK
      header[`Content-Type`].get.contentType shouldBe ContentTypes.`application/json`
      entityAs[String] shouldBe s"""{"name":"Jar","surname":"Key"}"""
    }
    Get(s"/data/by_key_view_1?/Jar/Key") ~> route ~> check {
      status shouldEqual StatusCodes.OK
      header[`Content-Type`].get.contentType shouldBe ContentTypes.`application/json`
      entityAs[String] shouldBe s"""{"name":"Jar","surname":"Key"}"""
    }
    Get(s"/data/by_key_view_1?/Wu/null") ~> route ~> check {
      status shouldEqual StatusCodes.NotFound
    }
    createPerson("Wu")
    Get(s"/data/by_key_view_1?/Wu/null") ~> route ~> check {
      status shouldEqual StatusCodes.OK
      header[`Content-Type`].get.contentType shouldBe ContentTypes.`application/json`
      entityAs[String] shouldBe s"""{"name":"Wu","surname":null}"""
    }
    // ensure string keys are not used as numbers and leading zeros are not lost
    hasPerson("01110") shouldBe false
    createPerson("01110")
    hasPerson("01110") shouldBe true
    Get(s"/data/by_key_view_2?/01110") ~> route ~> check {
      status shouldEqual StatusCodes.OK
      header[`Content-Type`].get.contentType shouldBe ContentTypes.`application/json`
      entityAs[String] shouldBe s"""{"name":"01110","surname":null}"""
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
    Get("/data/count:by_id_view_1") ~> route ~> check {
      status shouldEqual StatusCodes.OK
      header[`Content-Type`].get.contentType shouldBe ContentTypes.`text/plain(UTF-8)`
    }
    Get("/data/count/by_id_view_1") ~> route ~> check {
      status shouldEqual StatusCodes.OK
      header[`Content-Type`].get.contentType shouldBe ContentTypes.`text/plain(UTF-8)`
    }
    Get("/data/count:by_id_view_1?name=A") ~> route ~> check {
      status shouldEqual StatusCodes.OK
      header[`Content-Type`].get.contentType shouldBe ContentTypes.`text/plain(UTF-8)`
      entityAs[String].toInt shouldBe 0
    }
    Get("/data/count/by_id_view_1?name=A") ~> route ~> check {
      status shouldEqual StatusCodes.OK
      header[`Content-Type`].get.contentType shouldBe ContentTypes.`text/plain(UTF-8)`
      entityAs[String].toInt shouldBe 0
    }
    val id1 = createPerson("Anna")
    Get("/data/count:by_id_view_1?name=A") ~> route ~> check {
      status shouldEqual StatusCodes.OK
      header[`Content-Type`].get.contentType shouldBe ContentTypes.`text/plain(UTF-8)`
      entityAs[String].toInt shouldBe 1
    }
    Get("/data/count/by_id_view_1?name=A") ~> route ~> check {
      status shouldEqual StatusCodes.OK
      header[`Content-Type`].get.contentType shouldBe ContentTypes.`text/plain(UTF-8)`
      entityAs[String].toInt shouldBe 1
    }
  }

  it should "create" in {
    Get("/data/create:by_id_view_1") ~> route ~> check {
      status shouldEqual StatusCodes.OK
      header[`Content-Type`].get.contentType shouldBe ContentTypes.`application/json`
      entityAs[String] shouldBe """{"id":null,"name":null,"surname":null}"""
    }
    Get("/data/create/by_id_view_1") ~> route ~> check {
      status shouldEqual StatusCodes.OK
      header[`Content-Type`].get.contentType shouldBe ContentTypes.`application/json`
      entityAs[String] shouldBe """{"id":null,"name":null,"surname":null}"""
    }
    Get("/data/create:by_key_view_1") ~> route ~> check {
      status shouldEqual StatusCodes.OK
      header[`Content-Type`].get.contentType shouldBe ContentTypes.`application/json`
      entityAs[String] shouldBe """{"name":null,"surname":null}"""
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
      val location = header[Location].get.uri.toString
      location should startWith ("/data/by_id_view_1?/")
      (location.substring(location.lastIndexOf("/") + 1).toLong > 0) shouldBe true
    }
    hasPerson("Sia") shouldBe true
  }

  it should "insert by key" in {
    hasPerson("Jane") shouldBe false
    Post("/data/by_key_view_1", """{"name": "Jane"}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.toString
      location shouldBe "/data/by_key_view_1?/Jane/null"
    }
    hasPerson("Jane") shouldBe true
    hasPerson("Bruce") shouldBe false
    Post("/data/by_key_view_1", """{"name": "Bruce", "surname": "Fur"}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.toString
      location shouldBe "/data/by_key_view_1?/Bruce/Fur"
    }
    hasPerson("Bruce") shouldBe true
  }

  it should "update by id" in {
    hasPerson("Peter") shouldBe false
    hasPerson("Pete")  shouldBe false
    val id = createPerson("Peter")
    hasPerson("Peter") shouldBe true
    hasPerson("Pete")  shouldBe false
    Put(s"/data/by_id_view_1/${id + 420}", s"""{"id": $id,"name": "Pete"}""") ~> route ~> check {
      status shouldEqual StatusCodes.NotFound
    }
    Put(s"/data/by_id_view_1/$id", s"""{"id": $id,"name": "Pete"}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.toString
      location shouldBe s"/data/by_id_view_1?/$id"
    }
    hasPerson("Peter") shouldBe false
    hasPerson("Pete")  shouldBe true
    Put(s"/data/by_id_view_1?/$id", s"""{"id": $id,"name": "P"}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.toString
      location shouldBe s"/data/by_id_view_1?/$id"
    }
    hasPerson("P") shouldBe true
    hasPerson("Pete")  shouldBe false
  }

  it should "update by key" in {
    hasPerson("KeyName") shouldBe false
    val id = createPerson("KeyName", "OldSurname")
    hasPerson("KeyName", "OldSurname") shouldBe true
    Put(s"/data/by_key_view_2/MissingKeyName", s"""{"name": "KeyName", "surname": "NewSurname"}""") ~> route ~> check {
      status shouldEqual StatusCodes.NotFound
    }
    Put(s"/data/by_key_view_2/KeyName", s"""{"name": "KeyName", "surname": "NewSurname"}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.toString
      location shouldBe "/data/by_key_view_2?/KeyName"
    }
    hasPerson("KeyName", "OldSurname") shouldBe false
    hasPerson("KeyName", "NewSurname") shouldBe true
    // do not redirect to helper view
    Put(s"/data/by_key_view_3/KeyName", s"""{"name": "KeyName", "surname": "NewSurname"}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.toString
      location shouldBe "/data/by_key_view_3?/KeyName"
    }
    Put(s"/data/by_key_view_3?/KeyName", s"""{"name": "KeyName2", "surname": "NewSurname"}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.toString
      location shouldBe "/data/by_key_view_3?/KeyName2"
    }
    // ensure string keys are not used as numbers and leading zeros are not lost
    hasPerson("02220") shouldBe false
    createPerson("02220", "OldSurname")
    hasPerson("02220", "OldSurname") shouldBe true
    Put(s"/data/by_key_view_2/02220", s"""{"name": "02221", "surname": "NewSurname"}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.toString
      location shouldBe "/data/by_key_view_2?/02221"
    }
    hasPerson("02221", "NewSurname") shouldBe true
  }

  it should "update key" in {
    hasPerson("Winnie") shouldBe false
    hasPerson("Bear")   shouldBe false
    val id = createPerson("Winnie", "Pooh")
    hasPerson("Winnie") shouldBe true
    hasPerson("Bear")   shouldBe false
    Put(s"/data/by_key_view_1?/Winnie/Pooh", s"""{"name": "Greedy", "surname": "Pooh"}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.toString
      location shouldBe "/data/by_key_view_1?/Greedy/Pooh"
    }
    Put(s"/data/by_key_view_1/Greedy/Pooh", s"""{"name": "Bear", "surname": "Pooh"}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.toString
      location shouldBe "/data/by_key_view_1?/Bear/Pooh"
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
      val location = header[Location].get.uri.toString
      location shouldBe "/data/by_magic_key_view_1?/MagicIns"
    }
    hasPerson("MagicIns") shouldBe true
    hasPerson("MagicUpd") shouldBe false
    hasPerson("NotMagic") shouldBe false
    Put(s"/data/by_magic_key_view_1/MagicIns", s"""{"name": "NotMagic"}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.toString
      location shouldBe "/data/by_magic_key_view_1?/MagicUpd"
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
      val location = header[Location].get.uri.toString
      location shouldBe "/data/by_magic_key_view_2?/MagicIns"
    }
    hasPerson("MagicIns") shouldBe true
    hasPerson("MagicUpd") shouldBe false
    hasPerson("NotMagic") shouldBe false
    Put(s"/data/by_magic_key_view_2/MagicIns", s"""{"name": "NotMagic"}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.toString
      location shouldBe "/data/by_magic_key_view_2?/MagicUpd"
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
      val location = header[Location].get.uri.toString
      location shouldBe "/data/by_readonly_key_view_1?/RoName"
    }
    hasPerson("RoName") shouldBe true
    hasPerson("RwName") shouldBe false
    Put(s"/data/by_readonly_key_view_1?/RoName", s"""{"name": "RwName", "surname": null}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.toString
      location shouldBe "/data/by_readonly_key_view_1?/RoName"
    }
    hasPerson("RoName") shouldBe true
    hasPerson("RwName") shouldBe false
  }

  it should "delete by id" in {
    hasPerson("Tiger") shouldBe false
    val idt = createPerson("Tiger")
    hasPerson("Tiger") shouldBe true
    Delete(s"/data/by_id_view_1?/$idt") ~> route ~> check {
      status shouldEqual StatusCodes.OK
    }
    hasPerson("Tiger") shouldBe false
    hasPerson("Scott") shouldBe false
    val id = createPerson("Scott")
    hasPerson("Scott") shouldBe true
    Delete(s"/data/by_id_view_1/$id") ~> route ~> check {
      status shouldEqual StatusCodes.OK
    }
    hasPerson("Scott") shouldBe false
    Delete(s"/data/by_id_view_1/$id") ~> route ~> check {
      status shouldEqual StatusCodes.NotFound
    }
  }

  it should "delete by key" in {
    hasPerson("Deleme") shouldBe false
    createPerson("Deleme")
    hasPerson("Deleme") shouldBe true
    Delete(s"/data/by_key_view_2?/Deleme") ~> route ~> check {
      status shouldEqual StatusCodes.OK
    }
    hasPerson("Deleme") shouldBe false
    Delete(s"/data/by_key_view_2?/Deleme") ~> route ~> check {
      status shouldEqual StatusCodes.NotFound
    }
    // ensure string keys are not used as numbers and leading zeros are not lost
    hasPerson("03330") shouldBe false
    val id = createPerson("03330")
    hasPerson("03330") shouldBe true
    Delete(s"/data/by_key_view_2?/03330") ~> route ~> check {
      status shouldEqual StatusCodes.OK
    }
    hasPerson("03330") shouldBe false
  }

  it should "redirect by key explicitly" in {
    hasPerson("RediName") shouldBe false
    // on insert redirect to this explicitly
    Post("/data/by_key_redirect_view_1", """{"name": "RediName"}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.toString
      location shouldBe "/data/by_key_redirect_view_1?/RediName"
    }
    // on update redirect to this explicitly
    Put(s"/data/by_key_redirect_view_1/RediName", s"""{"name": "RediNameUpd"}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.toString
      location shouldBe "/data/by_key_redirect_view_1?/RediNameUpd"
    }
    hasPerson("RediOther") shouldBe false
    // on insert redirect to another view explicitly
    Post("/data/by_key_redirect_view_2", """{"name": "RediOther", "surname": "Surnm"}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.toString
      location shouldBe "/data/by_key_view_1?/RediOther/Surnm"
    }
    // on update redirect to another view explicitly
    Put(s"/data/by_key_redirect_view_2/RediOther",
        s"""{"name": "RediOther", "surname": "SurnmUpd"}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.toString
      location shouldBe "/data/by_key_view_1?/RediOther/SurnmUpd"
    }
  }

  it should "support hidden key" in {
    // half key hidden
    hasPerson("Hidden-1") shouldBe false
    Post("/data/by_hidden_key_view_1", """{"surname": "MeHidden"}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.toString
      location shouldBe "/data/by_hidden_key_view_1?/MeHidden"
    }
    hasPerson("Hidden-1") shouldBe true
    Put("/data/by_hidden_key_view_1/MeHidden", """{"surname": "MeHiddenUpd"}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.toString
      location shouldBe "/data/by_hidden_key_view_1?/MeHiddenUpd"
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

    // half key hidden, upsert, redirect
    hasPerson("Hidden-3") shouldBe false
    Post("/data/by_hidden_key_view_3", """{"surname": "MeHidden", "d": "2022-08-10"}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.toString
      location shouldBe "/data/by_hidden_key_view_3?/MeHidden"
    }
    hasPerson("Hidden-3") shouldBe true
    Put("/data/by_hidden_key_view_3?/MeHidden", """{"surname": "MeHidden", "d": "2022-08-11"}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.toString
      location shouldBe "/data/by_hidden_key_view_3?/MeHidden"
    }
    hasPerson("Hidden-3") shouldBe true
    Get("/data/by_hidden_key_view_3?/MeHidden") ~> route ~> check {
      status shouldEqual StatusCodes.OK
      header[`Content-Type`].get.contentType shouldBe ContentTypes.`application/json`
      entityAs[String] shouldBe """{"surname":"MeHidden","d":"2022-08-11"}"""
    }
  }

  it should "support date value as key" in {
    hasPerson("DateIns") shouldBe false
    hasPerson("DateUpd") shouldBe false
    Post("/data/by_date_key_view", """{"name": "DateIns", "birthdate": "2022-08-02"}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.toString
      location shouldBe "/data/by_date_key_view?/2022-08-02"
    }
    hasPerson("DateIns") shouldBe true
    Get("/data/by_date_key_view/2022-08-02") ~> route ~> check {
      status shouldEqual StatusCodes.OK
      header[`Content-Type`].get.contentType shouldBe ContentTypes.`application/json`
      entityAs[String] shouldBe s"""{"birthdate":"2022-08-02","name":"DateIns"}"""
    }
    Put("/data/by_date_key_view/2022-08-02", s"""{"name": "DateUpd"}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.toString
      location shouldBe "/data/by_date_key_view?/2022-08-02"
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
      val location = header[Location].get.uri.toString
      location shouldBe "/data/by_datetime_key_view?/2022-08-02_05:45:00"
    }
    hasPerson("DtTmIns") shouldBe true
    Get("/data/by_datetime_key_view/2022-08-02_05:45:00.0") ~> route ~> check {
      status shouldEqual StatusCodes.OK
      header[`Content-Type`].get.contentType shouldBe ContentTypes.`application/json`
      entityAs[String] shouldBe s"""{"date_time":"2022-08-02 05:45:00","name":"DtTmIns"}"""
    }
    Get("/data/by_datetime_key_view/2022-08-02_05:45:33") ~> route ~> check {
      status shouldEqual StatusCodes.NotFound
    }
    Get("/data/by_datetime_key_view?/2022-08-02_05:45") ~> route ~> check {
      status shouldEqual StatusCodes.OK
    }
    Get("/data/by_datetime_key_view?/2022-08-02_05:45:00") ~> route ~> check {
      status shouldEqual StatusCodes.OK
    }
    Get("/data/by_datetime_key_view?/2022-08-02_05:45:00.0") ~> route ~> check {
      status shouldEqual StatusCodes.OK
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
      val location = header[Location].get.uri.toString
      location shouldBe "/data/by_datetime_key_view?/2022-08-02_05:45:00"
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
      val location = header[Location].get.uri.toString
      location shouldBe "/data/by_datetime_key_view?/2022-08-02_05:45:01.234"
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
      val location = header[Location].get.uri.toString
      location shouldBe "/data/by_local_datetime_key_view?/2022-08-02_05:45:00"
    }
    hasPerson("DtTmIns") shouldBe true
    Get("/data/by_local_datetime_key_view/2022-08-02_05:45:00.0") ~> route ~> check {
      status shouldEqual StatusCodes.OK
      header[`Content-Type`].get.contentType shouldBe ContentTypes.`application/json`
      entityAs[String] shouldBe s"""{"l_date_time":"2022-08-02 05:45:00","name":"DtTmIns"}"""
    }
    Get("/data/by_local_datetime_key_view/2022-08-02_05:45:33") ~> route ~> check {
      status shouldEqual StatusCodes.NotFound
    }
    Get("/data/by_local_datetime_key_view?/2022-08-02_05:45") ~> route ~> check {
      status shouldEqual StatusCodes.OK
    }
    Get("/data/by_local_datetime_key_view?/2022-08-02_05:45:00") ~> route ~> check {
      status shouldEqual StatusCodes.OK
    }
    Get("/data/by_local_datetime_key_view?/2022-08-02_05:45:00.0") ~> route ~> check {
      status shouldEqual StatusCodes.OK
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
      val location = header[Location].get.uri.toString
      location shouldBe "/data/by_local_datetime_key_view?/2022-08-02_05:45:00"
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
      val location = header[Location].get.uri.toString
      location shouldBe "/data/by_local_datetime_key_view?/2022-08-02_05:45:01.234"
    }
    hasPerson("DtTmIns") shouldBe true
    Delete("/data/by_local_datetime_key_view/2022-08-02_05:45:01.234") ~> route ~> check {
      status shouldEqual StatusCodes.OK
    }
    hasPerson("DtTmIns") shouldBe false
  }

  // alternative supported response formats ------------------//
  it should "support csv, ods, excel" in {
    Get("/data/by_id_view_1?name=Z") ~> addHeader("Accept", "text/csv") ~> route ~> check {
      status shouldEqual StatusCodes.OK
      header[`Content-Type`].get.contentType shouldBe ContentTypes.`text/csv(UTF-8)`
      entityAs[String] shouldBe "" // TODO are we sure?
    }
    val id1 = createPerson("Zoe")
    val id2 = createPerson("Zorg")
    Get("/data/by_id_view_1?name=Z") ~> addHeader("Accept", "text/csv") ~> route ~> check {
      status shouldEqual StatusCodes.OK
      header[`Content-Type`].get.contentType shouldBe ContentTypes.`text/csv(UTF-8)`
      entityAs[String] shouldBe List(
        s"""Id,Name,Surname""",
        s"""$id1,Zoe,""",
        s"""$id2,Zorg,""",
      ).mkString("", "\n", "\n")
    }
    Get("/data/by_id_view_1?name=Z") ~> addHeader(
        "Accept", "application/vnd.oasis.opendocument.spreadsheet") ~> route ~> check {
      status shouldEqual StatusCodes.OK
      header[`Content-Type`].get.contentType.mediaType.shouldBe(
        MediaTypes.`application/vnd.oasis.opendocument.spreadsheet`
      )
      // TODO unzip, test ods, maybe elsewhere
    }
    Get("/data/by_id_view_1?name=Z") ~> addHeader(
        "Accept", "application/vnd.ms-excel") ~> route ~> check {
      status shouldEqual StatusCodes.OK
      header[`Content-Type`].get.contentType.mediaType shouldBe MediaTypes.`application/vnd.ms-excel`
      val excelXml = entityAs[String]
      excelXml should startWith (List(
        """<?xml version="1.0" encoding="UTF-8"?>""",
        """<?mso-application progid="Excel.Sheet"?>""",
        """<Workbook xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet"""",
      ).mkString("\n"))
      excelXml should include ("Zoe")
      excelXml should include ("Zorg")
      excelXml should endWith ("</Workbook>\n")
    }
  }

  // api -----------------------------------------------------//
  it should "serve api metadata" in {
    val io = qio
    import io._
    Get("/api") ~> route ~> check {
      status shouldEqual StatusCodes.OK
      val apiMap =  responseAs[String].parseJson.convertTo[Map[String, Any]]
      apiMap("by_id_view_1") shouldBe Seq("count", "create", "delete", "get", "save", "list")
      apiMap("by_hidden_key_view_2") shouldBe Seq("count", "create", "delete", "get", "save")
      apiMap.get("no_api_view") shouldBe None
    }
  }

  // metadata ------------------------------------------------//
  it should "serve metadata" in {
    // TODO test full metadata and various aspects
    val io = qio
    import io._
    Get("/metadata/by_key_view_1") ~> route ~> check {
      status shouldEqual StatusCodes.OK
      val mdMap =  responseAs[String].parseJson.convertTo[Map[String, Any]]
      mdMap("key") shouldBe Seq("name", "surname")
    }
    Get("/metadata/by_hidden_key_view_1") ~> route ~> check {
      status shouldEqual StatusCodes.OK
      val mdMap =  responseAs[String].parseJson.convertTo[Map[String, Any]]
      mdMap("key") shouldBe Seq("surname")
    }
  }

  // exception handling --------------------------------------//
  it should "respect api" in {
    Get("/data/no_api_view/0") ~> route ~> check {
      status shouldEqual StatusCodes.BadRequest
      responseAs[String] shouldBe "no_api_view.get is not a part of this API"
    }
    Get("/data/no.api.view/0") ~> route ~> check {
      status shouldEqual StatusCodes.BadRequest
      responseAs[String] shouldBe "no.api.view.get is not a part of this API"
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
    Post("/data/non_existing_view", "{bad json}") ~> route ~> check {
      status shouldEqual StatusCodes.BadRequest
      responseAs[String] should include ("non_existing_view.insert is not a part of this API")
    }
    Put("/data/non_existing_view/0", "{bad json}") ~> route ~> check {
      status shouldEqual StatusCodes.BadRequest
      responseAs[String] should include ("non_existing_view.update is not a part of this API")
    }
    Put("/data/non_existing_view?/0", "{bad json}") ~> route ~> check {
      status shouldEqual StatusCodes.BadRequest
      responseAs[String] should include ("non_existing_view.update is not a part of this API")
    }
  }

  it should "not reflect html" in {
    Get("/data/no%3Cb%3Eapi%3C%2Fb%3Eview/0") ~> route ~> check {
      status shouldEqual StatusCodes.BadRequest
      responseAs[String] shouldBe "Strange name.get is not a part of this API"
    }
    Get("/data/by_id_view_1?sort=x,no%3Cb%3Ecol%3C%2Fb%3E,id,z") ~> route ~> check {
      status shouldEqual StatusCodes.BadRequest
      responseAs[String] shouldBe "Not sortable: by_id_view_1 by x, (strange name), z"
    }
  }

  // JSON IO -------------------------------------------------//
  it should "transport json fields to db and back" in {
    var id: Long = 0L

    val jsons = Seq(
      null,
      """0""",
      """"0"""",
      """{}""",
      """[]""",
      """null""",
      """{"x":"y"}""",
      """["x","y"]""",
      """[1,"2","other"]""",
      """{"a":"b","c":{"d":["e"]}}""",
      """{"a":[1,"a",{},[],[42]],"x":1,"y":2,"z":"3","b":"d"}""",
    )

    Post("/data/json_test_any", """{}""") ~> route ~> check {
      status shouldEqual StatusCodes.SeeOther
      val location = header[Location].get.uri.toString
      location should startWith ("/data/json_test_any?/")
      id = location.substring(location.lastIndexOf("/") + 1).toLong
    }
    Get(s"/data/json_test_any?/$id") ~> route ~> check {
      status shouldEqual StatusCodes.OK
      responseAs[String] shouldBe s"""{"id":$id,"value":null}"""
    }

    for (value <- jsons) yield {
      Put(s"/data/json_test_any?/$id", s"""{"value":$value}""") ~> route ~> check {
        status shouldEqual StatusCodes.SeeOther
      }
      Get(s"/data/json_test_any?/$id") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[String] shouldBe s"""{"id":$id,"value":$value}"""
      }
    }

    // json io types ---------------------------------------------------------------
    val j1 = new json_test_any

    implicit val qe: QuereaseMetadata with ValueConverter = querease
    import DefaultAppQuereaseIo.MapJsonFormat
    def checkDtoRoundtrip(obj: json_test_types): Unit = {
      def comparable(s: String) =
        s.replace("E+", "E") // compact exponent
      val json = comparable(obj.toMap.toJson.compactPrint)
      import ResultEncoder.jsValEncoder
      import ResultEncoder.JsonEncoder.jsValueEncoderPF
      val json2: String = ResultEncoder.encodeToJsonString(obj.toMap)
      json shouldBe json2

      Put(s"/data/json_test_types?/$id", json) ~> route ~> check {
        status shouldEqual StatusCodes.SeeOther
      }
      Get(s"/data/json_test_any?/$id") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        comparable(responseAs[String]) shouldBe json.replace(s"""{"id":$id,"child"""", s"""{"id":$id,"value"""")
      }
      Get(s"/data/json_test_types?/$id") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        comparable(responseAs[String]) shouldBe json
      }
      Get(s"/data/json_test_types_legacy_flow?/$id") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[String] shouldBe json
      }

      Put(s"/data/json_test_types_legacy_flow?/$id", HttpEntity.Strict(ContentTypes.`application/json`, ByteString(json))) ~> route ~> check {
        status shouldEqual StatusCodes.SeeOther
      }

      Get(s"/data/json_test_any?/$id") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        comparable(responseAs[String]) shouldBe json.replace(s"""{"id":$id,"child"""", s"""{"id":$id,"value"""")
      }
      Get(s"/data/json_test_types?/$id") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        comparable(responseAs[String]) shouldBe json
      }
      Get(s"/data/json_test_types_legacy_flow?/$id") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[String] shouldBe json
      }

      val obj2 = new json_test_types
      obj2.fill(json.parseJson.asJsObject, emptyStringsToNull = false)
      if (obj.child == null)
        obj2.child shouldBe null
      else {
        val c1 = obj.child
        val c2 = obj2.child
        c2.id            shouldBe c1.id
        c2.long          shouldBe c1.long
        c2.string        shouldBe c1.string
        c2.instant       shouldBe c1.instant
        c2.l_date        shouldBe c1.l_date
        c2.l_time        shouldBe c1.l_time
        c2.l_date_time   shouldBe c1.l_date_time
        c2.o_date_time   shouldBe c1.o_date_time
        c2.z_date_time   shouldBe c1.z_date_time
        c2.sql_date      shouldBe c1.sql_date
        c2.sql_time      shouldBe c1.sql_time
        c2.sql_timestamp shouldBe c1.sql_timestamp
        c2.int           shouldBe c1.int
        c2.bigint        shouldBe c1.bigint
        c2.double        shouldBe c1.double
        c2.decimal       shouldBe c1.decimal
        c2.boolean       shouldBe c1.boolean
        c2.bytes         shouldBe c1.bytes
        c2.json          shouldBe c1.json
        if (c1.long_arr      == null)        c2.long_arr      shouldBe null else {
            c2.long_arr     .length shouldBe c1.long_arr     .length
            c2.long_arr.zip(c1.long_arr).foreach {
              case (e1, e2) => e1 shouldBe e2
            }
        }
        if (c1.string_arr    == null)        c2.string_arr    shouldBe null else {
            c2.string_arr   .length shouldBe c1.string_arr   .length
            c2.string_arr.zip(c1.string_arr).foreach {
              case (e1, e2) => e1 shouldBe e2
            }
        }
        if (c1.date_arr      == null)        c2.date_arr      shouldBe null else {
            c2.date_arr     .length shouldBe c1.date_arr     .length
            c2.date_arr.zip(c1.date_arr).foreach {
              case (e1, e2) => e1 shouldBe e2
            }
        }
        if (c1.time_arr      == null)        c2.time_arr      shouldBe null else {
            c2.time_arr     .length shouldBe c1.time_arr     .length
            c2.time_arr.zip(c1.time_arr).foreach {
              case (e1, e2) => e1 shouldBe e2
            }
        }
        if (c1.date_time_arr == null)        c2.date_time_arr shouldBe null else {
            c2.date_time_arr.length shouldBe c1.date_time_arr.length
            c2.date_time_arr.zip(c1.date_time_arr).foreach {
              case (e1, e2) => e1 shouldBe e2
            }
        }
        if (c1.int_arr       == null)        c2.int_arr       shouldBe null else {
            c2.int_arr      .length shouldBe c1.int_arr      .length
            c2.int_arr.zip(c1.int_arr).foreach {
              case (e1, e2) => e1 shouldBe e2
            }
        }
        if (c1.bigint_arr    == null)        c2.bigint_arr    shouldBe null else {
            c2.bigint_arr   .length shouldBe c1.bigint_arr   .length
            c2.bigint_arr.zip(c1.bigint_arr).foreach {
              case (e1, e2) => e1 shouldBe e2
            }
        }
        if (c1.double_arr    == null)        c2.double_arr    shouldBe null else {
            c2.double_arr   .length shouldBe c1.double_arr   .length
            c2.double_arr.zip(c1.double_arr).foreach {
              case (e1, e2) => e1 shouldBe e2
            }
        }
        if (c1.decimal_arr   == null)        c2.decimal_arr   shouldBe null else {
            c2.decimal_arr  .length shouldBe c1.decimal_arr  .length
            c2.decimal_arr.zip(c1.decimal_arr).foreach {
              case (e1, e2) => e1 shouldBe e2
            }
        }
        if (c1.boolean_arr   == null)        c2.boolean_arr   shouldBe null else {
            c2.boolean_arr  .length shouldBe c1.boolean_arr  .length
            c2.boolean_arr.zip(c1.boolean_arr).foreach {
              case (e1, e2) => e1 shouldBe e2
            }
        }
        /* TODO
        if (c1.child         == null) c2.child         shouldBe null else {
            c2.child        .length shouldBe c1.child        .length
            c2.child.zip(c1.child).foreach {
              case (e1, e2) => e1 shouldBe e2
            }
        }
        if (c1.children      == null) c2.children      shouldBe null else {
            c2.children     .length shouldBe c1.children     .length
            c2.children.zip(c1.children).foreach {
              case (e1, e2) => e1 shouldBe e2
            }
        }
        */
      }
    }

    // empty
    val obj = new json_test_types
    obj.id  = id
    obj.child shouldBe null
    checkDtoRoundtrip(obj)
    val child = new json_test_types_child
    obj.child = child
    obj.child shouldBe child
    obj.toMap.get("child").get.getClass.getName shouldBe "scala.collection.immutable.TreeMap"
    obj.toMap.toJson.compactPrint should startWith(s"""{"id":$id,"child":{"id":null""")

    // strings
    child.string = "Rūķīši-X-123"
    checkDtoRoundtrip(obj)

    // dates and times
    child.l_date        = LocalDate.parse("2021-12-21")
    child.l_time        = LocalTime.parse("10:42:15")
    child.l_date_time   =
      LocalDateTime.parse("2021-12-26 23:57:14.0".replace(' ', 'T'), DateTimeFormatter.ISO_LOCAL_DATE_TIME)
    child.sql_date      = java.sql.Date.valueOf("2021-12-21")
    child.sql_time      = java.sql.Time.valueOf("10:42:15")
    child.sql_timestamp = java.sql.Timestamp.valueOf("2021-12-26 23:57:14")
    child.instant       = java.time.Instant.parse("2024-10-12T03:09:08.722844Z")
    child.o_date_time   = java.time.OffsetDateTime.parse("2024-10-12T06:09:45.382568+07:00")
    child.z_date_time   = java.time.ZonedDateTime.parse("2024-10-12T06:10:32.898849+03:00[Europe/Riga]")
    checkDtoRoundtrip(obj)
    child.l_date        = null
    child.l_time        = null
    child.l_date_time   = null
    child.sql_date      = null
    child.sql_time      = null
    child.sql_timestamp = null
    child.instant       = null
    child.o_date_time   = null
    child.z_date_time   = null

    // negatives
    child.long = Long.MinValue
    child.int = Integer.MIN_VALUE
    child.bigint = BigInt(Long.MinValue) - 1
    child.double = Double.MinValue
    child.boolean = false
    checkDtoRoundtrip(obj)

    // positives
    child.long = Long.MaxValue
    child.int = Integer.MAX_VALUE
    child.bigint = BigInt(Long.MaxValue) + 1
    child.double = Double.MaxValue
    child.boolean = true
    checkDtoRoundtrip(obj)

    // zeroes
    child.long   = 0L
    child.int    = 0
    child.bigint = BigInt(0)
    child.double = 0
    checkDtoRoundtrip(obj)

    // child view
    child.child = new types_test_child
    child.child.name = "CHILD-1"
    child.child.date = LocalDate.parse("2021-11-08")  // java.sql.Date.valueOf("2021-11-08")
    child.child.date_time =                           // java.sql.Timestamp.valueOf("2021-12-26 23:57:14.0")
      LocalDateTime.parse("2021-12-26 23:57:14.0".replace(' ', 'T'), DateTimeFormatter.ISO_LOCAL_DATE_TIME)
    checkDtoRoundtrip(obj)

    // children
    child.children = List(new types_test_child, new types_test_child)
    child.children(0).name = "CHILD-2"
    child.children(1).name = "CHILD-3"
    checkDtoRoundtrip(obj)

    child.long_arr = List(Long.MinValue, 0, 42, Long.MaxValue)
    child.string_arr = List("one", "two", "three")
    child.date_arr    = List(
      LocalDate.parse("2021-11-08"),
      LocalDate.parse("2024-04-16"),
    )
    child.time_arr = List(
      LocalTime.parse("10:42:15"),
      LocalTime.parse("17:06:45"),
    )
    child.date_time_arr = List(
      LocalDateTime.parse("2021-12-26 23:57:14.0".replace(' ', 'T'), DateTimeFormatter.ISO_LOCAL_DATE_TIME),
      LocalDateTime.parse("2024-01-16 13:09:10.2".replace(' ', 'T'), DateTimeFormatter.ISO_LOCAL_DATE_TIME),
    )
    child.int_arr      = List(Int.MinValue, 0, 42, Int.MaxValue)
    child.bigint_arr   = List(BigInt(0), BigInt(Long.MaxValue) + 1)
    child.double_arr   = List(0, Double.MaxValue)
    child.boolean_arr  = List(true, false)
    checkDtoRoundtrip(obj)

    j1.id = id
    Delete(s"/data/json_test_any?/$id") ~> route ~> check {
      status shouldEqual StatusCodes.OK
    }
  }

  it should "fail properly on bad json" in {
    Post("/data/by_id_view_1", "{bad json}") ~> route ~> check {
      status shouldEqual StatusCodes.BadRequest
      responseAs[String] should include ("""Expected '"' but got 'b' (input position 1)""")
    }
  }

  it should "fail properly on unhandled path" in {
    Get("/unhandled") ~> route ~> check {
      status shouldEqual StatusCodes.NotFound
    }
  }

  // alternative uris ----------------------------------------//
  it should "support key in query string" in {
    def keyToPath(s: String): String = {
      class RequestContext_(req: HttpRequest, u_path: Uri.Path) extends RequestContext {
        override val request = req
        override val unmatchedPath = u_path
        override implicit def executionContext: ExecutionContextExecutor = ???
        override implicit def materializer: Materializer = ???
        override def log: LoggingAdapter = ???
        override def settings: RoutingSettings = ???
        override def parserSettings: ParserSettings = ???
        override def reconfigure(
          executionContext: ExecutionContextExecutor, materializer: Materializer,
          log: LoggingAdapter, settings: RoutingSettings): RequestContext = ???
        override def complete(obj: ToResponseMarshallable): Future[RouteResult] = ???
        override def reject(rejections: Rejection*): Future[RouteResult] = ???
        override def redirect(uri: Uri, redirectionType: StatusCodes.Redirection): Future[RouteResult] = ???
        override def fail(error: Throwable): Future[RouteResult] = ???
        override def withRequest(req: HttpRequest): RequestContext =
          new RequestContext_(req, unmatchedPath)
        override def withExecutionContext(ec: ExecutionContextExecutor): RequestContext = ???
        override def withMaterializer(materializer: Materializer): RequestContext = ???
        override def withLog(log: LoggingAdapter): RequestContext = ???
        override def withRoutingSettings(settings: RoutingSettings): RequestContext = ???
        override def withParserSettings(settings: ParserSettings): RequestContext = ???
        override def mapRequest(f: HttpRequest => HttpRequest): RequestContext = ???
        override def withUnmatchedPath(path: Uri.Path): RequestContext =
          new RequestContext_(req, path)
        override def mapUnmatchedPath(f: Uri.Path => Uri.Path): RequestContext = ???
        override def withAcceptAll: RequestContext = ???
      }
      val req = HttpRequest(uri = s)
      val context = new RequestContext_(req, req.uri.path)
      val transformedContext = service.keyFromQueryToPath(context)
      transformedContext.request.uri.path shouldBe transformedContext.unmatchedPath
      transformedContext.request.uri.toString
    }
    keyToPath("http://localhost")             shouldBe "http://localhost"
    keyToPath("http://localhost?")            shouldBe "http://localhost?"
    keyToPath("http://localhost?a=b")         shouldBe "http://localhost?a=b"
    keyToPath("http://localhost/")            shouldBe "http://localhost/"
    keyToPath("http://localhost?/")           shouldBe "http://localhost/"
    keyToPath("http://localhost/?/")          shouldBe "http://localhost//"
    keyToPath("http://localhost?//")          shouldBe "http://localhost//"
    keyToPath("http://localhost?/a")          shouldBe "http://localhost/a"
    keyToPath("http://localhost?/a/")         shouldBe "http://localhost/a/"
    keyToPath("http://localhost?/a//")        shouldBe "http://localhost/a//"
    keyToPath("http://localhost?/a/b")        shouldBe "http://localhost/a/b"
    keyToPath("http://localhost?/a//b")       shouldBe "http://localhost/a//b"
    keyToPath("http://localhost?/a?/b")       shouldBe "http://localhost/a?/b"
    keyToPath("http://localhost?/a/b/")       shouldBe "http://localhost/a/b/"
    keyToPath("http://localhost?/a?b=c")      shouldBe "http://localhost/a?b=c"
    keyToPath("http://localhost?/a/?b=c")     shouldBe "http://localhost/a/?b=c"
    keyToPath("http://localhost?/a=b/?c=d")   shouldBe "http://localhost/a=b/?c=d"
    keyToPath("http://localhost?/a%20b/?c=d") shouldBe "http://localhost/a%20b/?c=d"
    keyToPath("http://localhost/p")           shouldBe "http://localhost/p"
    keyToPath("http://localhost/p?")          shouldBe "http://localhost/p?"
    keyToPath("http://localhost/p?a=b")       shouldBe "http://localhost/p?a=b"
    keyToPath("http://localhost/p?/")         shouldBe "http://localhost/p/"
    keyToPath("http://localhost/p/?/")        shouldBe "http://localhost/p//"
    keyToPath("http://localhost/p?/a")        shouldBe "http://localhost/p/a"
    keyToPath("http://localhost/p?/a/")       shouldBe "http://localhost/p/a/"
    keyToPath("http://local%3F%2Fhost/p?/a/") shouldBe "http://local%3F%2Fhost/p/a/"
  }
}
