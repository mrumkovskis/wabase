package org.wabase

import akka.http.scaladsl.marshalling.{Marshal, ToResponseMarshaller}
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.Location
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.http.scaladsl.unmarshalling.Unmarshal
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.ListMap
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class MarshallingSpecs extends AnyFlatSpec with Matchers with TestQuereaseInitializer with ScalatestRouteTest {

  var streamerConfQe: QuereaseProvider with AppFileStreamerConfig = _

  var service: TestAppService = _

  var service2: TestAppService = _

  override def beforeAll(): Unit = {
    querease = new TestQuerease("/json-decoder-specs-metadata.yaml") {
      override lazy val viewNameToClassMap = JsonDecoderSpecs.viewNameToClass
    }
    val db = new DbAccess with Loggable {
      override val tresqlResources = null
    }

    super.beforeAll()

    val appl = new TestApp {
      override type QE = TestQuerease
      override protected def initQuerease: QE = querease
      override def dbAccessDelegate: DbAccess = db
    }

    service = new TestAppService(system) {
      override def initApp: App = appl
    }

    service2 = new TestAppService(system) {
      override def initApp: App = new TestApp {
        override type QE = TestQuerease
        override protected def initQuerease: QE = new TestQuerease("/json-decoder-specs-metadata.yaml") {
          override lazy val viewNameToClassMap = JsonDecoderSpecs.viewNameToClass
          override val tresqlUri: TresqlUri = new TresqlUri {
            override def uriWithKey(uri: Uri, key: Seq[Any]): Uri = uriWithKeyInPath(uri, key)
          }
        }
        override def dbAccessDelegate: DbAccess = db
      }
      override val jsonValueEncoder = w => {
        case d: java.sql.Date => w.writeString(d.toString.replaceAll("-", "."))
      }
    }
  }

  it should "marshal dto" in {
    val svc = service
    import svc.{dtoMarshaller, dtoSeqMarshaller}
    var entity: MessageEntity = null
    var entityFuture: Future[MessageEntity] = null

    val dto = new JsonDecoderSpecs.decoder_test
    entityFuture = Marshal(dto).to[MessageEntity]
    entity = Await.result(entityFuture, 1.second)
    entity.contentType shouldEqual ContentTypes.`application/json`

    val dtoSeq = List(new JsonDecoderSpecs.decoder_test, new JsonDecoderSpecs.decoder_test)
    entityFuture = Marshal(dtoSeq).to[MessageEntity]
    entity = Await.result(entityFuture, 1.second)
    entity.contentType shouldEqual ContentTypes.`application/json`
  }

  it should "marshal wabase result according to view" in {
    val svc = service
    import svc.app.qe
    import svc.{dtoUnmarshaller, toResponseWabaseResultMarshaller, mapForViewMarshaller, seqOfMapsForViewMarshaller}
    var httpResponse: HttpResponse = null
    def ctx(viewName: String) =
      svc.app.AppActionContext(null, null, null, null, null)(null, null, null, null, null, null)
        .copy(viewName = viewName)(null, null, null, null, null, null)
    def wRes(viewName: String, quereaseResult: QuereaseResult) =
      svc.app.WabaseResult(ctx(viewName), quereaseResult)
    def toBodyString[A: ToResponseMarshaller](marshallable: A): String =
      Await.result(Await.result(Marshal(marshallable).to[HttpResponse], 1.second)
        .entity.toStrict(1.second), 1.second).data.utf8String
    def marshal[A: ToResponseMarshaller](marshallable: A) =
      Await.result(Marshal(marshallable).to[HttpResponse], 1.second)

    var childCopy: JsonDecoderSpecs.decoder_test_child = null
    val child   = new JsonDecoderSpecs.decoder_test_child
    val chilD  = child.asInstanceOf[AppQuerease#DTO]

    child.id = 333
    child.name = "CHILD-1"
    child.date = java.sql.Date.valueOf("2021-11-08")
    child.date_time = java.sql.Timestamp.valueOf("2021-12-26 23:57:14.0")
    val fullJson     = """{"id":333,"name":"CHILD-1","date":"2021-11-08","date_time":"2021-12-26 23:57:14"}"""
    val filteredJson = """{"id":333,"date":"2021-11-08","date_time":"2021-12-26 23:57:14"}"""

    httpResponse = marshal((chilD.toMap, "decoder_test_child"))
    httpResponse.entity.contentType shouldEqual ContentTypes.`application/json`

    childCopy = Await.result(Unmarshal(httpResponse.entity).to[JsonDecoderSpecs.decoder_test_child], 1.second)
    childCopy.id        shouldBe child.id
    childCopy.name      shouldBe child.name
    childCopy.date      shouldBe child.date
    childCopy.date_time shouldBe child.date_time

    val otherCopy = Await.result(Unmarshal(httpResponse.entity).to[JsonDecoderSpecs.decoder_test], 1.second)
    otherCopy.id        shouldBe child.id
    otherCopy.string    shouldBe null
    otherCopy.date      shouldBe child.date
    otherCopy.date_time shouldBe child.date_time

    httpResponse = marshal((child.asInstanceOf[AppQuerease#DTO].toMap, "decoder_test"))

    childCopy = Await.result(Unmarshal(httpResponse.entity).to[JsonDecoderSpecs.decoder_test_child], 1.second)
    childCopy.id        shouldBe child.id
    childCopy.name      shouldBe null
    childCopy.date      shouldBe child.date
    childCopy.date_time shouldBe child.date_time

    toBodyString((chilD.toMap, "decoder_test_child"))       shouldBe fullJson
    toBodyString(wRes("decoder_test_child", MapResult(chilD.toMap)))            shouldBe fullJson
    toBodyString((Nil, "decoder_test_child"))                   shouldBe "[]"
    toBodyString((List(chilD.toMap), "decoder_test_child"))           shouldBe s"[$fullJson]"
    toBodyString((List(chilD, chilD).map(_.toMap), "decoder_test_child"))    shouldBe s"[$fullJson,$fullJson]"

    toBodyString(wRes("decoder_test", MapResult(chilD.toMap)))          shouldBe filteredJson
    toBodyString((Nil, "decoder_test"))                 shouldBe "[]"
    toBodyString((List(chilD.toMap), "decoder_test"))         shouldBe s"[$filteredJson]"
    toBodyString((List(chilD, chilD).map(_.toMap), "decoder_test"))  shouldBe s"[$filteredJson,$filteredJson]"
  }

  it should "marshal number result" in {
    val svc = service
    import svc.toEntityQuereaseLongResultMarshaller
    var entity: MessageEntity = null
    var entityFuture: Future[MessageEntity] = null

    val result = new LongResult(42)
    entityFuture = Marshal(result).to[MessageEntity]
    entity = Await.result(entityFuture, 1.second)
    entity.contentType shouldEqual ContentTypes.`text/plain(UTF-8)`
    Await.result(entity.toStrict(1.second), 1.second).data.decodeString("UTF-8") shouldEqual "42"
  }

  it should "marshal status result, key in query" in {
    val svc = service
    import svc.toResponseQuereaseStatusResultMarshaller
    def response(sr: StatusResult) = Await.result(Marshal(sr).to[HttpResponse], 1.second)

    var res: HttpResponse = null
    res = response(StatusResult(200, null))
    res.status shouldEqual StatusCodes.OK

    res = response(StatusResult(200, StringStatus("ok")))
    Await.result(res.entity.toStrict(1.second).map(_.data.decodeString("UTF-8")), 1.second) shouldEqual "ok"

    res = response(StatusResult(303, RedirectStatus(TresqlUri.Uri("/", Nil, ListMap()))))
    res.status shouldEqual StatusCodes.SeeOther
    res.header[Location] shouldEqual Some(Location("/"))

    res = response(StatusResult(303, RedirectStatus(TresqlUri.Uri("", List("s1", "1"), ListMap())))) // NOTE: if keys are specified uri must not end with slash !
    res.status shouldEqual StatusCodes.SeeOther
    res.header[Location] shouldEqual Some(Location("?/s1/1"))

    res = response(StatusResult(303, RedirectStatus(TresqlUri.Uri("data/path", Nil, ListMap()))))
    res.status shouldEqual StatusCodes.SeeOther
    res.header[Location] shouldEqual Some(Location("data/path"))

    res = response(StatusResult(303, RedirectStatus(TresqlUri.Uri("data/path", List("1"), ListMap()))))
    res.status shouldEqual StatusCodes.SeeOther
    res.header[Location] shouldEqual Some(Location("data/path?/1"))

    res = response(StatusResult(303, RedirectStatus(TresqlUri.Uri("data/path", Nil, ListMap("id" -> "1")))))
    res.status shouldEqual StatusCodes.SeeOther
    res.header[Location] shouldEqual Some(Location("data/path?id=1"))

    res = response(StatusResult(303, RedirectStatus(TresqlUri.Uri("/data", List("path", "redirect"), ListMap()))))
    res.status shouldEqual StatusCodes.SeeOther
    res.header[Location] shouldEqual Some(Location("/data?/path/redirect"))

    res = response(StatusResult(303, RedirectStatus(TresqlUri.Uri("/data", List("sub/path"), ListMap()))))
    res.status shouldEqual StatusCodes.SeeOther
    res.header[Location] shouldEqual Some(Location("/data?/sub%2Fpath"))

    res = response(StatusResult(303, RedirectStatus(TresqlUri.Uri("/data", List("sub?path"), ListMap()))))
    res.status shouldEqual StatusCodes.SeeOther
    res.header[Location] shouldEqual Some(Location("/data?/sub%3Fpath"))

    res = response(StatusResult(303, RedirectStatus(TresqlUri.Uri("person_health", List("Mr. Gunza", "2021-06-05"), ListMap("par1" -> "val1", "par2" -> "val2")))))
    res.status shouldEqual StatusCodes.SeeOther
    res.header[Location] shouldEqual Some(Location("person_health?/Mr.%20Gunza/2021-06-05?par1=val1&par2=val2"))

    res = response(StatusResult(303, RedirectStatus(TresqlUri.Uri("data/path/2", List(), ListMap()))))
    res.status shouldEqual StatusCodes.SeeOther
    res.header[Location] shouldEqual Some(Location("data/path/2"))

    intercept[IllegalArgumentException](response(StatusResult(303, RedirectStatus(TresqlUri.Uri(null, List("4"), ListMap("par1" -> "5"))))))

    res = response(StatusResult(303, RedirectStatus(TresqlUri.Uri("data/path", List(null), ListMap()))))
    res.status shouldEqual StatusCodes.SeeOther
    res.header[Location] shouldEqual Some(Location("data/path?/null"))

    res = response(StatusResult(303, RedirectStatus(TresqlUri.Uri("data/path", List(), ListMap("id" -> null)))))
    res.status shouldEqual StatusCodes.SeeOther
    res.header[Location] shouldEqual Some(Location("data/path?id="))

    res = response(StatusResult(303, RedirectStatus(TresqlUri.Uri("data/ēūīāšģķļžčņ", List("ēūīāšģķļžč/ņ"), ListMap("ē/ūīāšģķļžčņ" -> "ēūīāš/ģķļžčņ")))))
    res.status shouldEqual StatusCodes.SeeOther
    res.header[Location] shouldEqual Some(Location("data/%C4%93%C5%AB%C4%AB%C4%81%C5%A1%C4%A3%C4%B7%C4%BC%C5%BE%C4%8D%C5%86?/%C4%93%C5%AB%C4%AB%C4%81%C5%A1%C4%A3%C4%B7%C4%BC%C5%BE%C4%8D%2F%C5%86?%C4%93/%C5%AB%C4%AB%C4%81%C5%A1%C4%A3%C4%B7%C4%BC%C5%BE%C4%8D%C5%86=%C4%93%C5%AB%C4%AB%C4%81%C5%A1/%C4%A3%C4%B7%C4%BC%C5%BE%C4%8D%C5%86"))

    res = response(StatusResult(303, RedirectStatus(TresqlUri.Uri("http://foo.org", List(), ListMap()))))
    res.status shouldEqual StatusCodes.SeeOther
    res.header[Location] shouldEqual Some(Location("http://foo.org"))

    res = response(StatusResult(303, RedirectStatus(TresqlUri.Uri("https://foo.org:8080/", List(), ListMap()))))
    res.status shouldEqual StatusCodes.SeeOther
    res.header[Location] shouldEqual Some(Location("https://foo.org:8080/"))

    res = response(StatusResult(303, RedirectStatus(TresqlUri.Uri("http://foo.org", List("s1", "s2"), ListMap()))))
    res.status shouldEqual StatusCodes.SeeOther
    res.header[Location] shouldEqual Some(Location("http://foo.org?/s1/s2"))

    res = response(StatusResult(303, RedirectStatus(TresqlUri.Uri("http://foo.org/data", List("sā1", "sī2"), ListMap()))))
    res.status shouldEqual StatusCodes.SeeOther
    res.header[Location] shouldEqual Some(Location("http://foo.org/data?/s%C4%811/s%C4%AB2"))
  }

  it should "marshal status result, key in path" in {
    val svc = service2
    import svc.toResponseQuereaseStatusResultMarshaller
    def response(sr: StatusResult) = Await.result(Marshal(sr).to[HttpResponse], 1.second)

    var res: HttpResponse = null
    res = response(StatusResult(200, null))
    res.status shouldEqual StatusCodes.OK

    res = response(StatusResult(200, StringStatus("ok")))
    Await.result(res.entity.toStrict(1.second).map(_.data.decodeString("UTF-8")), 1.second) shouldEqual "ok"

    res = response(StatusResult(303, RedirectStatus(TresqlUri.Uri("/", Nil, ListMap()))))
    res.status shouldEqual StatusCodes.SeeOther
    res.header[Location] shouldEqual Some(Location("/"))

    res = response(StatusResult(303, RedirectStatus(TresqlUri.Uri("", List("s1", "1"), ListMap())))) // NOTE: if keys are specified uri must not end with slash !
    res.status shouldEqual StatusCodes.SeeOther
    res.header[Location] shouldEqual Some(Location("/s1/1"))

    res = response(StatusResult(303, RedirectStatus(TresqlUri.Uri("data/path", Nil, ListMap()))))
    res.status shouldEqual StatusCodes.SeeOther
    res.header[Location] shouldEqual Some(Location("data/path"))

    res = response(StatusResult(303, RedirectStatus(TresqlUri.Uri("data/path", List("1"), ListMap()))))
    res.status shouldEqual StatusCodes.SeeOther
    res.header[Location] shouldEqual Some(Location("data/path/1"))

    res = response(StatusResult(303, RedirectStatus(TresqlUri.Uri("data/path", Nil, ListMap("id" -> "1")))))
    res.status shouldEqual StatusCodes.SeeOther
    res.header[Location] shouldEqual Some(Location("data/path?id=1"))

    res = response(StatusResult(303, RedirectStatus(TresqlUri.Uri("/data", List("path", "redirect"), ListMap()))))
    res.status shouldEqual StatusCodes.SeeOther
    res.header[Location] shouldEqual Some(Location("/data/path/redirect"))

    res = response(StatusResult(303, RedirectStatus(TresqlUri.Uri("/data", List("sub/path"), ListMap()))))
    res.status shouldEqual StatusCodes.SeeOther
    res.header[Location] shouldEqual Some(Location("/data/sub%2Fpath"))

    res = response(StatusResult(303, RedirectStatus(TresqlUri.Uri("/data", List("sub?path"), ListMap()))))
    res.status shouldEqual StatusCodes.SeeOther
    res.header[Location] shouldEqual Some(Location("/data/sub%3Fpath"))

    res = response(StatusResult(303, RedirectStatus(TresqlUri.Uri("person_health", List("Mr. Gunza", "2021-06-05"), ListMap("par1" -> "val1", "par2" -> "val2")))))
    res.status shouldEqual StatusCodes.SeeOther
    res.header[Location] shouldEqual Some(Location("person_health/Mr.%20Gunza/2021-06-05?par1=val1&par2=val2"))

    res = response(StatusResult(303, RedirectStatus(TresqlUri.Uri("data/path/2", List(), ListMap()))))
    res.status shouldEqual StatusCodes.SeeOther
    res.header[Location] shouldEqual Some(Location("data/path/2"))

    intercept[IllegalArgumentException](response(StatusResult(303, RedirectStatus(TresqlUri.Uri(null, List("4"), ListMap("par1" -> "5"))))))

    res = response(StatusResult(303, RedirectStatus(TresqlUri.Uri("data/path", List(null), ListMap()))))
    res.status shouldEqual StatusCodes.SeeOther
    res.header[Location] shouldEqual Some(Location("data/path/null"))

    res = response(StatusResult(303, RedirectStatus(TresqlUri.Uri("data/path", List(), ListMap("id" -> null)))))
    res.status shouldEqual StatusCodes.SeeOther
    res.header[Location] shouldEqual Some(Location("data/path?id="))

    res = response(StatusResult(303, RedirectStatus(TresqlUri.Uri("data/ēūīāšģķļžčņ", List("ēūīāšģķļžč/ņ"), ListMap("ē/ūīāšģķļžčņ" -> "ēūīāš/ģķļžčņ")))))
    res.status shouldEqual StatusCodes.SeeOther
    res.header[Location] shouldEqual Some(Location("data/%C4%93%C5%AB%C4%AB%C4%81%C5%A1%C4%A3%C4%B7%C4%BC%C5%BE%C4%8D%C5%86/%C4%93%C5%AB%C4%AB%C4%81%C5%A1%C4%A3%C4%B7%C4%BC%C5%BE%C4%8D%2F%C5%86?%C4%93/%C5%AB%C4%AB%C4%81%C5%A1%C4%A3%C4%B7%C4%BC%C5%BE%C4%8D%C5%86=%C4%93%C5%AB%C4%AB%C4%81%C5%A1/%C4%A3%C4%B7%C4%BC%C5%BE%C4%8D%C5%86"))

    res = response(StatusResult(303, RedirectStatus(TresqlUri.Uri("http://foo.org", List(), ListMap()))))
    res.status shouldEqual StatusCodes.SeeOther
    res.header[Location] shouldEqual Some(Location("http://foo.org"))

    res = response(StatusResult(303, RedirectStatus(TresqlUri.Uri("https://foo.org:8080/", List(), ListMap()))))
    res.status shouldEqual StatusCodes.SeeOther
    res.header[Location] shouldEqual Some(Location("https://foo.org:8080/"))

    res = response(StatusResult(303, RedirectStatus(TresqlUri.Uri("http://foo.org", List("s1", "s2"), ListMap()))))
    res.status shouldEqual StatusCodes.SeeOther
    res.header[Location] shouldEqual Some(Location("http://foo.org/s1/s2"))

    res = response(StatusResult(303, RedirectStatus(TresqlUri.Uri("http://foo.org/data", List("sā1", "sī2"), ListMap()))))
    res.status shouldEqual StatusCodes.SeeOther
    res.header[Location] shouldEqual Some(Location("http://foo.org/data/s%C4%811/s%C4%AB2"))
  }

  it should "marshal conf result to json" in {
    val svc = service2
    import svc.toEntityConfResultMarshaller
    def response(cr: ConfResult) = Await.result(
      Marshal(cr).to[HttpEntity]
        .flatMap(_.toStrict(1.second))
        .map(_.data)
        .map (new CborOrJsonAnyValueDecoder().decode(_)),
      1.second)

    def checkSelf(param: String, value: Any) =
      response(ConfResult(param, value)) shouldEqual value
    def check(param: String, value: Any, res: Any) =
      response(ConfResult(param, value)) shouldEqual res
    checkSelf("int", 1)
    checkSelf("number", 3.14)
    checkSelf("string", "string value")
    checkSelf("boolean", true)
    checkSelf("array", List("a", "b", "c"))
    check("config", Map(
      "s" -> "string",
      "b" -> true,
      "n" -> BigDecimal(1.34), "null" -> null,
      "a" -> List(1, null, "s", false, Map(
        "s" -> "string",
        "b" -> true,
        "n" -> 1.4,
        "date" -> java.sql.Date.valueOf("2000-01-01")))),
      Map(
        "s" -> "string",
        "b" -> true,
        "n" -> BigDecimal(1.34), "null" -> null,
        "a" -> List(1, null, "s", false, Map(
          "s" -> "string",
          "b" -> true,
          "n" -> 1.4,
          "date" -> "2000.01.01")))
    )
  }
}
