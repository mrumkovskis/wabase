package org.wabase

import akka.http.scaladsl.marshalling.Marshal
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
    import svc.{dtoUnmarshaller, toResponseWabaseResultMarshaller}
    var httpResponse: HttpResponse = null
    def ctx(viewName: String) =
      svc.app.AppActionContext(null, null, null, null, null)(null, null, null, null, null)
        .copy(viewName = viewName)(null, null, null, null, null)
    def wRes(viewName: String, quereaseResult: QuereaseResult) =
      svc.app.WabaseResult(ctx(viewName), quereaseResult)
    def toBodyString(marshallable: svc.app.WabaseResult): String =
      Await.result(Await.result(Marshal(marshallable).to[HttpResponse], 1.second)
        .entity.toStrict(1.second), 1.second).data.utf8String
    def marshal(marshallable: svc.app.WabaseResult) =
      Await.result(Marshal(marshallable).to[HttpResponse], 1.second)

    var childCopy: JsonDecoderSpecs.decoder_test_child = null
    val child   = new JsonDecoderSpecs.decoder_test_child
    val chilD  = child.asInstanceOf[AppQuerease#DTO]

    child.id = 333
    child.name = "CHILD-1"
    child.date = java.sql.Date.valueOf("2021-11-08")
    child.date_time = java.sql.Timestamp.valueOf("2021-12-26 23:57:14.0")
    val fullJson     = """{"id":333,"name":"CHILD-1","date":"2021-11-08","date_time":"2021-12-26 23:57:14.0"}"""
    val filteredJson = """{"id":333,"date":"2021-11-08","date_time":"2021-12-26 23:57:14.0"}"""

    httpResponse = marshal(wRes("decoder_test_child", PojoResult(chilD)))
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

    httpResponse = marshal(wRes("decoder_test", PojoResult(child.asInstanceOf[AppQuerease#DTO])))

    childCopy = Await.result(Unmarshal(httpResponse.entity).to[JsonDecoderSpecs.decoder_test_child], 1.second)
    childCopy.id        shouldBe child.id
    childCopy.name      shouldBe null
    childCopy.date      shouldBe child.date
    childCopy.date_time shouldBe child.date_time

    toBodyString(wRes("decoder_test_child", OptionResult(Option(chilD))))       shouldBe fullJson
    toBodyString(wRes("decoder_test_child", MapResult(chilD.toMap)))            shouldBe fullJson
    toBodyString(wRes("decoder_test_child", PojoResult(chilD)))                 shouldBe fullJson
    toBodyString(wRes("decoder_test_child", ListResult(Nil)))                   shouldBe "[]"
    toBodyString(wRes("decoder_test_child", ListResult(List(chilD))))           shouldBe s"[$fullJson]"
    toBodyString(wRes("decoder_test_child", ListResult(List(chilD, chilD))))    shouldBe s"[$fullJson,$fullJson]"

    toBodyString(wRes("decoder_test", OptionResult(Option(chilD))))     shouldBe filteredJson
    toBodyString(wRes("decoder_test", MapResult(chilD.toMap)))          shouldBe filteredJson
    toBodyString(wRes("decoder_test", PojoResult(chilD)))               shouldBe filteredJson
    toBodyString(wRes("decoder_test", ListResult(Nil)))                 shouldBe "[]"
    toBodyString(wRes("decoder_test", ListResult(List(chilD))))         shouldBe s"[$filteredJson]"
    toBodyString(wRes("decoder_test", ListResult(List(chilD, chilD))))  shouldBe s"[$filteredJson,$filteredJson]"
  }

  it should "marshal number result" in {
    val svc = service
    import svc.toEntityQuereaseNumberResultMarshaller
    var entity: MessageEntity = null
    var entityFuture: Future[MessageEntity] = null

    val result = new NumberResult(42)
    entityFuture = Marshal(result).to[MessageEntity]
    entity = Await.result(entityFuture, 1.second)
    entity.contentType shouldEqual ContentTypes.`text/plain(UTF-8)`
    Await.result(entity.toStrict(1.second), 1.second).data.decodeString("UTF-8") shouldEqual "42"
  }

  it should "marshal status result" in {
    val svc = service
    import svc.toResponseQuereaseStatusResultMarshaller
    def response(sr: StatusResult) = Await.result(Marshal(sr).to[HttpResponse], 1.second)

    var res: HttpResponse = null
    res = response(StatusResult(200, null))
    res.status shouldEqual StatusCodes.OK

    res = response(StatusResult(200, "ok"))
    Await.result(res.entity.toStrict(1.second).map(_.data.decodeString("UTF-8")), 1.second) shouldEqual "ok"

    res = response(StatusResult(303, "data/path", Nil, ListMap()))
    res.status shouldEqual StatusCodes.SeeOther
    res.header[Location] shouldEqual Some(Location("data/path"))

    res = response(StatusResult(303, "data/path", List("1"), ListMap()))
    res.status shouldEqual StatusCodes.SeeOther
    res.header[Location] shouldEqual Some(Location("data/path/1"))

    res = response(StatusResult(303, "data/path", Nil, ListMap("id" -> "1")))
    res.status shouldEqual StatusCodes.SeeOther
    res.header[Location] shouldEqual Some(Location("data/path?id=1"))

    res = response(StatusResult(303, "/data", List("path", "redirect"), ListMap()))
    res.status shouldEqual StatusCodes.SeeOther
    res.header[Location] shouldEqual Some(Location("/data/path/redirect"))

    res = response(StatusResult(303, "person_health", List("Mr. Gunza", "2021-06-05"), ListMap("par1" -> "val1", "par2" -> "val2")))
    res.status shouldEqual StatusCodes.SeeOther
    res.header[Location] shouldEqual Some(Location("person_health/Mr.%20Gunza/2021-06-05?par1=val1&par2=val2"))

    res = response(StatusResult(303, "data/path/2", List(), ListMap()))
    res.status shouldEqual StatusCodes.SeeOther
    res.header[Location] shouldEqual Some(Location("data/path/2"))

    intercept[IllegalArgumentException](response(StatusResult(303, null, List("4"), ListMap("par1" -> "5"))))

    res = response(StatusResult(303, "data/path", List(null), ListMap()))
    res.status shouldEqual StatusCodes.SeeOther
    res.header[Location] shouldEqual Some(Location("data/path/null"))

    res = response(StatusResult(303, "data/path", List(), ListMap("id" -> null)))
    res.status shouldEqual StatusCodes.SeeOther
    res.header[Location] shouldEqual Some(Location("data/path?id="))
  }
}
