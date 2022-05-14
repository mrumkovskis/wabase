package org.wabase

import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model._
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.http.scaladsl.unmarshalling.Unmarshal
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

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
      svc.app.ActionContext(null, null, null, null)(null, null, null, null, null)
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
}
