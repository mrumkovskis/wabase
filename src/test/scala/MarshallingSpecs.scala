package org.wabase

import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model._
import akka.http.scaladsl.testkit.ScalatestRouteTest
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
}
