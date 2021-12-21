package org.wabase

import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import akka.util.ByteString
import io.bullet.borer.{Json, Target}
import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import org.tresql._
import org.wabase.SerializerStreams.BorerArrayTreeEncoder

class SerializerStreamsSpecs extends FlatSpec with QuereaseBaseSpecs {

  implicit protected var tresqlResources: Resources = _
  implicit val system = ActorSystem("serializer-streams-specs")
  implicit val executor = system.dispatcher

  override def beforeAll(): Unit = {
    querease = new QuereaseBase("/querease-action-specs-metadata.yaml") {
      override lazy val viewNameToClassMap = QuereaseActionsDtos.viewNameToClass
    }
    super.beforeAll()
    // TODO get rid of thread local resources
    tresqlResources = tresqlThreadLocalResources.withConn(tresqlThreadLocalResources.conn)
    // persons
    Query("+person{id, name, surname, sex, birthdate} [1, 'John', 'Doe', 'M', '1969-01-01']")
    Query("+person{id, name, surname, sex, birthdate} [2, 'Jane',  null, 'F', '1996-02-02']")
    // accounts
    Query("+account{id, number, balance, last_modified, person_id} [1, 'X64',  1001.01, '2021-12-21 00:55:55', 1]")
    Query("+account{id, number, balance, last_modified, person_id} [2, 'X94',  2002.02, '2021-12-21 01:59:30', 1]")
  }

  behavior of "SerializedArraysTresqlResultSource"

  def serializeTresqlResult(query: String, format: Target, bufferSizeHint: Int = 8, wrap: Boolean = false) = {
    val source = SerializerStreams.createBorerSerializedArraysTresqlResultSource(
      () => Query(query), format, bufferSizeHint, new BorerArrayTreeEncoder(_, wrap = wrap)
    )
    val sink   = Sink.fold[String, ByteString]("") { case (acc, str) =>
      acc + str.decodeString("UTF-8")
    }
    Await.result(source.runWith(sink), 1.second)
  }

  it should "serialize flat tresql result as arrays to json" in {
    def queryString(maxId: Int) = s"person [id <= $maxId] {id, name, surname, sex, birthdate}"
    def test(maxId: Int, bufferSizeHint: Int, wrap: Boolean = false) =
      serializeTresqlResult(queryString(maxId), Json, bufferSizeHint, wrap)
    test(0,    8) shouldBe ""
    test(1,    8) shouldBe """[1,"John","Doe","M","1969-01-01"]"""
    test(1, 1024) shouldBe """[1,"John","Doe","M","1969-01-01"]"""
    test(2,    8) shouldBe """[1,"John","Doe","M","1969-01-01"],[2,"Jane",null,"F","1996-02-02"]"""
    test(2, 1024) shouldBe """[1,"John","Doe","M","1969-01-01"],[2,"Jane",null,"F","1996-02-02"]"""
    test(0,    8, wrap = true) shouldBe "[]"
    test(1,    8, wrap = true) shouldBe """[[1,"John","Doe","M","1969-01-01"]]"""
  }

  it should "serialize hierarchical tresql result as arrays to json" in {
    serializeTresqlResult(
      s"person {id, name, |account[id < 2] {number, balance, last_modified}, sex}", Json
    ) shouldBe """[1,"John",[["X64",1001.01,"2021-12-21 00:55:55.0"]],"M"],[2,"Jane",[],"F"]"""

    serializeTresqlResult(
      "person {name, |account {number, balance}}", Json
    ) shouldBe """["John",[["X64",1001.01],["X94",2002.02]]],["Jane",[]]"""

    serializeTresqlResult(
      "person {|account {number, balance}}", Json
    ) shouldBe """[[["X64",1001.01],["X94",2002.02]]],[[]]"""
  }
}
