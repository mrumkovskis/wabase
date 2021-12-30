package org.wabase

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source, StreamConverters}
import akka.util.ByteString
import org.apache.commons.codec.binary.Hex
import io.bullet.borer.{Cbor, Json, Target}
import java.io.{ByteArrayInputStream, InputStream, OutputStream, OutputStreamWriter}
import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import org.tresql._

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

  def foldToStringSink(format: Target = null) =
    Sink.fold[String, ByteString]("") { case (acc, str) =>
      acc + (format match {
        case _: Cbor.type => str.toVector
          .map(b => if (b < ' ' || b >= '~') String.format("~%02x", Byte.box(b)) else b.toChar.toString).mkString
        case _ => str.decodeString("UTF-8")
      })
    }

  def foldToHexString(format: Target) =
    Sink.fold[String, ByteString]("") { case (acc, str) =>
      acc + (format match {
        case _: Cbor.type => str.toVector
          .map(b => String.format("%02x", Byte.box(b))).mkString
        case _: Json.type => str.decodeString("UTF-8")
      })
    }

  def serializeTresqlResult(
    query: String, format: Target, includeHeaders: Boolean = false,
    bufferSizeHint: Int = 8, wrap: Boolean = false,
  ) = {
    val source = TresqlResultSerializer(
      () => Query(query), includeHeaders, bufferSizeHint, BorerNestedArraysEncoder(_, format, wrap)
    )
    Await.result(source.runWith(foldToStringSink(format)), 1.second)
  }

  def serializeAndTransform(
      serializerSource: Source[ByteString, _], createEncoder: OutputStream => NestedArraysHandler) = {
    val source = BorerNestedArraysTransformer(
      () => serializerSource.runWith(StreamConverters.asInputStream()),
      createEncoder,
    )
    Await.result(source.runWith(foldToStringSink()), 1.second)
  }

  def serializeDtoResult(dtos: Seq[Dto], format: Target, includeHeaders: Boolean = false, wrap: Boolean = false) = {
    implicit val qe = querease
    val source = DtoDataSerializer(() =>
      dtos.iterator, includeHeaders, createEncoder = BorerNestedArraysEncoder(_, format, wrap))
    Await.result(source.runWith(foldToStringSink(format)), 1.second)
  }

  def serializeValuesToHexString(values: Iterator[_], format: Target = Cbor, bufferSizeHint: Int = 8) = {
    val source = Source.fromGraph(new NestedArraysSerializer(
      () => values,
      outputStream => new BorerNestedArraysEncoder(BorerNestedArraysEncoder.createWriter(outputStream, format)),
      bufferSizeHint,
    ))
    Await.result(source.runWith(foldToHexString(format)), 1.second)
  }

  def createPersonDtos: (QuereaseActionsDtos.Person, QuereaseActionsDtos.PersonAccountsDetails) = {
    val dto1 = new QuereaseActionsDtos.PersonAccounts
    dto1.number = "42"
    dto1.balance = 1001.01
    dto1.last_modified = java.sql.Timestamp.valueOf("2021-12-26 23:57:00.1")
    val dto2 = new QuereaseActionsDtos.PersonAccounts
    dto2.id = 2
    dto2.balance = 2002.02
    dto2.last_modified = java.sql.Timestamp.valueOf("2021-12-26 23:58:15.151")
    val person = new QuereaseActionsDtos.Person
    person.id = 0
    person.name = "John"
    person.surname = "Doe"
    person.accounts = List(dto1, dto2)
    val person_a = new QuereaseActionsDtos.PersonAccountsDetails
    person_a.id = 0
    person_a.name = "John"
    person_a.surname = "Doe"
    person_a.main_account = dto1
    person_a.accounts = List(dto1, dto2)
    person_a.balances = person_a.accounts.map(_.balance.toString)
    (person, person_a)
  }

  it should "serialize flat tresql result as arrays to json" in {
    def queryString(maxId: Int) = s"person [id <= $maxId] {id, name, surname, sex, birthdate}"
    def test(maxId: Int, bufferSizeHint: Int, includeHeaders: Boolean = false, wrap: Boolean = false) =
      serializeTresqlResult(queryString(maxId), Json, includeHeaders, bufferSizeHint, wrap)
    test(0,    8) shouldBe ""
    test(1,    8) shouldBe """[1,"John","Doe","M","1969-01-01"]"""
    test(1, 1024) shouldBe """[1,"John","Doe","M","1969-01-01"]"""
    test(2,    8) shouldBe """[1,"John","Doe","M","1969-01-01"],[2,"Jane",null,"F","1996-02-02"]"""
    test(2, 1024) shouldBe """[1,"John","Doe","M","1969-01-01"],[2,"Jane",null,"F","1996-02-02"]"""
    test(0,    8, wrap = true) shouldBe "[]"
    test(1,    8, wrap = true) shouldBe """[[1,"John","Doe","M","1969-01-01"]]"""
    test(0,    8, includeHeaders = true) shouldBe ""
    test(1,    8, includeHeaders = true) shouldBe List(
      """["id","name","surname","sex","birthdate"]""",
      """[1,"John","Doe","M","1969-01-01"]""",
    ).mkString(",")
  }

  it should "serialize dto result as arrays to json" in {
    def test(dtos: Seq[Dto], includeHeaders: Boolean) =
      serializeDtoResult(dtos, Json, includeHeaders)
    val (person, person_a) = createPersonDtos
    val dto1 = person.accounts(0)
    val dto2 = person.accounts(1)
    test(Nil, includeHeaders = false) shouldBe ""
    test(Nil, includeHeaders = true)  shouldBe ""
    test(List(dto1, dto2), includeHeaders = false) shouldBe List(
      """[null,"42",1001.01,"2021-12-26 23:57:00.1"]""",
      """[2,null,2002.02,"2021-12-26 23:58:15.151"]""",
    ).mkString(",")
    test(List(dto1, dto2), includeHeaders = true)  shouldBe List(
      """["id","number","balance","last_modified"]""",
      """[null,"42",1001.01,"2021-12-26 23:57:00.1"]""",
      """[2,null,2002.02,"2021-12-26 23:58:15.151"]""",
    ).mkString(",")
    test(List(person), includeHeaders = false)  shouldBe List(
      """[0,"John","Doe",null,null,null,""",
        """[[null,"42",1001.01,"2021-12-26 23:57:00.1"],""",
        """[2,null,2002.02,"2021-12-26 23:58:15.151"]]]""",
    ).mkString
    test(List(person), includeHeaders = true)  shouldBe List(
      """["id","name","surname","sex","birthdate","main_account","accounts"],""",
      """[0,"John","Doe",null,null,null,""",
      """[["id","number","balance","last_modified"],""",
       """[null,"42",1001.01,"2021-12-26 23:57:00.1"],""",
       """[2,null,2002.02,"2021-12-26 23:58:15.151"]]]""",
    ).mkString
    test(List(person_a), includeHeaders = true)  shouldBe List(
      """["id","name","surname","main_account","accounts","balances"],""",
      """[0,"John","Doe",""",
      """[["id","number","balance","last_modified"],""",
       """[null,"42",1001.01,"2021-12-26 23:57:00.1"]],""",
      """[["id","number","balance","last_modified"],""",
       """[null,"42",1001.01,"2021-12-26 23:57:00.1"],""",
       """[2,null,2002.02,"2021-12-26 23:58:15.151"]],""",
       """["1001.01","2002.02"]]""",
    ).mkString
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
    serializeTresqlResult(
      s"person {id, name, |account[id < 2] {number, balance, last_modified}, sex}", Json, includeHeaders = true
    ) shouldBe List(
      """["id","name",null,"sex"]""",
      """[1,"John",[["number","balance","last_modified"],["X64",1001.01,"2021-12-21 00:55:55.0"]],"M"]""",
      """[2,"Jane",[],"F"]""",
    ).mkString(",")

    serializeTresqlResult(
      "person {name, |account {number, balance}}", Json, includeHeaders = true
    ) shouldBe """["name",null],["John",[["number","balance"],["X64",1001.01],["X94",2002.02]]],["Jane",[]]"""

    serializeTresqlResult(
      "person {|account {number, balance}}", Json, includeHeaders = true
    ) shouldBe """[null],[[["number","balance"],["X64",1001.01],["X94",2002.02]]],[[]]"""
    serializeTresqlResult(
      "person {|account {number, balance} account}", Json, includeHeaders = true
    ) shouldBe """["account"],[[["number","balance"],["X64",1001.01],["X94",2002.02]]],[[]]"""
  }

  it should "serialize tresql result as arrays to cbor" in {
    def queryString(maxId: Int) = s"person [id <= $maxId] {id, name, surname, sex, birthdate || ''}"
    def test(maxId: Int, wrap: Boolean = false) =
      serializeTresqlResult(queryString(maxId), Cbor, wrap = wrap)
    test(0) shouldBe ""
    test(1) shouldBe """~9f~01dJohncDoeaMj1969-01-01~ff"""
    test(2) shouldBe """~9f~01dJohncDoeaMj1969-01-01~ff~9f~02dJane~f6aFj1996-02-02~ff"""
    test(0, wrap = true) shouldBe "~9f~ff"
    test(1, wrap = true) shouldBe """~9f~9f~01dJohncDoeaMj1969-01-01~ff~ff"""
  }

  it should "serialize tresql to cbor and transform to csv" in {
    val cols = "id, name, surname, sex, birthdate"
    def queryString(maxId: Int) = s"person [id <= $maxId] {${cols}}"
    def test(maxId: Int, withLabels: Boolean) = serializeAndTransform(
      TresqlResultSerializer(() => Query(queryString(maxId)), includeHeaders = false),
      outputStream => new CsvOutput(
        writer = new OutputStreamWriter(outputStream, "UTF-8"),
        labels = if (withLabels) cols.split(", ").toList else null,
      )
    )
    test(0, false) shouldBe ""
    test(1, false) shouldBe "1,John,Doe,M,1969-01-01\n"
    test(1, true ) shouldBe List(
      "id,name,surname,sex,birthdate",
      "1,John,Doe,M,1969-01-01",
    ).mkString("", "\n", "\n")
    test(2, true ) shouldBe List(
      "id,name,surname,sex,birthdate",
      "1,John,Doe,M,1969-01-01",
      "2,Jane,,F,1996-02-02",
    ).mkString("", "\n", "\n")
  }

  it should "serialize dtos to cbor and reformat to json maps" in {
    implicit val qe = querease
    def test(dtos: Seq[Dto], isCollection: Boolean, viewName: String = null) = serializeAndTransform(
      DtoDataSerializer(() => dtos.iterator),
      outputStream => JsonOutput(outputStream, isCollection, viewName, qe.nameToViewDef)
    )
    val (person, person_a) = createPersonDtos
    val dto1 = person.accounts(0)
    val dto2 = person.accounts(1)
    test(Nil, isCollection = false)  shouldBe ""
    test(Nil, isCollection = true)  shouldBe "[]"
    test(List(dto1, dto2), isCollection = true)  shouldBe List(
      """[{"id":null,"number":"42","balance":1001.01,"last_modified":"2021-12-26 23:57:00.1"},""",
      """{"id":2,"number":null,"balance":2002.02,"last_modified":"2021-12-26 23:58:15.151"}]""",
    ).mkString
    test(List(person), isCollection = false)  shouldBe List(
      """{"id":0,"name":"John","surname":"Doe","sex":null,"birthdate":null,"main_account":null,"accounts":""",
      """[{"id":null,"number":"42","balance":1001.01,"last_modified":"2021-12-26 23:57:00.1"},""",
      """{"id":2,"number":null,"balance":2002.02,"last_modified":"2021-12-26 23:58:15.151"}]}""",
    ).mkString
    test(List(person, person), isCollection = true)  shouldBe List(
      """[{"id":0,"name":"John","surname":"Doe","sex":null,"birthdate":null,"main_account":null,"accounts":""",
      """[{"id":null,"number":"42","balance":1001.01,"last_modified":"2021-12-26 23:57:00.1"},""",
      """{"id":2,"number":null,"balance":2002.02,"last_modified":"2021-12-26 23:58:15.151"}]},""",
      """{"id":0,"name":"John","surname":"Doe","sex":null,"birthdate":null,"main_account":null,"accounts":""",
      """[{"id":null,"number":"42","balance":1001.01,"last_modified":"2021-12-26 23:57:00.1"},""",
      """{"id":2,"number":null,"balance":2002.02,"last_modified":"2021-12-26 23:58:15.151"}]}]""",
    ).mkString
    // unknown wiew - wrap single child in array
    test(List(person_a), isCollection = false)  shouldBe List(
      """{"id":0,"name":"John","surname":"Doe","main_account":""",
      """[{"id":null,"number":"42","balance":1001.01,"last_modified":"2021-12-26 23:57:00.1"}],""",
      """"accounts":""",
      """[{"id":null,"number":"42","balance":1001.01,"last_modified":"2021-12-26 23:57:00.1"},""",
      """{"id":2,"number":null,"balance":2002.02,"last_modified":"2021-12-26 23:58:15.151"}],""",
      """"balances":["1001.01","2002.02"]}""",
    ).mkString
    // known wiew - do not wrap single child in array
    test(List(person_a), isCollection = false, viewName = "person_accounts_details")  shouldBe List(
      """{"id":0,"name":"John","surname":"Doe","main_account":""",
      """{"id":null,"number":"42","balance":1001.01,"last_modified":"2021-12-26 23:57:00.1"},""",
      """"accounts":""",
      """[{"id":null,"number":"42","balance":1001.01,"last_modified":"2021-12-26 23:57:00.1"},""",
      """{"id":2,"number":null,"balance":2002.02,"last_modified":"2021-12-26 23:58:15.151"}],""",
      """"balances":["1001.01","2002.02"]}""",
    ).mkString
  }

  it should "serialize known types to cbor and deserialize to somewhat similar types" in {
    import scala.language.existentials
    def test(value: Any) = {
      var deserialized: Any = null
      val handler = new NestedArraysHandler {
        override def writeStartOfInput():    Unit = {}
        override def writeArrayStart():      Unit = {}
        override def writeValue(value: Any): Unit = { deserialized = value }
        override def writeArrayBreak():      Unit = {}
        override def writeEndOfInput():      Unit = {}
      }
      val serialized  = serializeValuesToHexString(List(value).iterator)
      val transformer = new BorerNestedArraysTransformer(
        Cbor.reader(new ByteArrayInputStream(Hex.decodeHex(serialized))), handler
      )
      transformer.transformNext()
      Option(deserialized)
        .map(d => (d.getClass, d))
        .getOrElse((null, deserialized))
    }
    test(null)            shouldBe (null, null)
    test(true)            shouldBe (classOf[java.lang.Boolean], true)
    test(false)           shouldBe (classOf[java.lang.Boolean], false)
    (test('A')._2 match {
      case i: java.lang.Integer => i.toChar
    })                    shouldBe 'A'
    test(42.toByte)       shouldBe (classOf[java.lang.Integer], 42)
    test(42.toShort)      shouldBe (classOf[java.lang.Integer], 42)
    test(-42)             shouldBe (classOf[java.lang.Integer], -42)
    test(42)              shouldBe (classOf[java.lang.Integer], 42)
    test(42L)             shouldBe (classOf[java.lang.Integer], 42)
    test(Long.MinValue)   shouldBe (classOf[java.lang.Long],    Long.MinValue)
    test(Long.MaxValue)   shouldBe (classOf[java.lang.Long],    Long.MaxValue)
    test(-1.0.toFloat)    shouldBe (classOf[java.lang.Float],   -1)
    test(1.0.toFloat)     shouldBe (classOf[java.lang.Float],   1)
    test(1.5.toFloat)     shouldBe (classOf[java.lang.Float],   1.5)
    test(-1.0.toDouble)   shouldBe (classOf[java.lang.Float],   -1)
    test(1.0.toDouble)    shouldBe (classOf[java.lang.Float],   1)
    test(Double.MaxValue) shouldBe (classOf[java.lang.Double],  Double.MaxValue)
    test(Double.MinValue) shouldBe (classOf[java.lang.Double],  Double.MinValue)
    test("")              shouldBe (classOf[java.lang.String], "")
    test("Rūķīši")        shouldBe (classOf[java.lang.String], "Rūķīši")
    test(BigInt(-1))      shouldBe (classOf[java.lang.Integer], -1)
    test(BigInt(1))       shouldBe (classOf[java.lang.Integer], 1)
    test(BigInt(Long.MinValue))         shouldBe (classOf[java.lang.Long], Long.MinValue)
    test(BigInt(Long.MaxValue))         shouldBe (classOf[java.lang.Long], Long.MaxValue)
    test(BigInt(Long.MinValue) - 1)     shouldBe (classOf[java.math.BigInteger], new java.math.BigInteger("-9223372036854775809"))
    test(BigInt(Long.MaxValue) + 1)     shouldBe (classOf[java.math.BigInteger], new java.math.BigInteger( "9223372036854775808"))
    test(BigDecimal(Long.MinValue, 2))  shouldBe (classOf[java.math.BigDecimal], new java.math.BigDecimal("-92233720368547758.08"))
    test(BigDecimal(Long.MaxValue, 2))  shouldBe (classOf[java.math.BigDecimal], new java.math.BigDecimal( "92233720368547758.07"))
    test("abc".getBytes("UTF-8"))._1    shouldBe  classOf[Array[Byte]]
    (test("abc".getBytes("UTF-8"))._2 match {
      case ba: Array[Byte] =>
        new String(ba, "UTF-8")
    })                                  shouldBe  "abc"
    test(java.sql.Date.valueOf("1969-01-01")) shouldBe (classOf[java.sql.Date], java.sql.Date.valueOf("1969-01-01"))
    test(java.sql.Date.valueOf("1971-01-01")) shouldBe (classOf[java.sql.Date], java.sql.Date.valueOf("1971-01-01"))
    test(java.sql.Timestamp.valueOf("1969-01-01 00:00:00.0")) shouldBe
      (classOf[java.sql.Timestamp], java.sql.Timestamp.valueOf("1969-01-01 00:00:00.0"))
    test(java.sql.Timestamp.valueOf("1969-01-01 00:00:00.001")) shouldBe
      (classOf[java.sql.Timestamp], java.sql.Timestamp.valueOf("1969-01-01 00:00:00.001"))
    test(java.sql.Timestamp.valueOf("1971-01-01 00:00:00.001")) shouldBe
      (classOf[java.sql.Timestamp], java.sql.Timestamp.valueOf("1971-01-01 00:00:00.001"))
  }
}
