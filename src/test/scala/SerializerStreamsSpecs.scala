package org.wabase

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source, StreamConverters}
import akka.util.ByteString
import org.apache.commons.codec.binary.Hex
import io.bullet.borer.{Cbor, Json, Target}
import java.io.{ByteArrayInputStream, InputStream, OutputStream, OutputStreamWriter}
import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.scalatest.matchers.should.Matchers
import org.wabase.ResultEncoder.{ChunkType, EncoderFactory}

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import org.tresql._


class SerializerStreamsSpecs extends FlatSpec with Matchers with TestQuereaseInitializer {

  implicit protected var tresqlResources: Resources = _
  implicit val system = ActorSystem("serializer-streams-specs")
  implicit val executor = system.dispatcher
  import SerializerStreamsSpecsDtos._

  override def beforeAll(): Unit = {
    querease = new TestQuerease("/serializer-streams-specs-metadata.yaml") {
      override lazy val viewNameToClassMap = viewNameToClass
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
    val source = TresqlResultSerializer.source(
      () => Query(query), includeHeaders, bufferSizeHint, BorerNestedArraysEncoder(_, format, wrap)
    )
    Await.result(source.runWith(foldToStringSink(format)), 1.second)
  }

  def serializeAndTransform(
      serializerSource: Source[ByteString, _],
      createEncoder:    EncoderFactory,
      bufferSizeHint:   Int,
    ) = {
    val source = BorerNestedArraysTransformer.source(
      () => serializerSource.runWith(StreamConverters.asInputStream()),
      createEncoder,
      bufferSizeHint = bufferSizeHint,
    )
    Await.result(source.runWith(foldToStringSink()), 1.second)
  }

  def serializeDtoResult(dtos: Seq[Dto], format: Target, includeHeaders: Boolean = false, wrap: Boolean = false) = {
    implicit val qe = querease
    val source = DtoDataSerializer.source(() =>
      dtos.iterator, includeHeaders, createEncoder = BorerNestedArraysEncoder(_, format, wrap))
    Await.result(source.runWith(foldToStringSink(format)), 1.second)
  }

  def serializeValuesToString(values: Iterator[_], format: Target = Cbor, bufferSizeHint: Int = 8) = {
    val source = ResultSerializer.source(
      () => values,
      outputStream => new BorerNestedArraysEncoder(BorerNestedArraysEncoder.createWriter(outputStream, format)),
      bufferSizeHint,
    )
    Await.result(source.runWith(foldToStringSink(format)), 1.second)
  }

  def serializeValuesToHexString(values: Iterator[_], format: Target = Cbor, bufferSizeHint: Int = 8) = {
    val source = ResultSerializer.source(
      () => values,
      outputStream => new BorerNestedArraysEncoder(BorerNestedArraysEncoder.createWriter(outputStream, format)),
      bufferSizeHint,
    )
    Await.result(source.runWith(foldToHexString(format)), 1.second)
  }

  def createPersonDtos: (Person, PersonAccountsDetails) = {
    val dto1 = new PersonAccounts
    dto1.number = "42"
    dto1.balance = 1001.01
    dto1.last_modified = java.sql.Timestamp.valueOf("2021-12-26 23:57:00.1")
    val dto2 = new PersonAccounts
    dto2.id = 2
    dto2.balance = 2002.02
    dto2.last_modified = java.sql.Timestamp.valueOf("2021-12-26 23:58:15.151")
    val person = new Person
    person.id = 0
    person.name = "John"
    person.surname = "Doe"
    person.accounts = List(dto1, dto2)
    val person_a = new PersonAccountsDetails
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
      s"person {id, name, |account[id < 2] {number, balance, cast(last_modified, 'time')}, sex}", Json
    ) shouldBe """[1,"John",[["X64",1001.01,"00:55:55"]],"M"],[2,"Jane",[],"F"]"""

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
      serializeTresqlResult(queryString(maxId), Cbor, wrap = wrap, bufferSizeHint = 12)
    test(0) shouldBe ""
    test(1) shouldBe """~9f~01dJohncDoeaMj1969-01-01~ff"""
    test(2) shouldBe """~9f~01dJohncDoeaMj1969-01-01~ff~9f~02dJane~f6aFj1996-02-02~ff"""
    test(0, wrap = true) shouldBe "~9f~ff"
    test(1, wrap = true) shouldBe """~9f~9f~01dJohncDoeaMj1969-01-01~ff~ff"""
  }

  it should "serialize tresql to cbor and transform to csv" in {
    val cols = "id, name, surname, sex, birthdate"
    def queryString(maxId: Int) = s"person [id <= $maxId] {${cols}}"
    def test(
      maxId: Int,
      withLabels: Boolean,
      includeHeaders: Boolean = false,
      bufferSizeHint: Int = 8
    ) = serializeAndTransform(
      TresqlResultSerializer.source(
        () => Query(queryString(maxId)),
        includeHeaders = includeHeaders,
        bufferSizeHint = bufferSizeHint
      ),
      outputStream => new FlatTableResultRenderer(
        renderer  = new CsvResultRenderer(new OutputStreamWriter(outputStream, "UTF-8")),
        labels    = if (withLabels) cols.replace("birthdate", "birth date").split(", ").toList else null,
        viewName  = null,
        nameToViewDef = null,
        hasHeaders = includeHeaders,
      ),
      bufferSizeHint = bufferSizeHint,
    )
    test(0, false) shouldBe ""
    test(1, false) shouldBe "1,John,Doe,M,1969-01-01\n"
    test(1, true ) shouldBe List(
      "id,name,surname,sex,birth date",
      "1,John,Doe,M,1969-01-01",
    ).mkString("", "\n", "\n")
    test(2, true ) shouldBe List(
      "id,name,surname,sex,birth date",
      "1,John,Doe,M,1969-01-01",
      "2,Jane,,F,1996-02-02",
    ).mkString("", "\n", "\n")
    test(2, false, true) shouldBe List(
      "id,name,surname,sex,birthdate",
      "1,John,Doe,M,1969-01-01",
      "2,Jane,,F,1996-02-02",
    ).mkString("", "\n", "\n")
    test(2, true, true) shouldBe List(
      "id,name,surname,sex,birth date",
      "1,John,Doe,M,1969-01-01",
      "2,Jane,,F,1996-02-02",
    ).mkString("", "\n", "\n")
  }

  it should "serialize dtos to cbor and reformat to json maps" in {
    implicit val qe = querease
    def test(dtos: Seq[Dto], isCollection: Boolean, viewName: String = null, bufferSizeHint: Int = 256) =
      serializeAndTransform(
        DtoDataSerializer.source(() => dtos.iterator, bufferSizeHint = bufferSizeHint),
        outputStream => JsonResultRenderer(outputStream, isCollection, viewName, qe.nameToViewDef),
        bufferSizeHint = bufferSizeHint,
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
    // buffer overflow test - key
    person_a.accounts = Nil
    test(List(person_a), isCollection = false, viewName = "person_accounts_details", bufferSizeHint = 8)  shouldBe List(
      """{"id":0,"name":"John","surname":"Doe","main_account":""",
      """{"id":null,"number":"42","balance":1001.01,"last_modified":"2021-12-26 23:57:00.1"},""",
      """"accounts":[],""",
      """"balances":["1001.01","2002.02"]}""",
    ).mkString
    // buffer overflow test - value
    person_a.name = "John-0123456789abcdef"
    test(List(person_a), isCollection = false, viewName = "person_accounts_details", bufferSizeHint = 20)  shouldBe List(
      """{"id":0,"name":"John-0123456789abcdef","surname":"Doe","main_account":""",
      """{"id":null,"number":"42","balance":1001.01,"last_modified":"2021-12-26 23:57:00.1"},""",
      """"accounts":[],""",
      """"balances":["1001.01","2002.02"]}""",
    ).mkString
  }

  it should "serialize tresql to cbor and reformat to csv - only top level fields in view" in {
    implicit val qe = querease
    def test(
      viewName: String,
      bufferSizeHint: Int = 256,
    ) = serializeAndTransform(
      TresqlResultSerializer.source(
        () => Query("person {id, name, surname, sex, birthdate, |account {number, balance} accounts}"),
      ),
      outputStream => new FlatTableResultRenderer(
        renderer  = new CsvResultRenderer(new OutputStreamWriter(outputStream, "UTF-8")),
        labels    = null,
        viewName  = viewName,
        nameToViewDef = qe.nameToViewDef,
      ) { override def label(name: String) = name },
      bufferSizeHint = bufferSizeHint,
    )
    test(null) shouldBe List(
      "id,name,surname,sex,birthdate,accounts",
      "1,John,Doe,M,1969-01-01,",
      "2,Jane,,F,1996-02-02,",
    ).mkString("", "\n", "\n")
    test("person_simple") shouldBe List(
      "id,name,surname,sex,birthdate",
      "1,John,Doe,M,1969-01-01",
      "2,Jane,,F,1996-02-02",
    ).mkString("", "\n", "\n")
    test("person_accounts_details") shouldBe List(
      "id,name,surname",
      "1,John,Doe",
      "2,Jane,",
    ).mkString("", "\n", "\n")
  }

  it should "serialize tresql to cbor and reformat to json maps - only fields in view" in {
    implicit val qe = querease
    def test(
      viewName: String,
      bufferSizeHint: Int = 256,
    ) = serializeAndTransform(
      TresqlResultSerializer.source(
        () => Query("person {id, name, surname, sex, birthdate, |account {number, balance} accounts}"),
      ),
      outputStream => JsonResultRenderer(outputStream, isCollection = true, viewName, qe.nameToViewDef),
      bufferSizeHint = bufferSizeHint,
    )
    test(null) shouldBe List(
      """{"id":1,"name":"John","surname":"Doe","sex":"M","birthdate":"1969-01-01","accounts":""" +
        """[{"number":"X64","balance":1001.01},{"number":"X94","balance":2002.02}]}""",
      """{"id":2,"name":"Jane","surname":null,"sex":"F","birthdate":"1996-02-02","accounts":[]}""",
    ).mkString("[", ",", "]")
    test("person_simple") shouldBe List(
      """{"id":1,"name":"John","surname":"Doe","sex":"M","birthdate":"1969-01-01"}""",
      """{"id":2,"name":"Jane","surname":null,"sex":"F","birthdate":"1996-02-02"}""",
    ).mkString("[", ",", "]")
    test("person_accounts_details") shouldBe List(
      """{"id":1,"name":"John","surname":"Doe","accounts":[{"number":"X64","balance":1001.01},{"number":"X94","balance":2002.02}]}""",
      """{"id":2,"name":"Jane","surname":null,"accounts":[]}""",
    ).mkString("[", ",", "]")
  }

  it should "serialize tresql to cbor and reformat to json maps - with or without headers" in {
    implicit val qe = querease
    val queryString = qe.queryStringAndParams(qe.viewDef("person_accounts_details"), Map.empty)._1
    def test(
      viewName: String,
      includeHeaders: Boolean,
      bufferSizeHint: Int = 256,
    ) = serializeAndTransform(
      TresqlResultSerializer.source(
        () => Query(queryString),
        includeHeaders = includeHeaders,
      ),
      outputStream => new CborOrJsonResultRenderer(
          BorerNestedArraysEncoder.createWriter(outputStream, Json),
          isCollection = true, viewName, qe.nameToViewDef, hasHeaders = includeHeaders),
      bufferSizeHint = bufferSizeHint,
    )
    val expected = List(
      """{"id":1,"name":"John","surname":"Doe","main_account":null,"accounts":[""" +
        """{"id":1,"number":"X64","balance":1001.01,"last_modified":"2021-12-21 00:55:55.0"},""" +
        """{"id":2,"number":"X94","balance":2002.02,"last_modified":"2021-12-21 01:59:30.0"}]}""",
      """{"id":2,"name":"Jane","surname":null,"main_account":null,"accounts":[]}""",
    ).mkString("[", ",", "]")
    test("person_accounts_details", false) shouldBe expected
    test("person_accounts_details", true)  shouldBe expected
  }

  it should "serialize tresql to cbor and transform to csv - with or without headers" in {
    implicit val qe = querease
    val queryString = qe.queryStringAndParams(qe.viewDef("person_accounts_details"), Map.empty)._1
    def test(
      viewName: String,
      includeHeaders: Boolean,
      bufferSizeHint: Int = 256,
    ) = serializeAndTransform(
      TresqlResultSerializer.source(
        () => Query(queryString),
        includeHeaders = includeHeaders,
      ),
      outputStream => new FlatTableResultRenderer(
        renderer = new CsvResultRenderer(new OutputStreamWriter(outputStream, "UTF-8")),
        viewName, qe.nameToViewDef, hasHeaders = includeHeaders
      ) { override def label(name: String) = name },
      bufferSizeHint = bufferSizeHint,
    )
    val expected = List(
      "id,name,surname",
      "1,John,Doe",
      "2,Jane,",
    ).mkString("", "\n", "\n")
    test("person_accounts_details", false) shouldBe expected
    test("person_accounts_details", true)  shouldBe expected
  }

  it should "serialize tresql to cbor and transform to csv with labels - with or without headers" in {
    implicit val qe = querease
    def test(
      viewName: String,
      includeHeaders: Boolean,
      bufferSizeHint: Int = 256,
    ) = serializeAndTransform(
      TresqlResultSerializer.source(
        () => Query(qe.queryStringAndParams(qe.viewDef(viewName), Map.empty)._1),
        includeHeaders = includeHeaders,
      ),
      outputStream => new FlatTableResultRenderer(
        renderer = new CsvResultRenderer(new OutputStreamWriter(outputStream, "UTF-8")),
        viewName, qe.nameToViewDef, hasHeaders = includeHeaders
      ),
      bufferSizeHint = bufferSizeHint,
    )
    val expected = List(
      "Id,Name,Surname,Sex,Birthdate,Main account",
      "1,John,Doe,M,1969-01-01,",
      "2,Jane,,F,1996-02-02,",
    ).mkString("", "\n", "\n")
    test("person", false) shouldBe expected
    test("person", true)  shouldBe expected
    test("person_with_expression", true)  shouldBe List(
      "Id,Name,Surname",
      "1,John,Doe",
      "2,Jane,",
    ).mkString("", "\n", "\n")
  }

  it should "invoke all table result renderer methods properly" in {
    implicit val qe = querease
    class TestTableRenderer(writer: java.io.Writer) extends TableResultRenderer {
      def cellString(value: Any) = String.format("[%1$8s ] ", "" + Option(value).getOrElse("<null>"))
      override def renderHeader()                =
        writer.write("-- |---------| |---------| |---------| --\n")
      override def renderRowStart()              = writer.write("#> ")
      override def renderHeaderCell(value: Any)  = writer.write(cellString(value).toUpperCase)
      override def renderCell(value: Any)        = writer.write(cellString(value))
      override def renderRowEnd()                = writer.write("<#\n")
      override def renderFooter()                = {
        writer.write("-----------------------------------------\n")
        writer.flush
      }
    }
    val queryString = qe.queryStringAndParams(qe.viewDef("person_accounts_details"), Map.empty)._1
    def test(
      viewName: String,
      includeHeaders: Boolean = true,
      bufferSizeHint: Int = 256,
    ) = serializeAndTransform(
      TresqlResultSerializer.source(
        () => Query(queryString),
        includeHeaders = includeHeaders,
      ),
      outputStream => new FlatTableResultRenderer(
        renderer = new TestTableRenderer(new OutputStreamWriter(outputStream, "UTF-8")),
        viewName, qe.nameToViewDef, hasHeaders = includeHeaders),
      bufferSizeHint = bufferSizeHint,
    )
    test("person_accounts_details")  shouldBe List(
      "-- |---------| |---------| |---------| --",
      "#> [      ID ] [    NAME ] [ SURNAME ] <#",
      "#> [       1 ] [    John ] [     Doe ] <#",
      "#> [       2 ] [    Jane ] [  <null> ] <#",
      "-----------------------------------------",
    ).mkString("", "\n", "\n")
  }

  it should "serialize known types to cbor and deserialize to somewhat similar types" in {
    import scala.language.existentials
    def test(value: Any, bufferSizeHint: Int = 256) = {
      var deserialized: Any = null
      val handler = new ResultRenderer(isCollection = false, viewName = null, nameToViewDef = null, hasHeaders = false) {
        override def renderValue(value: Any): Unit = {}
        override def writeValue(value: Any): Unit = { deserialized = value }
      }
      val serialized  = serializeValuesToHexString(List(value).iterator, bufferSizeHint = bufferSizeHint)
      val transformer = new BorerNestedArraysTransformer(
        Cbor.reader(new ByteArrayInputStream(Hex.decodeHex(serialized))), handler
      )
      handler.writeStartOfInput()
      while (transformer.transformNext()) {}
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
    test("Rūķīši", 2)     shouldBe (classOf[java.lang.String], "Rūķīši")
    test("Rūķīši", 3)     shouldBe (classOf[java.lang.String], "Rūķīši")
    test("Rūķīši", 4)     shouldBe (classOf[java.lang.String], "Rūķīši")
    test("Rūķīši", 5)     shouldBe (classOf[java.lang.String], "Rūķīši")
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
    (test("Rūķīši".getBytes("UTF-8"), 2)._2 match {
      case ba: Array[Byte] =>
        new String(ba, "UTF-8")
    })                                  shouldBe  "Rūķīši"
    test(java.sql.Date.valueOf("1969-01-01")) shouldBe (classOf[java.sql.Date], java.sql.Date.valueOf("1969-01-01"))
    test(java.sql.Date.valueOf("1971-01-01")) shouldBe (classOf[java.sql.Date], java.sql.Date.valueOf("1971-01-01"))
    test(java.sql.Time.valueOf("12:34:55")) shouldBe
      (classOf[java.sql.Timestamp], java.sql.Timestamp.valueOf("1970-01-01 12:34:55"))
    test(java.sql.Timestamp.valueOf("1969-01-01 00:00:00.0")) shouldBe
      (classOf[java.sql.Timestamp], java.sql.Timestamp.valueOf("1969-01-01 00:00:00.0"))
    test(java.sql.Timestamp.valueOf("1969-01-01 00:00:00.001")) shouldBe
      (classOf[java.sql.Timestamp], java.sql.Timestamp.valueOf("1969-01-01 00:00:00.001"))
    test(java.sql.Timestamp.valueOf("1971-01-01 00:00:00.001")) shouldBe
      (classOf[java.sql.Timestamp], java.sql.Timestamp.valueOf("1971-01-01 00:00:00.001"))
    test(java.sql.Date.valueOf("1969-01-01").toLocalDate) shouldBe (classOf[java.sql.Date], java.sql.Date.valueOf("1969-01-01"))
    test(java.sql.Date.valueOf("1971-01-01").toLocalDate) shouldBe (classOf[java.sql.Date], java.sql.Date.valueOf("1971-01-01"))
    test(java.sql.Time.valueOf("12:34:55").toLocalTime) shouldBe
      (classOf[java.sql.Timestamp], java.sql.Timestamp.valueOf("1970-01-01 12:34:55"))
    test(java.sql.Timestamp.valueOf("1969-01-01 00:00:00.0").toLocalDateTime) shouldBe
      (classOf[java.sql.Timestamp], java.sql.Timestamp.valueOf("1969-01-01 00:00:00.0"))
    test(java.sql.Timestamp.valueOf("1969-01-01 00:00:00.001").toLocalDateTime) shouldBe
      (classOf[java.sql.Timestamp], java.sql.Timestamp.valueOf("1969-01-01 00:00:00.001"))
    test(java.sql.Timestamp.valueOf("1971-01-01 00:00:00.001").toLocalDateTime) shouldBe
      (classOf[java.sql.Timestamp], java.sql.Timestamp.valueOf("1971-01-01 00:00:00.001"))
  }

  it should "chunk strings according to buffer size when serializing to cbor" in {
    import scala.language.existentials
    def test(value: String, bufferSizeHint: Int) =
      serializeValuesToString(List(value).iterator, bufferSizeHint = bufferSizeHint)
    test("Rūķīši",  2) shouldBe "~7faRa~c5a~aba~c4a~b7a~c4a~aba~c5a~a1ai~ff"
    test("Rūķīši",  3) shouldBe "~7fbR~c5b~ab~c4b~b7~c4b~ab~c5b~a1i~ff"
    test("Rūķīši",  4) shouldBe "~7fcR~c5~abc~c4~b7~c4c~ab~c5~a1ai~ff"
    test("Rūķīši",  5) shouldBe "~7fdR~c5~ab~c4d~b7~c4~ab~c5b~a1i~ff"
    test("Rūķīši", 10) shouldBe "~7fiR~c5~ab~c4~b7~c4~ab~c5~a1ai~ff"
    test("Rūķīši", 11) shouldBe "jR~c5~ab~c4~b7~c4~ab~c5~a1i"
    test("12345678901234567890123",  23) shouldBe "~7fv1234567890123456789012a3~ff"
    test("12345678901234567890123",  24) shouldBe "w12345678901234567890123"
    test("123456789012345678901234", 24) shouldBe "~7fw12345678901234567890123a4~ff"
    test("123456789012345678901234", 25) shouldBe "~7fw12345678901234567890123a4~ff"
    test("123456789012345678901234", 26) shouldBe "x~18123456789012345678901234"
  }

  it should "chunk byte arrays according to buffer size when serializing to cbor" in {
    import scala.language.existentials
    def test(value: Array[Byte], bufferSizeHint: Int) =
      serializeValuesToString(List(value).iterator, bufferSizeHint = bufferSizeHint)
    test("Rūķīši".getBytes("UTF-8"),  2) shouldBe "_ARA~c5A~abA~c4A~b7A~c4A~abA~c5A~a1Ai~ff"
    test("Rūķīši".getBytes("UTF-8"),  3) shouldBe "_BR~c5B~ab~c4B~b7~c4B~ab~c5B~a1i~ff"
    test("Rūķīši".getBytes("UTF-8"), 11) shouldBe "JR~c5~ab~c4~b7~c4~ab~c5~a1i"
  }

  it should "encode byte arrays to text formats" in {
    implicit val qe = querease
    def createCsvResultRenderer(os: OutputStream) =
      new FlatTableResultRenderer(new CsvResultRenderer(new OutputStreamWriter(os, "UTF-8")),
        viewName = null, qe.nameToViewDef)
    def createJsonResultRenderer(os: OutputStream) =
      JsonResultRenderer(os, isCollection = true, viewName = null, qe.nameToViewDef)
    def test(dtos: Seq[Dto], rendererFactory: OutputStream => ResultRenderer, bufferSizeHint: Int = 256) =
      serializeAndTransform(
        DtoDataSerializer.source(() => dtos.iterator, bufferSizeHint = bufferSizeHint),
        rendererFactory,
        bufferSizeHint = bufferSizeHint,
      )
    val dto = new BytesTest
    dto.id    = 1
    dto.name  = "John"
    dto.bytes = "Rūķīši".getBytes("UTF-8")
    val expectedCsv = List(
      "id,name,bytes",
      "1,John,UsWrxLfEq8WhaQ==",
    ).mkString("", "\n", "\n")
    val expectedJson =
      """[{"id":1,"name":"John","bytes":"UsWrxLfEq8WhaQ=="}]"""
    test(Seq(dto), createCsvResultRenderer,   2) shouldBe expectedCsv
    test(Seq(dto), createCsvResultRenderer,  22) shouldBe expectedCsv
    test(Seq(dto), createJsonResultRenderer,  2) shouldBe expectedJson
    test(Seq(dto), createJsonResultRenderer, 22) shouldBe expectedJson
  }
}

object SerializerStreamsSpecsDtos {
  class Person extends DtoWithId {
    var id: java.lang.Long = null
    var name: String = null
    var surname: String = null
    var sex: String = null
    var birthdate: java.sql.Date = null
    var main_account: String = null
    var accounts: List[PersonAccounts] = Nil
  }
  class PersonAccounts extends DtoWithId {
    var id: java.lang.Long = null
    var number: String = null
    var balance: BigDecimal = null
    var last_modified: java.sql.Timestamp = null
  }
  class PersonAccountsDetails extends DtoWithId {
    var id: java.lang.Long = null
    var name: String = null
    var surname: String = null
    var main_account: PersonAccounts = null
    var accounts: List[PersonAccounts] = Nil
    var balances: List[String] = null
  }
  class PersonWithMainAccount extends DtoWithId {
    var id: java.lang.Long = null
  }
  class PersonSimple extends DtoWithId {
    var id: java.lang.Long = null
    var name: String = null
    var sex: String = null
    var birthdate: java.sql.Date = null
  }
  class BytesTest extends DtoWithId {
    var id: java.lang.Long = null
    var name: String = null
    var bytes: Array[Byte] = null
  }

  val viewNameToClass = Map[String, Class[_ <: Dto]](
    "person" -> classOf[Person],
    "person_accounts" -> classOf[PersonAccounts],
    "person_accounts_details" -> classOf[PersonAccountsDetails],
    "person_with_main_account" -> classOf[PersonWithMainAccount],
    "person_simple" -> classOf[PersonSimple],
    "bytes_test" -> classOf[BytesTest],
  )
}
