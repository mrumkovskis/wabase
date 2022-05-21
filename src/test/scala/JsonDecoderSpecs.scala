package org.wabase

import akka.util.ByteString
import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.scalatest.matchers.should.Matchers
import spray.json._

import scala.collection.immutable.TreeMap


class JsonDecoderSpecs extends FlatSpec with Matchers {
  import JsonDecoderSpecs._
  implicit val qe = new TestQuerease("/json-decoder-specs-metadata.yaml") with JsonConverter
  import qe._
  val strictDecoder = new CborOrJsonDecoder(qe.typeDefs, qe.nameToViewDef)
  val lenientDecoder = new CborOrJsonLenientDecoder(qe.typeDefs, qe.nameToViewDef)
  def jsonRoundtrip(dto: qe.DTO) =
    decodeToMap(
      ByteString(dto.toMap.toJson.prettyPrint),
      classToViewName(dto.getClass),
    )
  def decodeToMap(bytes: ByteString, viewName: String, decoder: CborOrJsonDecoder = strictDecoder) =
    decoder.decodeToMap(bytes, viewName)(qe.viewNameToMapZero)
  def encodeBytes(bytes: Array[Byte]) = ByteString.fromArrayUnsafe(bytes).encodeBase64.utf8String
  def comparable(map: Map[String, Any]): Map[String, Any] = // scalatest does not compare bytes - convert to string
    map.updated("bytes",     Option(map("bytes")).map(_.asInstanceOf[Array[Byte]]).map(encodeBytes).orNull)
       .updated("bytes_seq", Option(map("bytes_seq")).map(_.asInstanceOf[List[Array[Byte]]].map(encodeBytes)).orNull)

  it should "decode json to compatible map ignoring unknown and adding missing keys" in {
    val obj = new decoder_test_child
    val viewName = classToViewName(obj.getClass)
    obj.toMap shouldBe decodeToMap(ByteString("{}"), viewName)
    obj.toMap shouldBe decodeToMap(ByteString("""{"x_str": "x", "x_obj": {}}"""), viewName)
    val jsonized = """{
      "id": null,
      "name": null,
      "date": null,
      "date_time": null
    }""".replaceAll("\n    ", "\n")
    obj.toMap.toJson.prettyPrint shouldBe jsonized
    decodeToMap(ByteString("{}"), viewName).toJson.prettyPrint shouldBe jsonized
  }

  it should "decode json to compatible map" in {

    // empty
    val obj = new decoder_test
    obj.toMap shouldBe jsonRoundtrip(obj)

    // strings and dates
    obj.string = "Rūķīši-X-123"
    obj.date = java.sql.Date.valueOf("2021-12-21")
    obj.date_time = java.sql.Timestamp.valueOf("2021-12-26 23:57:14.0")

    // negatives
    obj.id = Long.MinValue
    obj.int = Integer.MIN_VALUE
    obj.bigint = BigInt(Long.MinValue) - 1
    obj.double = Double.MinValue
    obj.decimal = BigDecimal(Long.MinValue, 2)
    obj.boolean = false
    obj.toMap shouldBe jsonRoundtrip(obj)

    // positives
    obj.id = Long.MaxValue
    obj.int = Integer.MAX_VALUE
    obj.bigint = BigInt(Long.MaxValue) + 1
    obj.double = Double.MaxValue
    obj.decimal = BigDecimal(Long.MaxValue, 2)
    obj.boolean = true
    obj.toMap shouldBe jsonRoundtrip(obj)

    obj.bytes = "Rūķīši".getBytes("UTF-8")
    comparable(obj.toMap) shouldBe comparable(jsonRoundtrip(obj))

    // child view
    obj.child = new decoder_test_child
    obj.child.id = 333
    obj.child.name = "CHILD-1"
    obj.child.date = java.sql.Date.valueOf("2021-11-08")
    obj.child.date_time = java.sql.Timestamp.valueOf("2021-12-26 23:57:14.0")
    comparable(obj.toMap) shouldBe comparable(jsonRoundtrip(obj))

    // children
    obj.children = List(new decoder_test_child, new decoder_test_child)
    obj.children(0).name = "CHILD-2"
    obj.children(1).name = "CHILD-3"
    comparable(obj.toMap) shouldBe comparable(jsonRoundtrip(obj))

    // seqs of simple types
    obj.long_seq = List(0L, 1L)
    obj.string_seq = List("AB", "CD")
    obj.date_seq = List(java.sql.Date.valueOf("2021-11-28"), java.sql.Date.valueOf("2021-11-29"))
    obj.datetime_seq = List(
      java.sql.Timestamp.valueOf("2021-12-26 23:57:14.0"),
      java.sql.Timestamp.valueOf("2021-12-26 23:57:15.0"))
    obj.int_seq = List(1, 2, 3)
    obj.bigint_seq = List(BigInt(Long.MinValue) - 1, BigInt(Long.MaxValue) + 1)
    obj.double_seq = List(Double.MinValue, Double.MaxValue)
    obj.decimal_seq = List(BigDecimal(Long.MinValue, 2), BigDecimal(Long.MaxValue, 2))
    obj.boolean_seq = List(false, true, true)
    obj.bytes_seq = List("Rūķ".getBytes("UTF-8"), "īši".getBytes("UTF-8"))
    comparable(obj.toMap) shouldBe comparable(jsonRoundtrip(obj))

    val jsonized = """{
      "id": 9223372036854775807,
      "string": "Rūķīši-X-123",
      "date": "2021-12-21",
      "date_time": "2021-12-26 23:57:14",
      "int": 2147483647,
      "bigint": 9223372036854775808,
      "double": 1.7976931348623157E+308,
      "decimal": 92233720368547758.07,
      "boolean": true,
      "bytes": "UsWrxLfEq8WhaQ==",
      "child": {
        "id": 333,
        "name": "CHILD-1",
        "date": "2021-11-08",
        "date_time": "2021-12-26 23:57:14"
      },
      "long_seq": [0, 1],
      "string_seq": ["AB", "CD"],
      "date_seq": ["2021-11-28", "2021-11-29"],
      "datetime_seq": ["2021-12-26 23:57:14", "2021-12-26 23:57:15"],
      "int_seq": [1, 2, 3],
      "bigint_seq": [-9223372036854775809, 9223372036854775808],
      "double_seq": [-1.7976931348623157E+308, 1.7976931348623157E+308],
      "decimal_seq": [-92233720368547758.08, 92233720368547758.07],
      "boolean_seq": [false, true, true],
      "bytes_seq": ["UsWrxLc=", "xKvFoWk="],
      "children": [{
        "id": null,
        "name": "CHILD-2",
        "date": null,
        "date_time": null
      }, {
        "id": null,
        "name": "CHILD-3",
        "date": null,
        "date_time": null
      }]
    }""".replaceAll("\n    ", "\n")
    obj.toMap.toJson.prettyPrint          shouldBe jsonized
    jsonRoundtrip(obj).toJson.prettyPrint shouldBe jsonized

    val asStringsJsonized = """{
      "id": "9223372036854775807",
      "string": "Rūķīši-X-123",
      "date": "2021-12-21",
      "date_time": "2021-12-26 23:57:14",
      "int": "2147483647",
      "bigint": "9223372036854775808",
      "double": "1.7976931348623157E+308",
      "decimal": "92233720368547758.07",
      "boolean": "true",
      "bytes": "UsWrxLfEq8WhaQ==",
      "child": {
        "id": "333",
        "name": "CHILD-1",
        "date": "2021-11-08",
        "date_time": "2021-12-26 23:57:14"
      },
      "long_seq": ["0", "1"],
      "string_seq": ["AB", "CD"],
      "date_seq": ["2021-11-28", "2021-11-29"],
      "datetime_seq": ["2021-12-26 23:57:14", "2021-12-26 23:57:15"],
      "int_seq": ["1", "2", "3"],
      "bigint_seq": ["-9223372036854775809", "9223372036854775808"],
      "double_seq": ["-1.7976931348623157E+308", "1.7976931348623157E+308"],
      "decimal_seq": ["-92233720368547758.08", "92233720368547758.07"],
      "boolean_seq": ["false", "true", "true"],
      "bytes_seq": ["UsWrxLc=", "xKvFoWk="],
      "children": [{
        "id": null,
        "name": "CHILD-2",
        "date": null,
        "date_time": null
      }, {
        "id": null,
        "name": "CHILD-3",
        "date": null,
        "date_time": null
      }]
    }""".replaceAll("\n    ", "\n")

    // strict decoder
    val strictExcMsg =
      intercept[org.wabase.UnprocessableEntityException] {
        decodeToMap(ByteString(asStringsJsonized), "decoder_test")
      }.getMessage
    strictExcMsg should include ("decoder_test")
    strictExcMsg should include ("Failed to read id of type long")
    strictExcMsg should include ("Expected Long but got")

    // lenient decoder
    decodeToMap(
      ByteString(asStringsJsonized),
      "decoder_test",
      lenientDecoder).toJson.prettyPrint  shouldBe jsonized

    // compatibility
    val cpy = new decoder_test
    obj.fill(obj.toMap) shouldBe      obj
    cpy.fill(obj.toMap) should not be obj
    obj.toMap.toJson.prettyPrint          shouldBe jsonized
    cpy.toMap.toJson.prettyPrint          shouldBe jsonized

    // nulls for bytes, seqs
    obj.bytes = null
    obj.long_seq = null
    obj.string_seq = null
    obj.date_seq = null
    obj.datetime_seq = null
    obj.int_seq = null
    obj.bigint_seq = null
    obj.double_seq = null
    obj.decimal_seq = null
    obj.boolean_seq = null
    obj.bytes_seq = null
    obj.children = null
    obj.toMap shouldBe jsonRoundtrip(obj)
  }
}

object JsonDecoderSpecs {
  class decoder_test extends DtoWithId {
    var id: java.lang.Long = null
    var string: String = null
    var date: java.sql.Date = null
    var date_time: java.sql.Timestamp = null
    var int: java.lang.Integer = null
    var bigint: BigInt = null
    var double: java.lang.Double = null
    var decimal: BigDecimal = null
    var boolean: java.lang.Boolean = null
    var bytes: Array[Byte] = null
    var child: decoder_test_child = null
    var long_seq: List[java.lang.Long] = Nil
    var string_seq: List[String] = Nil
    var date_seq: List[java.sql.Date] = Nil
    var datetime_seq: List[java.sql.Timestamp] = Nil
    var int_seq: List[java.lang.Integer] = Nil
    var bigint_seq: List[BigInt] = Nil
    var double_seq: List[java.lang.Double] = Nil
    var decimal_seq: List[BigDecimal] = Nil
    var boolean_seq: List[java.lang.Boolean] = Nil
    var bytes_seq: List[Array[Byte]] = Nil
    var children: List[decoder_test_child] = Nil
  }

  class decoder_test_child extends Dto {
    var id: java.lang.Long = null
    var name: String = null
    var date: java.sql.Date = null
    var date_time: java.sql.Timestamp = null
  }

  val viewNameToClass = Map[String, Class[_ <: Dto]](
    "decoder_test" -> classOf[decoder_test],
    "decoder_test_child" -> classOf[decoder_test_child],
  )
  val classToViewName: Map[Class[_ <: Dto], String] =
    viewNameToClass.map(_.swap)
}
