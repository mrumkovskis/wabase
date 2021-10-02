package org.wabase

import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.scalatest.matchers.should.Matchers

class PostgresTextFormatTest extends FlatSpec with Matchers {
  behavior of "PostgresTextFormat"
  val specialChars = List(
    "\u0000",
    "\u0001",
    "\u0002",
    "\u0003",
    "\u0004",
    "\u0005",
    "\u0006",
    "\u0007",
    "\b",
    "\t",
    "\n",
    "\u000b",
    "\f",
    "\r",
    "\\",
  )
  val values = List(
    null,
    -1,
    0,
    1L,
    true,
    false,
    "null",
    "true",
    "false",
    "\r\n",
    "\"",
    "#",
    ".,;!?",
    "0123456789",
    "{}[]()*&^%$#@!~`'|/",
    "",
    " ",
    "  ",
    """\""",
    """\n""",
    """\v""",
    """\N""",
    """\000""",
    "abc",
    "glāžšķūņu rūķīši",
    specialChars.mkString,
    specialChars.reverse.mkString,
  ) ++ specialChars

  it should "properly encode and decode values" in {
    val data        = values.zipWithIndex.map { case (v, i) => (s"c$i" -> v) }.toMap
    val names       = data.keySet.toList.sorted
    val encoded     = PostgresTextFormat.encodeColumns(names, data)
    val decoded     = PostgresTextFormat.decodeColumns(names, encoded)
    val stringified = data.map { case (k, v) => if (v == null) (k, v) else (k, v.toString) }

    values.size  shouldBe 42
    decoded.size shouldBe values.size
    stringified  shouldBe decoded

    val encoded_c0  = PostgresTextFormat.encodeColumns(Seq(names.head), data)
    val decoded_c0  = PostgresTextFormat.decodeColumns(Seq(names.head), encoded_c0)
    encoded_c0.utf8String shouldBe """\N"""
    decoded_c0.values.toSet.head shouldBe null

    val encoded_c1  = PostgresTextFormat.encodeColumns(Seq(names.tail.head), data)
    val decoded_c1  = PostgresTextFormat.decodeColumns(Seq(names.tail.head), encoded_c1)
    decoded_c1.values.toSet.head shouldBe decoded("c1")

    PostgresTextFormat
      .encodeColumns(Seq("a", "b"), Map("a" -> "\r\n", "b" -> " {[(a) + b]} = \téléphone "))
      .utf8String
      .shouldBe("\\r\\n\t {[(a) + b]} = \\téléphone ")

    val errorMessage =
      intercept[RuntimeException] {
        PostgresTextFormat.decodeColumns(List("c0", "c1"), encoded_c0)
      }.getMessage
    errorMessage should include ("Unexpected field count")
  }
}
