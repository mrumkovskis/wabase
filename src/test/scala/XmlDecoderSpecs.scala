package org.wabase

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.w3c.dom.Element

import javax.xml.parsers.DocumentBuilderFactory

class XmlDecoderSpecs extends AnyFlatSpec with Matchers {

  def parseXml(xmlStr: String): Element = {
    val dbFactory = DocumentBuilderFactory.newInstance()
    val dBuilder = dbFactory.newDocumentBuilder()
    val doc = dBuilder.parse(new java.io.ByteArrayInputStream(xmlStr.getBytes))
    doc.getDocumentElement
  }

  it should "convert simple xml to map" in {
    XmlDecoderFactory.elementToMap(parseXml("""
      <person id="1">
        <name>John Doe</name>
        <age>30</age>
        <address>
          <street>Main St</street>
          <city>NY</city>
        </address>
        <note>this text should not be ignored<child>value</child></note>
      </person>""".trim
    )) shouldBe Map(
      "id" -> "1",
      "name" -> "John Doe",
      "age" -> "30",
      "address" -> Map(
        "street" -> "Main St",
        "city" -> "NY",
      ),
      "note" -> Map(
        "#text" -> "this text should not be ignored",
        "child" -> "value",
      )
    )

    XmlDecoderFactory.elementToMap(parseXml(
      """<person id="1">Hello<name>John</name> Doe<age>30</age></person>""")) shouldBe Map(
        "id" -> "1", "#text" -> "Hello Doe", "name" -> "John", "age" -> "30",
    )

    XmlDecoderFactory.elementToMap(parseXml(
      """<address><street>Main St</street><city>NY</city></address>""")) shouldBe Map(
        "street" -> "Main St", "city" -> "NY",
    )

    XmlDecoderFactory.elementToMap(parseXml(
      """<note>text<child>value</child></note>""")) shouldBe Map(
        "#text" -> "text", "child" -> "value",
    )

    XmlDecoderFactory.elementToMap(parseXml(
      """<person><name>John</name><name>Doe</name></person>""")) shouldBe Map(
        "name" -> Vector("John", "Doe"),
    )

    XmlDecoderFactory.elementToMap(parseXml(
      """<persons><person><name>John</name></person><person><name>Doe</name></person></persons>""")) shouldBe Map(
        "person" -> Vector(Map("name" -> "John"), Map("name" -> "Doe")),
    )

    XmlDecoderFactory.elementToMap(parseXml(
      """<person id="1">Hello</person>""")) shouldBe Map(
        "id" -> "1", "#text" -> "Hello",
    )
  }
}
