package org.wabase.spreadsheet.xlsxml

import java.io._
import org.wabase.Format

case class Font(bold: Boolean) {
  def write(out: Writer) {
    out write "    <Font ss:Bold=\""
    out write (if (bold) "1" else "0")
    out write "\"/>\r\n"
  }
}
object Font {
  val BOLD = Font(true)
}
case class NumberFormat(format: String) {
  require(format != null, "NumberFormat format must not be null")
  def write(out: Writer) {
    out write "    <NumberFormat ss:Format=\""
    out write format
    out write "\"/>\r\n"
  }
}
object NumberFormat {
  val DATE = NumberFormat("dd.mm.yyyy;@")
}
case class Style(id: String, numberFormat: NumberFormat = null, font: Font = null) {
  require(id != null, "Style id must not be null")
  def write(out: Writer) {
    out write "   <Style ss:ID=\""
    out write id
    out write "\">\r\n"
    if (font != null)
      font write out
    if (numberFormat != null)
      numberFormat write out
    out write "   </Style>\r\n"
  }
}

sealed trait DataType { def name: String }
case object NUMBER extends DataType { val name = "Number" }
case object DATE_TIME extends DataType { val name = "DateTime" }
case object STRING extends DataType { val name = "String" }

case class Cell(type_ : DataType, value: String, style: Style)

class XlsXmlStreamer(val out: Writer) {

  def startWorkbook(styles: Seq[Style]) {
    out write """  |<?xml version="1.0" encoding="UTF-8"?>
                   |<?mso-application progid="Excel.Sheet"?>
                   |<Workbook xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet"
                   |          xmlns="urn:schemas-microsoft-com:office:spreadsheet">
                   |""".stripMargin
    if (styles != null && !styles.isEmpty) {
      out write """|  <Styles>
                   |""".stripMargin
      styles.foreach(_ write out)
      out write """|  </Styles>
                   |""".stripMargin
    }
  }
  def startWorksheet(name: String) {
    out write "  <Worksheet ss:Name=\""
    out write name
    out write "\">\r\n"
  }
  def startTable {
    out write "    <Table>\r\n"
  }
  def startRow {
    out write "      <Row>"
  }
  def cell(value: Any, style: Style = null) {
    var t: DataType = null
    var v: String = null
    value match {
      case null =>
      // ok
      case n: Number =>
        t = NUMBER
        v = n.toString
      case d: java.util.Date =>
        t = DATE_TIME
        v = Format.xsdDate(d) + "T00:00:00.000" //for example xlsxDateTime(d) add 1 hour to specific dates, like '1984-04-01'
      case x =>
        t = STRING
        v = Format.xmlEscape(x.toString) // TODO do not build escaped string, stream it!
    }
    out write "<Cell"
    if (style != null) {
      out write " ss:StyleID=\""
      out write style.id
      out write "\""
    }
    out write ">"
    if (v != null && v.length > 0) {
      out write "<Data ss:Type=\""
      out write t.name
      out write "\">"
      out write v
      out write "</Data>"
    }
    out write "</Cell>"
  }
  def endRow {
    out write "</Row>\r\n"
  }
  def endTable: Unit = {
    out write "    </Table>\r\n"
  }
  def endWorksheet {
    out write "  </Worksheet>\r\n"
  }
  def endWorkbook {
    out write "</Workbook>\r\n"
  }
}
