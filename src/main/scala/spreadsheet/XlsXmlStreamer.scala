package org.wabase.spreadsheet.xlsxml

import java.io._
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset}
import org.wabase.Format

case class Font(bold: Boolean) {
  def write(out: Writer): Unit = {
    out write "    <Font ss:Bold=\""
    out write (if (bold) "1" else "0")
    out write "\"/>\n"
  }
}
object Font {
  val BOLD = Font(true)
}
case class NumberFormat(format: String) {
  require(format != null, "NumberFormat format must not be null")
  def write(out: Writer): Unit = {
    out write "    <NumberFormat ss:Format=\""
    out write format
    out write "\"/>\n"
  }
}
object NumberFormat {
  //? DATE_DMY          = NumberFormat("""dd.mm.yyyy;@""")
  val DATE_YMD          = NumberFormat("""yyyy\-mm\-dd""")
  val DATE_TIME_HM      = NumberFormat("""yyyy\-mm\-dd\ hh:mm""")
  val DATE_TIME_HMS     = NumberFormat("""yyyy\-mm\-dd\ hh:mm:ss""")
  val DATE_TIME_HMS_MS  = NumberFormat("""yyyy\-mm\-dd\ hh:mm:ss.000""")
}
case class Style(id: String, numberFormat: NumberFormat = null, font: Font = null) {
  require(id != null, "Style id must not be null")
  def write(out: Writer): Unit = {
    out write "   <Style ss:ID=\""
    out write id
    out write "\">\n"
    if (font != null)
      font write out
    if (numberFormat != null)
      numberFormat write out
    out write "   </Style>\n"
  }
}

sealed trait DataType { def name: String }
case object NUMBER extends DataType { val name = "Number" }
case object DATE_TIME extends DataType { val name = "DateTime" }
case object STRING extends DataType { val name = "String" }

case class Cell(type_ : DataType, value: String, style: Style)

class XlsXmlStreamer(val out: Writer) {
  private val xlsXmlDateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS")

  def startWorkbook(styles: Seq[Style]): Unit = {
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
  def startWorksheet(name: String): Unit = {
    out write "  <Worksheet ss:Name=\""
    out write name
    out write "\">\n"
  }
  def startTable: Unit = {
    out write "    <Table>\n"
  }
  def startRow: Unit = {
    out write "      <Row>"
  }
  def cell(value: Any, style: Style = null): Unit = {
    var t: DataType = null
    var v: String = null
    value match {
      case null =>
      // ok
      case n: Number =>
        t = NUMBER
        v = n.toString
      case d: java.time.LocalDate =>
        t = DATE_TIME
        v = d.atStartOfDay.format(xlsXmlDateTimeFormatter)
      case d: java.time.LocalDateTime =>
        t = DATE_TIME
        v = d.format(xlsXmlDateTimeFormatter)
      case d: java.sql.Timestamp =>
        t = DATE_TIME
        v = d.toLocalDateTime.format(xlsXmlDateTimeFormatter)
      case d: java.sql.Date =>
        t = DATE_TIME
        v = d.toLocalDate.atStartOfDay.format(xlsXmlDateTimeFormatter)
      case d: java.time.Instant =>
        t = DATE_TIME
        v = LocalDateTime.ofInstant(d, ZoneOffset.UTC).format(xlsXmlDateTimeFormatter)
      case d: java.util.Date =>
        t = DATE_TIME
        v = LocalDateTime.ofInstant(d.toInstant, ZoneOffset.UTC).format(xlsXmlDateTimeFormatter)
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
  def endRow: Unit = {
    out write "</Row>\n"
  }
  def endTable: Unit = {
    out write "    </Table>\n"
  }
  def endWorksheet: Unit = {
    out write "  </Worksheet>\n"
  }
  def endWorkbook: Unit = {
    out write "</Workbook>\n"
  }
}
