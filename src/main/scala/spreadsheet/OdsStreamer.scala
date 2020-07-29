package org.wabase.spreadsheet.ods

import java.io._
import scala.io.Source
import org.wabase.Format
import java.util.zip._

class OdsStreamer(val zip: java.util.zip.ZipOutputStream, styles: List[OdsStyle] = Nil) {
  private val writer = new OutputStreamWriter(zip)
  private val contentStreamer = new OdsContentStreamer(writer, styles)
  private def resourceToZip(resourceName: String) = {
    val source = Source.fromResource(resourceName)
    val value  = source.getLines.mkString("\n")
    source.close
    zip.write(value.getBytes("UTF-8"))
  }
  def startWorkbook: Unit = {
    import scala.io.Source
    zip.putNextEntry(new ZipEntry("META-INF/"))
    zip.putNextEntry(new ZipEntry("META-INF/manifest.xml"))
    resourceToZip("spreadsheet/ods/META-INF/manifest.xml")
    zip.putNextEntry(new ZipEntry("manifest.rdf"))
    resourceToZip("spreadsheet/ods/manifest.rdf")
    zip.putNextEntry(new ZipEntry("mimetype"))
    resourceToZip("spreadsheet/ods/mimetype")
    zip.putNextEntry(new ZipEntry("content.xml"))
    contentStreamer.startWorkbook
  }
  def startWorksheet =
    contentStreamer.startWorksheet
  def startTable(name: String) =
    contentStreamer.startTable(name, styles)
  def startTable(name: String, individualStyles: List[OdsStyle]) =
    contentStreamer.startTable(name, individualStyles)
  def startRow =
    contentStreamer.startRow(null)
  def startRow(style: String) =
    contentStreamer.startRow(style)
  def cell(value: Any, style: String = null, formula: String = null) =
    contentStreamer.cell(value, style, formula)
  def endRow = {
    contentStreamer.endRow
    writer.flush
  }
  def endTable = {
    contentStreamer.endTable
    writer.flush
  }
  def endWorksheet = {
    contentStreamer.endWorksheet
    writer.flush
  }
  def endWorkbook = {
    contentStreamer.endWorkbook
    writer.close
  }
}

case class OdsStyle(name: String, family: String, body: String)
class OdsContentStreamer(val out: Writer, styles: List[OdsStyle] = Nil) {
  def startWorkbook {
    out write """|<?xml version="1.0" encoding="UTF-8"?>
                 |<office:document-content
                 |  xmlns:office="urn:oasis:names:tc:opendocument:xmlns:office:1.0"
                 |  xmlns:calcext="urn:org:documentfoundation:names:experimental:calc:xmlns:calcext:1.0"
                 |  xmlns:number="urn:oasis:names:tc:opendocument:xmlns:datastyle:1.0"
                 |  xmlns:style="urn:oasis:names:tc:opendocument:xmlns:style:1.0"
                 |  xmlns:table="urn:oasis:names:tc:opendocument:xmlns:table:1.0"
                 |  xmlns:text="urn:oasis:names:tc:opendocument:xmlns:text:1.0"
                 |  xmlns:fo="urn:oasis:names:tc:opendocument:xmlns:xsl-fo-compatible:1.0"
                 |  xmlns:of="urn:oasis:names:tc:opendocument:xmlns:of:1.2">
                 |""".stripMargin
    out write """|  <office:automatic-styles>
                 |     <number:date-style style:name="N1">
                 |        <number:year number:style="long" />
                 |        <number:text>-</number:text>
                 |        <number:month number:style="long" />
                 |        <number:text>-</number:text>
                 |        <number:day number:style="long" />
                 |     </number:date-style>
                 |     <number:date-style style:name="N2">
                 |        <number:year number:style="long" />
                 |        <number:text>-</number:text>
                 |        <number:month number:style="long" />
                 |        <number:text>-</number:text>
                 |        <number:day number:style="long" />
                 |        <number:text> </number:text>
                 |        <number:hours number:style="long" />
                 |        <number:text>:</number:text>
                 |        <number:minutes number:style="long" />
                 |        <number:text>:</number:text>
                 |        <number:seconds number:style="long" />
                 |     </number:date-style>
                 |     <style:style style:name="N1" style:family="table-cell" style:data-style-name="N1" />
                 |     <style:style style:name="N2" style:family="table-cell" style:data-style-name="N2" />
                 |""".stripMargin

    styles.filter(_.family=="table-cell").foreach{style =>
      out write
        s"""<style:style style:name="${style.name}" style:family="${style.family}">
           |${style.body}
           |</style:style>""".stripMargin
      out write
        s"""<style:style style:name="${style.name}_N1" style:family="${style.family}" style:data-style-name="N1">
           |${style.body}
           |</style:style>""".stripMargin
      out write
        s"""<style:style style:name="${style.name}_N2" style:family="${style.family}" style:data-style-name="N2">
           |${style.body}
           |</style:style>""".stripMargin
    }
    styles.filterNot(_.family=="table-cell").foreach{style =>
      out write
        s"""<style:style style:name="${style.name}" style:family="${style.family}">
           |${style.body}
           |</style:style>""".stripMargin
    }
    out write """|  </office:automatic-styles>
                 |""".stripMargin
    out write """|  <office:body>
                 |""".stripMargin
  }

  def startWorksheet {
    out write s"""|    <office:spreadsheet>""".stripMargin
  }
  def startTable(name: String, individualStyles: List[OdsStyle] = Nil) {
    out write s"""|      <table:table table:name="$name"${
                    ( if (!individualStyles.filter(_.family=="table").isEmpty)
                        " table:style-name=\"" + individualStyles.filter(_.family=="table").head.name + "\""
                      else "")}>
                  |${ ( if (!individualStyles.filter(_.family=="table-column").isEmpty)
                        individualStyles.filter(_.family=="table-column").map(s => "        <table:table-column table:style-name=\"" + s.name + "\"/>").mkString("\n")
                        else "")}
                  |""".stripMargin
  }
  def startRow(style: String) {
    out write "        <table:table-row"
    if (style != null) {
      out write " table:style-name=\""
      out write style
      out write "\""
    }
    out write ">"
  }
  def cell(value: Any, style: String = null, formula: String = null) {
    var t: String = null
    var a: String = null
    var v: String = null
    var s: String = null
    var l: String = null
    value match {
      case null =>
      // ok
      case b: Boolean =>
        t = "boolean"
        a = "office:boolean-value"
        v = b.toString
      case b: java.lang.Boolean =>
        t = "boolean"
        a = "office:boolean-value"
        v = b.toString
      case n: Number =>
        t = "float"
        a = "office:value"
        v = n.toString
      case d: java.util.Date =>
        t = "date"
        a = "office:date-value"
        v = Format.timerDate(d)
        s = "N2"
        if (v endsWith "00:00:00") {
          v = Format.xsdDate(d)
          s = "N1"
        } else {
          l = Format.humanDateTime(d)
        }
      case s: Seq[_] =>
        t = "string"
        a = null // skip string-value attribute
        v = Option(s.mkString("; ")).filter(_ != "").map(Format.xmlEscape).orNull
      case x =>
        t = "string"
        a = null // skip string-value attribute
        v = Format.xmlEscape(x.toString) // TODO do not build escaped string, stream it!
    }
    if (l == null) l = v
    out write "<table:table-cell"

    val styleName = List(style, s).filter(_!= null).mkString("_")
    if (styleName.nonEmpty) {
      out write " table:style-name=\""
      out write styleName
      out write "\""
    }
    if (formula != null) {
      out write " table:formula=\""
      out write formula
      out write "\""
    }
    if(t != null){
      out write " office:value-type=\""
      out write t
      out write "\" calcext:value-type=\""
      out write t
      out write "\""
    }
    if (a != null && v != null) {
      out write " "
      out write a
      out write "=\""
      out write v
      out write "\""
    }
    out write ">"
    if (l != null && l.nonEmpty) {
      out write "<text:p>"
      out write l
      out write "</text:p>"
    }
    out write "</table:table-cell>"
  }

  def endRow {
    out write "</table:table-row>\r\n"
  }
  def endTable {
    out write "      </table:table>\r\n"
  }
  def endWorksheet {
    out write "    </office:spreadsheet>\r\n"
  }
  def endWorkbook {
    out write "  </office:body>\r\n"
    out write "</office:document-content>\r\n"
  }
}