package org.wabase

import org.wabase.Format.{xlsxDateTime, xsdDate}

import java.io.Writer
import java.util.zip.ZipOutputStream
import scala.collection.immutable.Seq

abstract class FlatTableOutput(val labels: Seq[String]) extends NestedArraysHandler {
  protected var row = 0
  protected var col = 0
  protected var lvl = 0
  override def writeStartOfInput(): Unit =
    writeHeader()
  override def writeArrayStart(): Unit = {
    lvl += 1
    if (lvl == 1)
      writeRowStart()
  }
  override def writeValue(value: Any): Unit =
    if (lvl == 1) {
      writeCell(value)
      col += 1
    }
  override def writeArrayBreak(): Unit = {
    if (lvl == 1) {
      writeRowEnd()
      row += 1
      col = 0
    }
    lvl -= 1
  }
  override def writeEndOfInput(): Unit =
    writeFooter()
  def writeHeader(): Unit = {
    writeRowStart()
    labels foreach { label =>
      writeCell(label)
      col += 1
    }
    writeRowEnd()
    row += 1
    col = 0
  }
  def writeRowStart():        Unit = {}
  def writeCell(value: Any):  Unit
  def writeRowEnd():          Unit = {}
  def writeFooter():          Unit = {}
}

class CsvOutput(writer: Writer, labels: Seq[String]) extends FlatTableOutput(labels) {
  def escapeValue(s: String) =
    if (s == null) null
    else if (s.contains(",") || s.contains("\"")) ("\"" + s.replaceAll("\"", "\"\"") + "\"")
    else s
  def csvValue(v: Any): String = Option(v).map{
    case m: Map[String @unchecked, Any @unchecked] => ""
    case l: Traversable[Any] => ""
    case n: java.lang.Number => String.valueOf(n)
    case t: Timestamp => xlsxDateTime(t)
    case d: jDate => xsdDate(d)
    case x => x.toString
  }.map(escapeValue).getOrElse("")
  override def writeCell(value: Any) = {
    if (col > 0)
      writer.write(",")
    writer.write(csvValue(value))
    writer.flush
  }
  override def writeRowEnd() = {
    writer.write("\n")
    writer.flush
  }
}

class OdsOutput(zos: ZipOutputStream, labels: Seq[String], worksheetName: String = "data") extends FlatTableOutput(labels) {
  import org.wabase.spreadsheet.ods._
  val streamer = new OdsStreamer(zos)
  override def writeHeader() = {
    streamer.startWorkbook
    streamer.startWorksheet
    streamer.startTable(worksheetName)
    super.writeHeader()
  }
  override def writeRowStart()        = streamer.startRow
  override def writeCell(value: Any)  = streamer.cell(value)
  override def writeRowEnd()          = streamer.endRow
  override def writeFooter() = {
    streamer.endTable
    streamer.endWorksheet
    streamer.endWorkbook
  }
}

class XlsXmlOutput(writer: Writer, labels: Seq[String], worksheetName: String = "data") extends FlatTableOutput(labels) {
  import org.wabase.spreadsheet.xlsxml._
  val headerStyle = Style("header", null, Font.BOLD)
  val streamer = new XlsXmlStreamer(writer)
  override def writeHeader() = {
    streamer.startWorkbook(Seq(headerStyle))
    streamer.startWorksheet(worksheetName)
    streamer.startTable
    super.writeHeader()
  }
  override def writeRowStart()        = streamer.startRow
  override def writeCell(value: Any)  =
    if (row == 0) streamer.cell(value, headerStyle)
    else          streamer.cell(value)
  override def writeRowEnd()          = streamer.endRow
  override def writeFooter() = {
    streamer.endTable
    streamer.endWorksheet
    streamer.endWorkbook
  }
}
