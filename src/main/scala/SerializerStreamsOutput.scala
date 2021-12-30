package org.wabase

import org.wabase.Format.{xlsxDateTime, xsdDate}
import io.bullet.borer
import io.bullet.borer.{Cbor, Json}

import java.io
import java.io.OutputStream
import java.util.zip.ZipOutputStream
import org.mojoz.metadata.MojozViewDef

import scala.collection.immutable.Seq

class CborOrJsonOutput(
  w: borer.Writer,
  isCollection: Boolean,
  viewName: String,
  nameToViewDef: Map[String, MojozViewDef],
) extends BorerValueEncoder(w) with NestedArraysHandler {
  import CborOrJsonOutput.Context
  var contextStack: List[Context] = Nil
  override def writeStartOfInput(): Unit = {
    if (isCollection) w.writeArrayStart()
    val viewDef =
      if (viewName != null && nameToViewDef != null)
        nameToViewDef.getOrElse(viewName, null)
      else null
    contextStack = new Context(isRow = true, viewDef) :: contextStack
  }
  override def writeArrayStart(): Unit = {
    val context = contextStack.head
    if (context.isRow) {
      if (!context.isFirstRow) {
        w.writeMapStart()
        contextStack = new Context(isRow = false, context.viewDef, allNames = context.names) :: contextStack
      } else {
        context.readingNames = true
      }
    } else {
      val name = context.names.head
      val fieldDef =
        if (context.viewDef != null)
          context.viewDef.fields.find(f => Option(f.alias).getOrElse(f.name) == name).orNull
        else null
      val viewDef =
        if (fieldDef != null && fieldDef.type_.isComplexType)
          nameToViewDef.getOrElse(fieldDef.type_.name, null)
        else null
      val isCollection = fieldDef == null || fieldDef.isCollection
      super.writeValue(name)
      if (isCollection)
        w.writeArrayStart()
      context.names = context.names.tail
      contextStack = new Context(isRow = true, viewDef, allNames = Nil, isCollection) :: contextStack
    }
  }
  override def writeValue(value: Any): Unit = {
    val context = contextStack.head
    if (!context.isRow) {
      super.writeValue(context.names.head)
      super.writeValue(value)
      context.names = context.names.tail
    } else if (context.isFirstRow) {
      if (context.readingNames) {
        context.names = ("" + value) :: context.names
      } else {
        super.writeValue(value)
      }
    }
  }
  override def writeArrayBreak(): Unit = {
    val context = contextStack.head
    if (context.isRow && context.isFirstRow && context.readingNames) {
      context.isFirstRow = false
      context.readingNames = false
      context.names = context.names.reverse
    } else {
      if (context.isCollection)
        w.writeBreak()
      contextStack = contextStack.tail
    }
  }
  override def writeEndOfInput(): Unit = {
    if (isCollection) w.writeBreak()
    contextStack = contextStack.tail
  }
}

object CborOrJsonOutput {
  class Context(
    val isRow: Boolean,
    val viewDef: MojozViewDef,
    val allNames: List[String] = Nil,
    val isCollection: Boolean = true
  ) {
    var isFirstRow: Boolean = isRow
    var readingNames: Boolean = false
    var names: List[String] = allNames
  }
}

object CborOutput {
  def apply(outputStream: OutputStream, isCollection: Boolean, viewName: String, nameToViewDef: Map[String, MojozViewDef]) =
    new CborOrJsonOutput(BorerNestedArraysEncoder.createWriter(outputStream, Cbor), isCollection, viewName, nameToViewDef)
}

object JsonOutput {
  def apply(outputStream: OutputStream, isCollection: Boolean, viewName: String, nameToViewDef: Map[String, MojozViewDef]) =
    new CborOrJsonOutput(BorerNestedArraysEncoder.createWriter(outputStream, Json), isCollection, viewName, nameToViewDef)
}

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
    if (labels != null && labels.nonEmpty) {
      writeRowStart()
      labels foreach { label =>
        writeCell(label)
        col += 1
      }
      writeRowEnd()
    }
    row += 1
    col = 0
  }
  def writeRowStart():        Unit = {}
  def writeCell(value: Any):  Unit
  def writeRowEnd():          Unit = {}
  def writeFooter():          Unit = {}
}

class CsvOutput(writer: io.Writer, labels: Seq[String]) extends FlatTableOutput(labels) {
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

class XlsXmlOutput(writer: io.Writer, labels: Seq[String], worksheetName: String = "data") extends FlatTableOutput(labels) {
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
