package org.wabase

import akka.util.ByteString
import org.wabase.Format.{xlsxDateTime, xsdDate}
import io.bullet.borer
import io.bullet.borer.{Cbor, Json}
import io.bullet.borer.compat.akka.ByteStringByteAccess

import java.io
import java.io.OutputStream
import java.util.zip.ZipOutputStream
import org.mojoz.metadata.MojozViewDef
import org.wabase.ResultEncoder.{ByteChunks, ChunkType, TextChunks}

import scala.collection.immutable.Seq

import AppMetadata.AugmentedAppFieldDef

object ResultEncoder {
  type EncoderFactory = OutputStream => ResultEncoder
  trait ChunkType
  object TextChunks extends ChunkType
  object ByteChunks extends ChunkType
}

import ResultEncoder._
trait ResultEncoder {
  def writeStartOfInput():      Unit
  def writeArrayStart():        Unit
  def writeValue(value: Any):   Unit
  def startChunks(
        chunkType: ChunkType):  Unit
  def writeChunk(chunk: Any):   Unit
  def writeBreak():             Unit
  def writeEndOfInput():        Unit
}

abstract class ResultRenderer(
  isCollection: Boolean,
  viewName: String,
  nameToViewDef: Map[String, MojozViewDef],
  hasHeaders: Boolean,
) extends ResultEncoder {
  import ResultRenderer.Context
  protected var contextStack: List[Context] = Nil
  protected var chunkType: ChunkType = null
  protected var buffer: ByteString = null
  protected var level = 0
  protected def renderHeader(): Unit      = {}
  protected def renderHeaderData(): Unit  = {}
  protected def renderArrayStart(): Unit  = {}
  protected def renderMapStart(): Unit    = {}
  protected def renderKey(key: Any): Unit = renderValue(key)
  protected def renderValue(value: Any): Unit
  protected def renderBreak(): Unit       = {}
  protected def renderFooter(): Unit      = {}
  protected def renderRawValue(value: Any): Unit = value match {
    case bytes: Array[Byte] =>
      renderValue(ByteString.fromArrayUnsafe(bytes).encodeBase64.utf8String)
    case _ => renderValue(value)
  }
  protected def shouldRender(name: String): Boolean = {
    val context = contextStack.head
    context.viewDef == null || context.viewDef.fieldOpt(name).exists(!_.api.excluded)
  }
  protected def getViewDef(viewName: String) =
    if (viewName != null)
      nameToViewDef.getOrElse(viewName, sys.error(s"View $viewName not found - can not render result"))
    else null
  protected def impliedNames(viewDef: MojozViewDef): List[String] = {
    if  (hasHeaders || viewDef == null) Nil
    else viewDef.fields.map(_.fieldName).toList
  }
  protected def nextName(context: Context) = {
    if (context.names.nonEmpty) {
      val name = context.names.head
      context.names = context.names.tail
      name
    } else null
  }
  override def writeStartOfInput(): Unit = {
    val viewDef = getViewDef(viewName)
    val allNames = impliedNames(viewDef)
    contextStack = new Context(isForRows = true, viewDef, allNames, isCollection = isCollection) :: contextStack
    renderHeader()
    if (!hasHeaders) {
      renderHeaderData()
    }
  }
  override def writeArrayStart(): Unit = {
    level += 1
    val context = contextStack.head
    if (context.isForRows) {
      val isHeaderRow = context.index == 0 && hasHeaders
      val shouldRender_ = context.shouldRender && !isHeaderRow
      contextStack = new Context(
        isForRows = false, context.viewDef, context.names, shouldRender = shouldRender_) :: contextStack
      if (isHeaderRow) {
        contextStack.head.readingNames = true
      } else {
        contextStack.head.namesToHide = context.namesToHide
        if (shouldRender_) {
          renderMapStart()
          context.hadContent = true
        }
      }
    } else {
      val name = nextName(context)
      val fieldDef =
        if (context.viewDef != null)
          context.viewDef.fieldOpt(name).orNull
        else null
      val isCollection = fieldDef == null || fieldDef.isCollection
      val shouldRender_ = context.shouldRender && !context.namesToHide.contains(name)
      val viewDef =
        if (shouldRender_ && fieldDef != null && fieldDef.type_.isComplexType)
          getViewDef(fieldDef.type_.name)
        else null
      val allNames = impliedNames(viewDef)
      if (shouldRender_) {
        renderKey(name)
        if (isCollection)
          renderArrayStart()
      }
      contextStack = new Context(isForRows = true, viewDef, allNames, isCollection, shouldRender_) :: contextStack
    }
  }
  override def writeValue(value: Any): Unit = {
    val context = contextStack.head
    if (context.readingNames) {
      context.names = ("" + value) :: context.names
    } else if (!context.isForRows) {
      val name = nextName(context)
      val shouldRender_ = context.shouldRender && !context.namesToHide.contains(name)
      if (shouldRender_) {
        renderKey(name)
        renderRawValue(value)
      }
    } else  {
      renderRawValue(value)
    }
    context.index += 1
  }
  override def startChunks(chunkType: ChunkType): Unit = {
    this.chunkType = chunkType
    buffer = ByteString.empty
  }
  override def writeChunk(chunk: Any): Unit = {
    chunk match {
      case bytes: ByteString =>
        buffer = ByteStringByteAccess.concat(buffer, bytes)
      case x => sys.error("Unsupported chunk class: " + x.getClass.getName)
    }
  }
  override def writeBreak(): Unit = {
    if (chunkType != null) {
      chunkType match {
        case TextChunks => writeValue(buffer.utf8String)
        case ByteChunks => writeValue(buffer.toArrayUnsafe())
        case _ => sys.error("Unsupported ChunkType: " + chunkType)
      }
      chunkType = null
      buffer = null
    } else {
      val context = contextStack.head
      contextStack = contextStack.tail
      if (context.readingNames) {
        val parent = contextStack.head
        parent.names = context.names.reverse
        if (parent.shouldRender) {
          parent.namesToHide = parent.names.filterNot(shouldRender).toSet
          renderHeaderData()
        }
      } else {
        if (context.shouldRender)
          if (context.isCollection)
            renderBreak()
          else if (!context.hadContent)
            renderValue(null)
      }
      level -= 1
    }
    contextStack.head.index += 1
  }
  override def writeEndOfInput(): Unit = {
    if (hasHeaders && contextStack.head.index == 0) // when no rows - handle missing headers
      renderHeaderData()
    renderFooter()
    contextStack = contextStack.tail
  }
}

object ResultRenderer {
  class Context(
    val isForRows:    Boolean,
    val viewDef:      MojozViewDef,
    val allNames:     List[String]  = Nil,
    val isCollection: Boolean       = true,
    val shouldRender: Boolean       = true,
  ) {
    var index:        Int           = 0
    var hadContent:   Boolean       = false
    var readingNames: Boolean       = false
    var names:        List[String]  = allNames
    var namesToHide:  Set[String]   = Set.empty
  }
}

class CborOrJsonResultRenderer(
  w: borer.Writer,
  isCollection: Boolean,
  viewName: String,
  nameToViewDef: Map[String, MojozViewDef],
  hasHeaders: Boolean = true,
) extends ResultRenderer(isCollection, viewName, nameToViewDef, hasHeaders) {
  val valueEncoder = new BorerValueEncoder(w)
  override protected def renderHeader()             = if (isCollection && level <= 1) renderArrayStart()
  override protected def renderFooter()             = if (isCollection) renderBreak()
  override protected def renderArrayStart()         = w.writeArrayStart()
  override protected def renderMapStart()           = w.writeMapStart()
  override protected def renderBreak()              = w.writeBreak()
  override protected def renderValue(value: Any)    = valueEncoder.writeValue(value)
  override protected def renderRawValue(value: Any) = value match {
    case bytes: Array[Byte] if w.writingCbor => w.writeBytes(bytes)
    case _ => super.renderRawValue(value)
  }
  override def startChunks(chunkType: ChunkType): Unit = {
    if (w.writingCbor) {
      chunkType match {
        case TextChunks => if (w.writingCbor && contextStack.head.shouldRender) w.writeTextStart()
        case ByteChunks => if (w.writingCbor && contextStack.head.shouldRender) w.writeBytesStart()
        case _ => sys.error("Unsupported ChunkType: " + chunkType)
      }
      this.chunkType = chunkType
    } else super.startChunks(chunkType)
  }
  override def writeChunk(chunk: Any): Unit = {
    if (w.writingCbor) {
      chunk match {
        case bytes: ByteString =>
          chunkType match {
            case TextChunks => if (contextStack.head.shouldRender) w.writeText(bytes)
            case ByteChunks => if (contextStack.head.shouldRender) w.writeBytes(bytes)
            case _ => sys.error("Unsupported ChunkType: " + chunkType)
          }
        case x => sys.error("Unsupported chunk class: " + x.getClass.getName)
      }
    } else super.writeChunk(chunk)
  }
  override def writeBreak(): Unit = {
    if (chunkType != null && w.writingCbor) {
      if (contextStack.head.shouldRender)
        w.writeBreak()
      chunkType = null
      contextStack.head.index += 1
    } else {
      super.writeBreak()
    }
  }
}

object CborResultRenderer {
  def apply(outputStream: OutputStream, isCollection: Boolean, viewName: String, nameToViewDef: Map[String, MojozViewDef]) =
    new CborOrJsonResultRenderer(BorerNestedArraysEncoder.createWriter(outputStream, Cbor), isCollection, viewName, nameToViewDef)
}

object JsonResultRenderer {
  def apply(outputStream: OutputStream, isCollection: Boolean, viewName: String, nameToViewDef: Map[String, MojozViewDef]) =
    new CborOrJsonResultRenderer(BorerNestedArraysEncoder.createWriter(outputStream, Json), isCollection, viewName, nameToViewDef)
}

class FlatTableResultRenderer(
  renderer: TableResultRenderer,
  viewName: String,
  nameToViewDef: Map[String, MojozViewDef],
  labels: Seq[String] = null,
  hasHeaders: Boolean = true,
) extends ResultRenderer(false, viewName, nameToViewDef, hasHeaders) {
  import renderer._
  private lazy val nameToLabel: Map[String, String] =
    if (viewName != null)
      nameToViewDef.getOrElse(viewName, sys.error(s"View $viewName not found - can not render result"))
        .fields.map { f =>
          f.fieldName -> Option(f.label).getOrElse(f.fieldName)
        }.toMap
    else Map.empty
  protected def label(name: String): String = nameToLabel.getOrElse(name, name)
  override protected def renderHeaderData(): Unit =
    if (level <= 1) {
      val labels =
        if  (this.labels != null)
             this.labels
        else contextStack.head.names.filter(shouldRender).map(label)
      if (labels.nonEmpty) {
        renderRowStart()
        labels foreach { label =>
          renderHeaderCell(label)
        }
        renderRowEnd()
      }
    }
  override protected def shouldRender(name: String): Boolean = {
    val context = contextStack.head
    context.viewDef == null || context.viewDef.fieldOpt(name).exists { f =>
      !f.type_.isComplexType && !f.isCollection
    }
  }
  override protected def renderHeader():  Unit = renderer.renderHeader()
  override protected def renderKey(key: Any): Unit = {}
  override protected def renderValue(value: Any): Unit = {
    if (level == 1) {
      renderCell(value)
    }
  }
  override protected def renderBreak():   Unit = if (level == 1) renderRowEnd()
  override protected def renderFooter():  Unit = renderer.renderFooter()
  override def writeArrayStart(): Unit = {
    super.writeArrayStart()
    if (level == 1 && contextStack.head.shouldRender) {
      renderRowStart()
    }
    if (level == 2 && viewName == null)
      renderCell("") // placeholder for nested when structure unknown
  }
}

trait TableResultRenderer {
  def renderHeader()                = {}
  def renderRowStart()              = {}
  def renderHeaderCell(value: Any)  = { renderCell(value) }
  def renderCell(value: Any): Unit
  def renderRowEnd(): Unit
  def renderFooter()                = {}
}

class CsvResultRenderer(writer: io.Writer) extends TableResultRenderer {
  protected var isAtRowStart = true
  protected def escapeValue(s: String) =
    if (s == null) null
    else if (s.contains(",") || s.contains("\"")) ("\"" + s.replaceAll("\"", "\"\"") + "\"")
    else s
  protected def csvValue(v: Any): String = Option(v).map{
    case n: java.lang.Number => String.valueOf(n)
    case t: Timestamp => xlsxDateTime(t)
    case d: jDate => xsdDate(d)
    case x => x.toString
  }.map(escapeValue).getOrElse("")
  override def renderCell(value: Any) = {
    if (!isAtRowStart)
      writer.write(",")
    writer.write(csvValue(value))
    isAtRowStart = false
  }
  override def renderRowEnd() = {
    writer.write("\n")
    isAtRowStart = true
  }
  override def renderFooter() =
    writer.flush
}

class OdsResultRenderer(zos: ZipOutputStream, worksheetName: String = "data") extends TableResultRenderer {
  import org.wabase.spreadsheet.ods._
  val streamer = new OdsStreamer(zos)
  override def renderHeader() = {
    streamer.startWorkbook
    streamer.startWorksheet
    streamer.startTable(worksheetName)
  }
  override def renderRowStart()             = streamer.startRow
  override def renderHeaderCell(value: Any) = streamer.cell(value) // TODO ods headerStyle
  override def renderCell(value: Any)       = streamer.cell(value)
  override def renderRowEnd()               = streamer.endRow
  override def renderFooter() = {
    streamer.endTable
    streamer.endWorksheet
    streamer.endWorkbook
  }
}

class XlsXmlResultRenderer(writer: io.Writer, worksheetName: String = "data") extends TableResultRenderer {
  import org.wabase.spreadsheet.xlsxml._
  val headerStyle = Style("header", null, Font.BOLD)
  val streamer = new XlsXmlStreamer(writer)
  override def renderHeader() = {
    streamer.startWorkbook(Seq(headerStyle))
    streamer.startWorksheet(worksheetName)
    streamer.startTable
  }
  override def renderRowStart()             = streamer.startRow
  override def renderHeaderCell(value: Any) = streamer.cell(value, headerStyle)
  override def renderCell(value: Any)       = streamer.cell(value)
  override def renderRowEnd()               = streamer.endRow
  override def renderFooter() = {
    streamer.endTable
    streamer.endWorksheet
    streamer.endWorkbook
    writer.flush
  }
}
