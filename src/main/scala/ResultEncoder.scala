package org.wabase

import akka.util.ByteString
import org.wabase.Format.{xlsxDateTime, xsdDate}
import io.bullet.borer
import io.bullet.borer.{Cbor, Decoder, Encoder, Json, Tag, Target, Writer, DataItem => DI}
import io.bullet.borer.compat.akka.ByteStringByteAccess
import io.bullet.borer.compat.akka.ByteStringProvider

import java.io
import java.io.{OutputStream, OutputStreamWriter}
import java.lang.{Boolean => JBoolean, Double => JDouble, Long => JLong}
import java.math.{BigDecimal => JBigDecimal, BigInteger => JBigInteger}
import java.time.{LocalDate, LocalDateTime, LocalTime}
import java.util.zip.ZipOutputStream
import org.mojoz.metadata.{Type, ViewDef}
import org.wabase.ResultEncoder.{ByteChunks, ChunkType, TextChunks}

import scala.collection.immutable.{ListMap, Seq}
import AppMetadata.AugmentedAppFieldDef
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model.{ContentType, ContentTypes}
import org.wabase.ResultRenderers.EncoderFactoryCreator

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer

object ResultEncoder {
  type EncoderFactory = OutputStream => ResultEncoder
  trait ChunkType
  object TextChunks extends ChunkType
  object ByteChunks extends ChunkType

  /** Implement this type to create extendable json encoder from scala value.
    * See [[JsonEncoder.extendableJsValueEncoderPF]]
    */
  type JsValueEncoderPF = Writer => PartialFunction[Any, Writer]
  /**
    * Creates {{{io.bullet.borer.Encoder[Any]}}} encoder from [[JsValueEncoderPF]]
    * Designed to encode scala structure representing json value.
    * Primitive types: {{{String, Number, Boolean, null}}}
    * Complex types: {{{Iterable[_], Map[String, _]}}}
    * */
  implicit def jsValEncoder(implicit jsEncoderPF: JsValueEncoderPF): Encoder[Any] = jsEncoderPF(_)(_)

  def encodeJsValue[T: Encoder](value: T): Array[Byte] = Json.encode(value).toByteArray

  object JsonEncoder {
    /**
      * Default scala value json encoder as a partial function.
      * */
    implicit lazy val jsValueEncoderPF: JsValueEncoderPF =
      extendableJsValueEncoderPF(jsValueEncoderPF)(_ => PartialFunction.empty)
    /**
      * Creates json value encoder as partial function, see [[jsValueEncoderPF]].
      * Default implemention encodes scala values - {{{String, Number, Boolean, null, Map[String, Any], Iterable[Any]}}}.
      * @param encoder        value returned by this method, used for recursive calls for nested structures.
      * @param customEncoder  custom value encoder. Can be used to encode non standard values like
      *                       [[java.sql.Date]] etc.
      * */
    def extendableJsValueEncoderPF(encoder: => JsValueEncoderPF)(customEncoder: JsValueEncoderPF): JsValueEncoderPF =
      w => {
        val defaultEncoder: JsValueEncoderPF = w => {
            case v: String => w.writeString(v)
            case v: Number => w.writeNumberString(v.toString)
            case v: Boolean => w.writeBoolean(v)
            case null => w.writeNull()
            case v: Map[Any@unchecked, Any@unchecked] => w.writeMap(v)(jsValEncoder(encoder), jsValEncoder(encoder))
            case v: Iterable[Any@unchecked] => w.writeIterator(v.iterator)(jsValEncoder(encoder))
            case v: Iterator[Any@unchecked] => w.writeIterator(v)(jsValEncoder(encoder))
            case v => w.writeString(v.toString)
          }
        customEncoder(w) orElse defaultEncoder(w)
      }
  }
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
  resultFilter: ResultRenderer.ResultFilter,
  hasHeaders: Boolean,
) extends ResultEncoder {
  import ResultRenderer.Context
  protected var contextStack: List[Context] = Nil
  protected var chunkType: ChunkType = null
  protected var buffer: ByteString = null
  protected var level = 0
  protected var unwrapJson = false
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
    context.resultFilter == null || context.resultFilter.shouldRender(name)
  }
  protected def childFilter(resFilter: ResultRenderer.ResultFilter, fieldName: String) =
    if (resFilter != null)
      resFilter.childFilter(fieldName)
    else null
  protected def impliedNames(resFilter: ResultRenderer.ResultFilter): List[String] = {
    if  (hasHeaders || resFilter == null) Nil
    else resFilter.unfilteredNames
  }
  protected def nextName(context: Context) = {
    if (context.names.nonEmpty) {
      val name = context.names.head
      context.names = context.names.tail
      name
    } else null
  }
  override def writeStartOfInput(): Unit = {
    val allNames = impliedNames(resultFilter)
    contextStack = new Context(isForRows = true, resultFilter, allNames, isCollection = isCollection) :: contextStack
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
        isForRows = false, context.resultFilter, context.names, shouldRender = shouldRender_) :: contextStack
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
      val resFilter = context.resultFilter
      val isCollection = resFilter == null || resFilter.isCollection(name)
      val shouldRender_ = context.shouldRender && !context.namesToHide.contains(name)
      val childFilter =
        if (shouldRender_ && resFilter != null && resFilter.type_(name).isComplexType)
          resFilter.childFilter(name)
        else null
      val allNames = impliedNames(childFilter)
      if (shouldRender_) {
        renderKey(name)
        if (isCollection)
          renderArrayStart()
      }
      contextStack = new Context(isForRows = true, childFilter, allNames, isCollection, shouldRender_) :: contextStack
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
        if (unwrapJson && context.resultFilter != null && context.resultFilter.type_(name).name == "json") {
          value match {
            case null => renderRawValue(null)
            case jsonString: String =>
              new ResultRenderer.JsonForwarder(this).forwardJson(ByteString(jsonString))
            case x => sys.error(s"Unexpected value class for json type: ${x.getClass.getName}")
          }
        } else renderRawValue(value)
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
    val resultFilter: ResultRenderer.ResultFilter,
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

  trait ResultFilter {
    def name: String
    def shouldRender(field: String): Boolean
    def isCollection(field: String): Boolean
    def type_       (field: String): Type
    def childFilter (field: String): ResultFilter
    def unfilteredNames: List[String]
  }

  class ViewFieldFilter(viewName: String, nameToViewDef: Map[String, ViewDef]) extends ResultFilter {
    protected val viewDef =
      nameToViewDef.getOrElse(viewName, sys.error(s"View $viewName not found - can not render result"))
    override def name = viewName
    override def shouldRender(field: String) = viewDef.fieldOpt(field).exists(!_.api.excluded)
    override def isCollection(field: String) = viewDef.fieldOpt(field).exists(_.isCollection)
    override def type_       (field: String) = viewDef.fieldOpt(field).map(_.type_).orNull
    override def childFilter (field: String) = viewDef.fieldOpt(field)
      .map(_.type_.name)
      .map(new ViewFieldFilter(_, nameToViewDef))
      .orNull
    override def unfilteredNames = viewDef.fields.map(_.fieldName).toList
  }

  class IntersectionFilter(filter1: ResultFilter, filter2: ResultFilter) extends ResultFilter {
    override def name = s"(${filter1.name}, ${filter2.name})"
    override def shouldRender(field: String) = filter1.shouldRender(field) && filter2.shouldRender(field)
    override def isCollection(field: String) = filter2.isCollection(field)
    override def type_       (field: String) = filter2.type_(field)
    override def childFilter (field: String) =
      new IntersectionFilter(filter1.childFilter(field), filter2.childFilter(field))
    override def unfilteredNames = filter1.unfilteredNames
  }

  class JsonForwarder(renderer: ResultRenderer) {
    val anyValueForwarder: Decoder[Any] = Decoder { r =>
      import renderer.{renderRawValue => wv, renderArrayStart => wa, renderBreak => wb}
      import BorerDatetimeDecoders._
      r.dataItem() match {
        case DI.Null          => wv(r.readNull())
        case DI.Undefined     => wv{r.readUndefined(); null}
        case DI.Boolean       => wv(r[Boolean])
        case DI.Int           => wv(r[Int])
        case DI.Long          => wv(r[Long])
        case DI.OverLong      => wv(r[JBigInteger])
        case DI.Float16       => wv(r[Float])
        case DI.Float         => wv(r[Float])
        case DI.Double        => wv(r[Double])
        case DI.NumberString  => wv(r[JBigDecimal])
        case DI.String        => wv(r[String])
        case DI.Chars         => wv(r[String])
        case DI.Text          => wv(r[String])
        case DI.TextStart     => wv(r[String])
        case DI.Bytes         => wv(r[Array[Byte]])
        case DI.BytesStart    => wv(r[Array[Byte]])
        case DI.ArrayHeader   => implicit val d = anyValueForwarder; wa(); r[Array[Any]]; wb()
        case DI.ArrayStart    => implicit val d = anyValueForwarder; wa(); r[Array[Any]]; wb()
        case DI.MapHeader     => implicit val d = mapForwarder;      r[Any];
        case DI.MapStart      => implicit val d = mapForwarder;      r[Any];
        case DI.Tag           =>
          if      (r.hasTag(Tag.PositiveBigNum))   wv(r[JBigInteger])
          else if (r.hasTag(Tag.NegativeBigNum))   wv(r[JBigInteger])
          else if (r.hasTag(Tag.DecimalFraction))  wv(r[JBigDecimal])
          else if (r.hasTag(Tag.DateTimeString))   wv(r[LocalDateTime])
          else if (r.hasTag(Tag.EpochDateTime)) {
            r.readTag()
            r.dataItem() match {
              case DI.Int | DI.Long   => wv(r[LocalDate])
              case _                  => wv(r[LocalDateTime])
            }
          }
          else if (r.hasTag(BorerDatetimeEncoders.TimeTag)) wv(r[LocalTime])
          else wv(r[String])
        case DI.SimpleValue   => wv(r[Int])
      }
      null
    }

    val mapForwarder: Decoder[Any] = Decoder { r =>
      import renderer.{renderKey, renderMapStart, renderBreak}
      def forwardKeyValue(): Unit = {
        renderKey(r.readString())
        r[Any](anyValueForwarder)
      }
      if (r.hasMapHeader) {
        @tailrec def rec(remaining: Int): Any = {
          if (remaining > 0) { forwardKeyValue(); rec(remaining - 1) } else null
        }
        val size = r.readMapHeader()
        renderMapStart()
        if (size <= Int.MaxValue) rec(size.toInt)
        else r.overflow(s"Cannot deserialize Map with size $size (> Int.MaxValue)")
        renderBreak()
      } else if (r.hasMapStart) {
        @tailrec def rec(): Unit =
          if (r.tryReadBreak()) ()
          else { forwardKeyValue(); rec() }
        r.readMapStart()
        renderMapStart()
        rec()
        renderBreak()
      } else r.unexpectedDataItem(expected = "Map")
    }

    protected def reader(data: ByteString, decodeFrom: Target) = decodeFrom match {
      case _: Cbor.type => Cbor.reader(data)
      case _: Json.type => Json.reader(data, Json.DecodingConfig.default.copy(
        maxNumberAbsExponent = 308, // to accept up to Double.MaxValue
      ))
    }

    def forwardJson(
      data:       ByteString,
      decodeFrom: Target = Json,
    ): Unit = {
      implicit val decoder = anyValueForwarder
      reader(data, decodeFrom)[Any]
    }
  }
}

class CborOrJsonResultRenderer(
  w: borer.Writer,
  isCollection: Boolean,
  resultFilter: ResultRenderer.ResultFilter,
  hasHeaders: Boolean = true,
) extends ResultRenderer(isCollection, resultFilter, hasHeaders) {
  val valueEncoder = new BorerValueEncoder(w)
  unwrapJson = !w.writingCbor // TODO unwrap json for cbor, too
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
  def apply(outputStream: OutputStream, isCollection: Boolean, resultFilter: ResultRenderer.ResultFilter) =
    new CborOrJsonResultRenderer(BorerNestedArraysEncoder.createWriter(outputStream, Cbor), isCollection, resultFilter)
}

object JsonResultRenderer {
  def apply(outputStream: OutputStream, isCollection: Boolean, resultFilter: ResultRenderer.ResultFilter) =
    new CborOrJsonResultRenderer(BorerNestedArraysEncoder.createWriter(outputStream, Json), isCollection, resultFilter)
}

class FlatTableResultRenderer(
  renderer: TableResultRenderer,
  resultFilter: ResultRenderer.ResultFilter,
  viewDef: ViewDef = null,
  labels: Seq[String] = null,
  hasHeaders: Boolean = true,
) extends ResultRenderer(false, resultFilter, hasHeaders) {
  import renderer._
  private lazy val nameToLabel: Map[String, String] =
    if (viewDef != null)
      viewDef.fields.map { f =>
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
    val rf = context.resultFilter
    rf == null || (rf.shouldRender(name) && !rf.isCollection(name) && !rf.type_(name).isComplexType)
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
    if (level == 2 && resultFilter == null)
      renderCell("") // placeholder for nested when structure unknown
  }
}

class FormUrlEncoder(
  os: OutputStream,
  isCollection: Boolean,
  resultFilter: ResultRenderer.ResultFilter,
  hasHeaders: Boolean = true
) extends ResultRenderer(isCollection, resultFilter, hasHeaders) {
  private[this] val data: ListBuffer[(String, String)] = ListBuffer()
  private[this] var param: String = null

  override def renderKey(key: Any): Unit = param = String.valueOf(key)
  override def renderValue(value: Any): Unit = {
    data += (param -> String.valueOf(value))
    param = null
  }
  override def renderFooter(): Unit = {
    os.write(Query(data.toSeq: _*).toString.getBytes("UTF8"))
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

class ResultRenderers {
  def renderers: ListMap[ContentType, EncoderFactoryCreator] = ResultRenderers.renderers
}

object ResultRenderers {
  type EncoderFactoryCreator = (Boolean, ResultRenderer.ResultFilter, ViewDef) => EncoderFactory

  import akka.http.scaladsl.model.MediaTypes._
  val renderers: ListMap[ContentType, EncoderFactoryCreator] =
    ListMap(
      (`application/json`,                                createJsonEncoderFactory),
      (`application/cbor`,                                createCborEncoderFactory),
      (ContentTypes.`text/csv(UTF-8)`,                           createCsvEncoderFactory),
      (`application/vnd.oasis.opendocument.spreadsheet`,  createOdsEncoderFactory),
      (`application/vnd.ms-excel`,                        createXlsXmlEncoderFactory),
      (`application/x-www-form-urlencoded`,               createFormUrlEncodedFactory),
    )
  def createJsonEncoderFactory(isCollection: Boolean, resultFilter: ResultRenderer.ResultFilter,
                               viewDef: ViewDef): EncoderFactory =
    JsonResultRenderer(_, isCollection, resultFilter)

  def createCborEncoderFactory(isCollection: Boolean, resultFilter: ResultRenderer.ResultFilter,
                               viewDef: ViewDef): EncoderFactory =
    CborResultRenderer(_, isCollection, resultFilter)

  def createCsvEncoderFactory(isCollection: Boolean, resultFilter: ResultRenderer.ResultFilter,
                              viewDef: ViewDef): EncoderFactory =
    os => new FlatTableResultRenderer(new CsvResultRenderer(new OutputStreamWriter(os, "UTF-8")),
      resultFilter, viewDef)

  def createOdsEncoderFactory(isCollection: Boolean, resultFilter: ResultRenderer.ResultFilter,
                              viewDef: ViewDef): EncoderFactory =
    os => new FlatTableResultRenderer(new OdsResultRenderer(new ZipOutputStream(os)),
      resultFilter, viewDef)

  def createXlsXmlEncoderFactory(isCollection: Boolean, resultFilter: ResultRenderer.ResultFilter,
                                 viewDef: ViewDef): EncoderFactory =
    os => new FlatTableResultRenderer(new XlsXmlResultRenderer(new OutputStreamWriter(os, "UTF-8")),
      resultFilter, viewDef)
  def createFormUrlEncodedFactory(isCollection: Boolean, resultFilter: ResultRenderer.ResultFilter,
                                  viewDef: ViewDef): EncoderFactory =
    new FormUrlEncoder(_, isCollection, resultFilter)
}
