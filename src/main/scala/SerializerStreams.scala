package org.wabase

import akka.NotUsed
import akka.stream.scaladsl.Source
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import akka.stream.{Attributes, Outlet, SourceShape}
import akka.util.{ByteString, ByteStringBuilder}

import java.io.{InputStream, OutputStream}
import java.lang.{Boolean => JBoolean, Byte => JByte, Double => JDouble, Float => JFloat, Long => JLong, Short => JShort}
import java.math.{BigDecimal => JBigDecimal, BigInteger => JBigInteger}

trait NestedArraysHandler {
  def writeStartOfInput():    Unit
  def writeArrayStart():      Unit
  def writeValue(value: Any): Unit
  def writeChunk( // for blob / clob etc support
    chunk:   Any,
    isFirst: Boolean,
    isLast:  Boolean):        Unit = ???
  def writeArrayBreak():      Unit
  def writeEndOfInput():      Unit
}

/** Serializes nested iterators as nested arrays. To serialize tresql Result, use TresqlRowsIterator */
class NestedArraysSerializer(
  createEncodable: () => Iterator[_],
  createEncoder:  OutputStream => NestedArraysHandler,
  bufferSizeHint: Int = 1024,
) extends GraphStage[SourceShape[ByteString]] {
  val out = Outlet[ByteString]("SerializedArraysTresqlResultSource")
  override val shape: SourceShape[ByteString] = SourceShape(out)
  override def createLogic(attrs: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    var buf:        ByteStringBuilder   = _
    var encodable:  Iterator[_]         = _
    var encoder:    NestedArraysHandler = _
    var iterators:  List[Iterator[_]]   = _
    override def preStart(): Unit = {
      buf       = new ByteStringBuilder
      encodable = createEncodable()
      encoder   = createEncoder(buf.asOutputStream)
      iterators = encodable :: Nil
      buf.sizeHint(bufferSizeHint)
      encoder.writeStartOfInput()
    }
    def encodeNext(): Unit = {
      val iterator = iterators.head
      if (iterator.hasNext) iterator.next() match {
        case children: Iterator[_] =>
          iterators = children :: iterators
          encoder.writeArrayStart()
        // TODO blob / clob etc support
        case value =>
          encoder.writeValue(value)
      } else {
        iterators = iterators.tail
        if (iterators.nonEmpty)
          encoder.writeArrayBreak()
        else
          encoder.writeEndOfInput()
      }
    }
    override def postStop(): Unit = iterators.collect {
      case closeable: AutoCloseable => closeable.close()
    }
    setHandler(out, new OutHandler {
      override def onPull: Unit = {
        do encodeNext() while (iterators.nonEmpty && buf.length < bufferSizeHint)
        if (buf.nonEmpty) {
          val chunk = buf.result()
          buf.clear()
          push(out, chunk)
        }
        if (iterators.isEmpty)
          completeStage()
      }
    })
  }
}

import io.bullet.borer._
import java.sql
object BorerDatetimeEncoders {
  implicit val javaSqlTimestampEncoder: Encoder[Timestamp] = Encoder { (w, value) =>
    if (w.writingCbor) {
      val seconds = value.getTime.toDouble / 1000 // TODO nanos?
      w writeTag    Tag.EpochDateTime
      w writeDouble seconds
    } else {
      w writeString value.toString
    }
  }
  implicit val javaSqlDateEncoder: Encoder[sql.Date] = Encoder { (w, value) =>
    if (w.writingCbor) {
      val seconds = value.getTime / 1000
      w writeTag    Tag.EpochDateTime
      w writeLong   seconds
    } else {
      w writeString value.toString
    }
  }
}

class BorerValueEncoder(w: Writer) {
  import BorerDatetimeEncoders._
  def writeValue(value: Any):  Unit = value match {
    case null               => w.writeNull()
    case value: Boolean     => w writeBoolean value
    case value: Char        => w writeChar    value
    case value: Byte        => w writeByte    value
    case value: Short       => w writeShort   value
    case value: Int         => w writeInt     value
    case value: Long        => w writeLong    value
    case value: Float       => w writeFloat   value
    case value: Double      => w writeDouble  value
    case value: String      => w writeString  value
    case value: JBoolean    => w writeBoolean value
    case value: Character   => w writeChar    value
    case value: JByte       => w writeByte    value
    case value: JShort      => w writeShort   value
    case value: Integer     => w writeInt     value
    case value: JLong       => w writeLong    value
    case value: JFloat      => w writeFloat   value
    case value: JDouble     => w writeDouble  value
    case value: BigInt      => w ~ value
    case value: JBigInteger => w ~ value
    case value: BigDecimal  => w ~ value
    case value: JBigDecimal => w ~ value
    case value: Array[Byte] => w ~ value
    case value: Timestamp   => w ~ value
    case value: sql.Date    => w ~ value
    case x                  => w writeString x.toString
  }
}

class BorerNestedArraysEncoder(w: Writer, wrap: Boolean = false) extends BorerValueEncoder(w) with NestedArraysHandler {
  override def writeStartOfInput():     Unit = { if (wrap) w.writeArrayStart() }
  override def writeArrayStart():       Unit = w.writeArrayStart()
  override def writeValue(value: Any):  Unit = super.writeValue(value)
  override def writeChunk(chunk: Any, isFirst: Boolean, isLast: Boolean): Unit = ??? // TODO blob / clob etc support
  override def writeArrayBreak(): Unit = w.writeBreak()
  override def writeEndOfInput(): Unit = { if (wrap) w.writeBreak() }
}

object BorerNestedArraysEncoder {
  def apply(outputStream:  OutputStream, format: Target = Cbor, wrap: Boolean = false) =
    new BorerNestedArraysEncoder(createWriter(outputStream, format), wrap = wrap)
  def createWriter(outputStream:  OutputStream, format: Target = Cbor): Writer = format match {
    case _: Json.type => Json.writer(Output.ToOutputStreamProvider(outputStream, 0, allowBufferCaching = true))
    case _: Cbor.type => Cbor.writer(Output.ToOutputStreamProvider(outputStream, 0, allowBufferCaching = true))
  }
}

object BorerDatetimeDecoders {
  import io.bullet.borer.{DataItem => DI}
  implicit val javaSqlTimestampDecoder: Decoder[Timestamp] = Decoder { reader =>
    import reader._
    dataItem() match {
      case DI.String => sql.Timestamp.valueOf(readString())
      case _ if tryReadTag(Tag.DateTimeString)  =>
        new sql.Timestamp(Format.jsIsoDateTime.parse(readString()).getTime)
      case _ if tryReadTag(Tag.EpochDateTime)   => new Timestamp((readDouble() * 1000).toLong)
      case _                                    => unexpectedDataItem(expected = "Timestamp")
    }
  }
  implicit val javaSqlDateDecoder: Decoder[sql.Date] = Decoder { reader =>
    import reader._
    dataItem() match {
      case DI.String => sql.Date.valueOf(readString())
      case _ if tryReadTag(Tag.DateTimeString)  =>
        new sql.Date(Format.jsIsoDateTime.parse(readString()).getTime)
      case _ if tryReadTag(Tag.EpochDateTime)   => new sql.Date(readLong() * 1000)
      case _                                    => unexpectedDataItem(expected = "Date")
    }
  }
}

class BorerNestedArraysTransformer(reader: Reader, handler: NestedArraysHandler) {
  import reader._
  import handler._
  import BorerDatetimeDecoders._
  import io.bullet.borer.{DataItem => DI}
  private var di = DI.None
  def transformNext(): Boolean = {
    if (di != DI.EndOfInput) {
      di = dataItem()
      di match {
        case DI.Null          => writeValue(readNull())
        case DI.Undefined     => readUndefined();   writeValue(null)  // unexpected
        case DI.Boolean       => writeValue(readBoolean())
        case DI.Int           => writeValue(readInt())        // byte, char, short also here, convert if necessary
        case DI.Long          => writeValue(readLong())
        case DI.OverLong      => writeValue(read[JBigInteger]())
        case DI.Float16       => writeValue(readFloat())
        case DI.Float         => writeValue(readFloat())
        case DI.Double        => writeValue(readDouble())
        case DI.NumberString  => writeValue(read[JBigDecimal]())
        case DI.String        => writeValue(readString())
        case DI.Chars         => writeValue(readString())
        case DI.Text          => writeValue(readString())
        case DI.TextStart     => writeValue(readString())     // TODO blob / clob etc support
        case DI.Bytes         => writeValue(readByteArray())
        case DI.BytesStart    => writeValue(readByteArray())  // TODO blob / clob etc support
        case DI.ArrayHeader   => readArrayHeader(); writeArrayStart()
        case DI.ArrayStart    => readArrayStart();  writeArrayStart()
        case DI.MapHeader     => readMapHeader();   writeArrayStart() // unexpected TODO map support?
        case DI.MapStart      => readMapStart();    writeArrayStart() // unexpected TODO map support?
        case DI.Break         => readBreak();       writeArrayBreak()
        case DI.Tag           => writeValue {
          if      (hasTag(Tag.PositiveBigNum))   read[JBigInteger]()
          else if (hasTag(Tag.NegativeBigNum))   read[JBigInteger]()
          else if (hasTag(Tag.DecimalFraction))  read[JBigDecimal]()
          else if (hasTag(Tag.DateTimeString))   read[Timestamp]()
          else if (hasTag(Tag.EpochDateTime)) {
            readTag()
            dataItem() match {
              case DI.Int | DI.Long   => new sql.Date(readLong() * 1000)
              case _                  => new Timestamp((readDouble() * 1000).toLong)
            }
          }
          else readString()                                   // unexpected
        }
        case DI.SimpleValue   => writeValue(readInt())        // unexpected
        case DI.EndOfInput    => readEndOfInput(); writeEndOfInput()
      }
    }
    di != DI.EndOfInput
  }
}

object BorerNestedArraysTransformer {
  private class TransformerSource(
    createTransformable:  () => InputStream,
    createEncoder:        OutputStream => NestedArraysHandler,
    transformFrom:        Target,
    bufferSizeHint:       Int,
  ) extends GraphStage[SourceShape[ByteString]] {
    val out = Outlet[ByteString]("BorerNestedArraysTransformer")
    override val shape: SourceShape[ByteString] = SourceShape(out)
    override def createLogic(attrs: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
      var buf:          ByteStringBuilder             = _
      var transformer:  BorerNestedArraysTransformer  = _
      override def preStart(): Unit = {
        buf         = new ByteStringBuilder
        val encoder = createEncoder(buf.asOutputStream)
        transformer = new BorerNestedArraysTransformer(
          transformFrom match {
            case _: Cbor.type => Cbor.reader(createTransformable())
            case _: Json.type => Json.reader(createTransformable())
          },
          encoder,
        )
        buf.sizeHint(bufferSizeHint)
        encoder.writeStartOfInput()
      }
      setHandler(out, new OutHandler {
        override def onPull: Unit = {
          while (transformer.transformNext() && buf.length < bufferSizeHint) {}
          if (buf.nonEmpty) {
            val chunk = buf.result()
            buf.clear()
            push(out, chunk)
          } else {
            completeStage()
          }
        }
      })
    }
  }

  def apply(
    createTransformable:  () => InputStream,
    createEncoder:        OutputStream => NestedArraysHandler,
    transformFrom:        Target = Cbor,
    bufferSizeHint:       Int = 1024,
  ): Source[ByteString, NotUsed] = Source.fromGraph(
    new TransformerSource(createTransformable, createEncoder, transformFrom, bufferSizeHint)
  )
}

object DtoDataSerializer {
  /** Dto iterator wrapper for data serialization (without field names) */
  class DtoDataIterator(
    items: Iterator[_],
    asRows: Boolean,
    includeHeaders: Boolean,
  )(implicit val qe: AppQuerease) extends Iterator[Any] with AutoCloseable {
    var isBeforeFirst = true
    var headering = false
    var nextItem: Any = null
    override def hasNext: Boolean = headering || items.hasNext
    override def next() = {
      if (!headering)
        nextItem = items.next()
      nextItem match {
        case child: Dto => nextItem = child.toMap
        case _ =>
      }
      headering = asRows && isBeforeFirst && includeHeaders && nextItem.isInstanceOf[Map[_, _]]
      isBeforeFirst = false
      nextItem match {
        case child: Map[_, _] =>
          if (headering)
            new DtoDataIterator(child.keys.iterator,    asRows = false, includeHeaders)
          else if (asRows)
            new DtoDataIterator(child.values.iterator,  asRows = false, includeHeaders)
          else
            new DtoDataIterator(List(child).iterator,   asRows = true,  includeHeaders)
        case children: Seq[_] =>
            new DtoDataIterator(children.iterator,      asRows = true,  includeHeaders)
        case x => x
      }
    }
    override def close(): Unit = items match {
      case closeable: AutoCloseable => closeable.close()
      case _ =>
    }
  }
  def apply(
    createResult:   () => Iterator[_],
    includeHeaders: Boolean = true,
    bufferSizeHint: Int     = 1024,
    createEncoder:  OutputStream => NestedArraysHandler = BorerNestedArraysEncoder(_),
  )(implicit
    qe: AppQuerease,
  ): Source[ByteString, NotUsed] = {
    Source.fromGraph(new NestedArraysSerializer(
      () => new DtoDataIterator(createResult(), asRows = true, includeHeaders),
      createEncoder(_),
      bufferSizeHint,
    ))
  }
}

import org.tresql.Result
object TresqlResultSerializer {
  /** Tresql Result wrapper for serialization - returns column iterator instead of self */
  class TresqlRowsIterator(
    rows: Result[_],
    includeHeaders: Boolean,
  ) extends Iterator[TresqlColsIterator] with AutoCloseable {
    var isBeforeFirst = true
    var headering = false
    override def hasNext: Boolean = headering || rows.hasNext
    override def next(): TresqlColsIterator = {
      if (!headering)
        rows.next()
      headering = isBeforeFirst && includeHeaders
      isBeforeFirst = false
      if (headering)
        new TresqlColsIterator(rows.columns.map(_.name).toVector.iterator, includeHeaders)
      else
        new TresqlColsIterator(rows.values.iterator, includeHeaders)
    }
    override def close(): Unit = rows.close()
  }
  class TresqlColsIterator(cols: Iterator[_], includeHeaders: Boolean) extends Iterator[Any] {
    override def hasNext: Boolean = cols.hasNext
    override def next(): Any = cols.next() match {
      case rows: Result[_] => new TresqlRowsIterator(rows, includeHeaders)
      case value => value
    }
  }
  def apply(
    createResult:   () => Result[_],
    includeHeaders: Boolean = true,
    bufferSizeHint: Int     = 1024,
    createEncoder:  OutputStream => NestedArraysHandler = BorerNestedArraysEncoder(_),
  ): Source[ByteString, _] = {
    Source.fromGraph(new NestedArraysSerializer(
      () => new TresqlRowsIterator(createResult(), includeHeaders), createEncoder(_), bufferSizeHint
    ))
  }
}
