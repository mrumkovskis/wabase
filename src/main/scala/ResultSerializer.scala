package org.wabase

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet, SourceShape}
import akka.util.{ByteString, ByteStringBuilder}

import java.io.{InputStream, OutputStream}
import java.lang.{Boolean => JBoolean, Byte => JByte, Double => JDouble, Float => JFloat, Long => JLong, Short => JShort}
import java.math.{BigDecimal => JBigDecimal, BigInteger => JBigInteger}

import ResultEncoder._

object ResultSerializer {
  trait ChunkInfo {
    def chunkSize(bufferSize: Int): Int
  }
  class StringChunker(s: String, chunkSize: Int) {
    // chunk strings to enable reactive streaming with limited buffer size
    val bytes = ByteString.fromString(s)
    val shouldChunk = chunkSize != Int.MaxValue && bytes.length > chunkSize
    def chunks: Iterator[ByteString] = new Iterator[ByteString] {
      var remaining = bytes
      override def hasNext: Boolean = remaining.nonEmpty
      override def next(): ByteString = {
        if (remaining.length > chunkSize) {
          val parts = remaining.splitAt(chunkSize)
          remaining = parts._2
          parts._1
        } else {
          val chunk = remaining
          remaining = ByteString.empty
          chunk
        }
      }
    }
  }
  def source(
    createEncodable:  () => Iterator[_],
    createEncoder:    EncoderFactory,
    bufferSizeHint:   Int = 1024,
  ): Source[ByteString, NotUsed] = {
    Source.fromGraph(new ResultSerializer(createEncodable, createEncoder, bufferSizeHint))
  }
}

import ResultSerializer._
/** Serializes nested iterators as nested arrays. To serialize tresql Result, use TresqlRowsIterator */
class ResultSerializer(
  createEncodable:  () => Iterator[_],
  createEncoder:    EncoderFactory,
  bufferSizeHint:   Int = 1024,
) extends GraphStage[SourceShape[ByteString]] {
  val out = Outlet[ByteString]("SerializedArraysTresqlResultSource")
  override val shape: SourceShape[ByteString] = SourceShape(out)
  override def createLogic(attrs: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val buf       = new ByteStringBuilder
    private val encodable = createEncodable()
    private val encoder   = createEncoder(buf.asOutputStream)
    private val chunkSize = encoder match {
      case chunkInfo: ChunkInfo =>
        chunkInfo.chunkSize(bufferSizeHint)
      case _ => Int.MaxValue
    }
    if (chunkSize <= 0) {
      throw new IllegalArgumentException(
        s"Unable to chunk for buffer size $bufferSizeHint. " +
        s"Resulting chunk size is $chunkSize but should be > 0")
    }
    private var isChunking = false
    private var iterators  = encodable :: Nil
    override def preStart(): Unit = {
      buf.sizeHint(bufferSizeHint)
      encoder.writeStartOfInput()
    }
    private def encodeNext(): Unit = {
      val iterator = iterators.head
      if (iterator.hasNext) iterator.next() match {
        case children: Iterator[_] =>
          iterators = children :: iterators
          encoder.writeArrayStart()
        case s: String if chunkSize != Int.MaxValue =>
          val stringChunker = new StringChunker(s, chunkSize)
          if (stringChunker.shouldChunk) {
            isChunking = true
            iterators = stringChunker.chunks :: iterators
            encoder.startChunks(TextChunks)
          } else {
            encoder.writeValue(s)
          }
        // TODO blob / clob etc support
        case value =>
          if (isChunking)
            encoder.writeChunk(value)
          else
            encoder.writeValue(value)
      } else {
        iterators = iterators.tail
        isChunking = false
        if (iterators.nonEmpty)
          encoder.writeBreak()
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
import io.bullet.borer.compat.akka.ByteStringByteAccess
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

class BorerNestedArraysEncoder(
  w: Writer,
  wrap: Boolean = false,
) extends BorerValueEncoder(w) with ResultEncoder with ChunkInfo {
  private  var chunkType: ChunkType = null
  override def writeStartOfInput():     Unit = { if (wrap) w.writeArrayStart() }
  override def writeArrayStart():       Unit = w.writeArrayStart()
  override def writeValue(value: Any):  Unit = super.writeValue(value)
  override def startChunks(chunkType: ChunkType): Unit = {
    chunkType match {
      case TextChunks => w.writeTextStart()
      case ByteChunks => w.writeBytesStart()
      case _ => sys.error("Unsupported ChunkType: " + chunkType)
    }
    this.chunkType = chunkType
  }
  override def writeChunk(chunk: Any):  Unit =
    chunk match {
      case bytes: ByteString =>
        chunkType match {
          case TextChunks => w.writeText(bytes)
          case ByteChunks => w.writeBytes(bytes)
          case _ => sys.error("Unsupported ChunkType: " + chunkType)
        }
      case x => sys.error("Unsupported chunk class: " + x.getClass.getName)
    }
  override def writeBreak():            Unit = {
    chunkType = null
    w.writeBreak()
  }
  override def writeEndOfInput():       Unit = { if (wrap) w.writeBreak() }
  override def chunkSize(bufferSize: Int): Int =
    if      (!w.writingCbor)            Int.MaxValue // borer does not support chunking for json
    else if (bufferSize <= (   23 + 1)) bufferSize - 1
    else if (bufferSize <= (  255 + 2)) bufferSize - 2
    else if (bufferSize <= (65535 + 3)) bufferSize - 3
    else 65535 // enough?
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

class BorerNestedArraysTransformer(reader: Reader, handler: ResultEncoder) {
  import reader._
  import handler._
  import BorerDatetimeDecoders._
  import io.bullet.borer.{DataItem => DI}
  private var di = DI.None
  private var isChunking = false
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
        case DI.Text          => if  (isChunking)
                                      writeChunk(readSizedTextBytes[ByteString]())
                                 else writeValue(readString())
        case DI.TextStart     => readTextStart();   isChunking = true;  startChunks(TextChunks)
        case DI.Bytes         => if  (isChunking)
                                      writeChunk(readSizedBytes[ByteString]())
                                 else writeValue(readByteArray())
        case DI.BytesStart    => readBytesStart();  isChunking = true;  startChunks(ByteChunks)
        case DI.ArrayHeader   => readArrayHeader(); writeArrayStart()
        case DI.ArrayStart    => readArrayStart();  writeArrayStart()
        case DI.MapHeader     => readMapHeader();   writeArrayStart() // unexpected TODO map support?
        case DI.MapStart      => readMapStart();    writeArrayStart() // unexpected TODO map support?
        case DI.Break         => readBreak();       isChunking = false; writeBreak()
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

import io.bullet.borer.compat.akka.ByteStringProvider
object BorerNestedArraysTransformer {
  private class TransformerSource(
    createTransformable:  () => InputStream,
    createEncoder:        EncoderFactory,
    transformFrom:        Target,
    bufferSizeHint:       Int,
  ) extends GraphStage[SourceShape[ByteString]] {
    val out = Outlet[ByteString]("BorerNestedArraysTransformerSource.out")
    override val shape: SourceShape[ByteString] = SourceShape(out)
    override def createLogic(attrs: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
      private val buf         = new ByteStringBuilder
      private val encoder     = createEncoder(buf.asOutputStream)
      private val transformer = new BorerNestedArraysTransformer(
        transformFrom match {
          case _: Cbor.type => Cbor.reader(createTransformable())
          case _: Json.type => Json.reader(createTransformable())
        },
        encoder,
      )
      override def preStart(): Unit = {
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

  private class TransformerFlow(
    createEncoder:  EncoderFactory,
    transformFrom:  Target,
    bufferSizeHint: Int,
  ) extends GraphStage[FlowShape[ByteString, ByteString]] {
    val in = Inlet[ByteString]("BorerNestedArraysTransformerFlow.in")
    val out = Outlet[ByteString]("BorerNestedArraysTransformerFlow.out")
    override val shape = FlowShape.of(in, out)
    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
      // XXX adding empty bytestring to queue because borer reader starts reading in constructor
      private val inQueue           = collection.mutable.Queue(ByteString.empty)
      private var inQueueByteCount  = 0
      private val buf               = new ByteStringBuilder
      private val transformable     = new Iterator[ByteString] {
        override def hasNext  = inQueue.nonEmpty || !isClosed(in)
        override def next()   = {
          if (inQueue.isEmpty) sys.error(
            "TransformerFlow input buffer exhausted. " +
            "Please use output of chunking serializer with matching buffer size" +
            " or split at (atomic) data item boundaries"
          )
          val elem = inQueue.dequeue()
          inQueueByteCount -= elem.size
          elem
        }
      }
      private val encoder     = createEncoder(buf.asOutputStream)
      private val transformer = new BorerNestedArraysTransformer(
        transformFrom match {
          case _: Cbor.type => Cbor.reader(transformable)
          case _: Json.type => Json.reader(transformable)
        },
        encoder,
      )
      override def preStart(): Unit = {
        buf.sizeHint(bufferSizeHint)
        encoder.writeStartOfInput()
      }
      private def transform(): Unit = {
        while ((inQueueByteCount >= bufferSizeHint || isClosed(in)) &&
               buf.length < bufferSizeHint && transformer.transformNext()) {}
        if (inQueueByteCount < bufferSizeHint && !isClosed(in) && !hasBeenPulled(in))
          pull(in)
        if (buf.nonEmpty) {
          if (isAvailable(out)) {
            val chunk = buf.result()
            buf.clear()
            push(out, chunk)
          }
        } else if (isClosed(in)) {
          completeStage()
        }
      }
      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val elem = grab(in)
          inQueue.enqueue(elem)
          inQueueByteCount += elem.size
          transform()
        }
        override def onUpstreamFinish(): Unit = {
          transform()
        }
      })
      setHandler(out, new OutHandler {
        override def onPull(): Unit =
          transform()
      })
    }
  }

  def source(
    createTransformable:  () => InputStream,
    createEncoder:        EncoderFactory,
    transformFrom:        Target = Cbor,
    bufferSizeHint:       Int = 1024,
  ): Source[ByteString, NotUsed] = Source.fromGraph(
    new TransformerSource(createTransformable, createEncoder, transformFrom, bufferSizeHint)
  )

  def flow(
    createEncoder:  EncoderFactory,
    transformFrom:  Target = Cbor,
    bufferSizeHint: Int = 1024,
  ): Flow[ByteString, ByteString, NotUsed] = Flow.fromGraph(
    new TransformerFlow(createEncoder, transformFrom, bufferSizeHint)
  )

  def transform[T](
    transformable:  T,
    createEncoder:  EncoderFactory,
    transformFrom:  Target = Cbor,
  )(implicit p: Input.Provider[T]): ByteString = {
    val buf = new ByteStringBuilder
    val encoder = createEncoder(buf.asOutputStream)
    val transformer = new BorerNestedArraysTransformer(
      transformFrom match {
        case _: Cbor.type => Cbor.reader(transformable)
        case _: Json.type => Json.reader(transformable)
      },
      encoder,
    )
    encoder.writeStartOfInput()
    while (transformer.transformNext()) {}
    buf.result()
  }
}

object DtoDataSerializer {
  /** Dto iterator wrapper for data serialization (without field names) */
  private class DtoDataIterator(
    items:  Iterator[_],
    asRows: Boolean,
    includeHeaders: Boolean,
  )(implicit val qe: AppQuerease) extends Iterator[Any] with AutoCloseable {
    private  var isBeforeFirst = true
    private  var headering = false
    private  var nextItem: Any = null
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
  def source(
    createResult:   () => Iterator[_],
    includeHeaders: Boolean = true,
    bufferSizeHint: Int     = 1024,
    createEncoder:  EncoderFactory = BorerNestedArraysEncoder(_),
  )(implicit
    qe: AppQuerease,
  ): Source[ByteString, NotUsed] = {
    ResultSerializer.source(
      () => new DtoDataIterator(createResult(), asRows = true, includeHeaders),
      createEncoder(_),
      bufferSizeHint,
    )
  }
}

import org.tresql.Result
object TresqlResultSerializer {
  /** Tresql Result wrapper for serialization - returns column iterator instead of self */
  private class TresqlRowsIterator(
    rows: Result[_],
    includeHeaders: Boolean,
  ) extends Iterator[TresqlColsIterator] with AutoCloseable {
    private  var isBeforeFirst = true
    private  var headering = false
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
  private class TresqlColsIterator(cols: Iterator[_], includeHeaders: Boolean) extends Iterator[Any] {
    override def hasNext: Boolean = cols.hasNext
    override def next(): Any = cols.next() match {
      case rows: Result[_] => new TresqlRowsIterator(rows, includeHeaders)
      case value => value
    }
  }
  def source(
    createResult:   () => Result[_],
    includeHeaders: Boolean = true,
    bufferSizeHint: Int     = 1024,
    createEncoder:  EncoderFactory = BorerNestedArraysEncoder(_),
  ): Source[ByteString, NotUsed] = {
    ResultSerializer.source(
      () => new TresqlRowsIterator(createResult(), includeHeaders), createEncoder(_), bufferSizeHint
    )
  }
}
