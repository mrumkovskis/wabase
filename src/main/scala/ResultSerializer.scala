package org.wabase

import akka.{Done, NotUsed}
import akka.stream.scaladsl.{Flow, Source, StreamConverters}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Materializer, Outlet, SourceShape}
import akka.util.{ByteString, ByteStringBuilder}

import java.io.{InputStream, OutputStream}
import java.lang.{Boolean => JBoolean, Byte => JByte, Double => JDouble, Float => JFloat, Long => JLong, Short => JShort}
import java.math.{BigDecimal => JBigDecimal, BigInteger => JBigInteger}
import java.nio.{ByteBuffer, CharBuffer}
import java.nio.charset.{Charset, CharsetEncoder, CodingErrorAction}
import java.nio.charset.StandardCharsets.UTF_8
import java.time.{Instant, LocalDate, LocalDateTime, LocalTime, OffsetDateTime, ZonedDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import ResultEncoder._
import com.typesafe.scalalogging.Logger

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Failure

object ResultSerializer {
  trait ChunkInfo {
    def chunkSize(bufferSize: Int): Int
    def chunkable(value: Any): Any // returns null for non-chunkable value, potentially chunkable value otherwise
  }
  class ByteStringChunker(bytes: ByteString, chunkSize: Int) {
    // chunk byte arrays to enable reactive streaming with limited buffer size
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
  class ByteArrayChunker(bytes: Array[Byte], chunkSize: Int)
    extends ByteStringChunker(ByteString(bytes), chunkSize)
  class StringChunker(s: String, chunkSize: Int)
  {
    // chunk strings to enable reactive streaming with limited buffer size
    val shouldChunk = chunkSize != Int.MaxValue && (
      s.length > chunkSize || {
        val tryIt = chunks
        tryIt.next()
        tryIt.hasNext
      }
    )
    def chunks: Iterator[ByteString] = new Iterator[ByteString] {
      val in = CharBuffer.wrap(s)
      val out = ByteBuffer.allocate(chunkSize)
      val coder = UTF_8.newEncoder().onMalformedInput(CodingErrorAction.REPLACE)
      var hasMore = true
      override def hasNext: Boolean = hasMore
      override def next(): ByteString = {
        out.clear()
        val coderResult = coder.encode(in, out, true)
        if (!coderResult.isOverflow) {
          hasMore = false
          if (coderResult.isError)
            coderResult.throwException()
        }
        out.flip()
        if (out.limit() == 0)
          throw new IllegalArgumentException(
            s"Chunkable string is not compatible with chunk size $chunkSize.")
        ByteString(out)
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
    private val (chunkSize, chunkInfo) = encoder match {
      case chunkInfo: ChunkInfo =>
        (chunkInfo.chunkSize(bufferSizeHint), chunkInfo)
      case _ => (Int.MaxValue, null)
    }
    if (chunkSize <= 0) {
      throw new IllegalArgumentException(
        s"Unable to chunk for buffer size $bufferSizeHint. " +
        s"Resulting chunk size is $chunkSize but should be > 0")
    }
    private val isChunkingEnabled = chunkSize != Int.MaxValue
    private val neverChunkStringLength = chunkSize / 4   // max 4 utf-8 bytes per codepoint
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
        // TODO blob / clob etc support
        case value =>
          if (isChunking)
            encoder.writeChunk(value)
          else if (isChunkingEnabled)
            chunkInfo.chunkable(value) match {
              case null => encoder.writeValue(value)
              case s: String =>
                if (s.length > neverChunkStringLength) {
                  val stringChunker = new StringChunker(s, chunkSize)
                  if (stringChunker.shouldChunk) {
                    isChunking = true
                    iterators = stringChunker.chunks :: iterators
                    encoder.startChunks(TextChunks)
                  } else encoder.writeValue(value)
                } else encoder.writeValue(value)
              case bytes: Array[Byte] =>
                if (bytes.length > chunkSize) {
                  isChunking = true
                  iterators = new ByteArrayChunker(bytes, chunkSize).chunks :: iterators
                  encoder.startChunks(ByteChunks)
                } else encoder.writeValue(value)
              case x =>
                sys.error(s"Unexpected class of chunkable: ${x.getClass.getName}")
            }
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
      override def onPull(): Unit = {
        while ({ encodeNext(); iterators.nonEmpty && buf.length < bufferSizeHint })()
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
    } else value.toString match {
      case s if s endsWith ".0" =>
        w writeString s.substring(0, 19)
      case s =>
        w writeString s
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
  val TimeTag = Tag.Other(1042)
  implicit val javaSqlTimeEncoder: Encoder[sql.Time] = Encoder { (w, value) =>
    if (w.writingCbor) {
      val seconds = value.getTime.toDouble / 1000 // TODO nanos?
      w writeTag    TimeTag
      w writeDouble seconds
    } else {
      w writeString value.toString
    }
  }
  implicit val javaTimeInstantEncoder: Encoder[Instant] = Encoder { (w, value) =>
    // TODO optimize cbor for Instant - maybe see https://datatracker.ietf.org/doc/draft-ietf-cbor-time-tag/
    w writeString value.toString
  }
  implicit val localDateEncoder: Encoder[LocalDate] =
    Encoder { (w, value) => w ~ sql.Date.valueOf(value) }
  implicit val localTimeEncoder: Encoder[LocalTime] =
    Encoder { (w, value) => w ~ sql.Time.valueOf(value) }
  implicit val localDateTimeEncoder: Encoder[LocalDateTime] =
    Encoder { (w, value) => w ~ sql.Timestamp.valueOf(value) }
  implicit val offsetDateTimeEncoder: Encoder[OffsetDateTime] = Encoder { (w, value) =>
    // TODO optimize cbor for OffsetDateTime - maybe see https://datatracker.ietf.org/doc/draft-ietf-cbor-time-tag/
    w writeString value.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
  }
  implicit val zonedDateTimeEncoder: Encoder[ZonedDateTime] = Encoder { (w, value) =>
    // TODO optimize cbor for ZonedDateTime - maybe see https://datatracker.ietf.org/doc/draft-ietf-cbor-time-tag/
    w writeString value.toString
  }
}

class BorerValueEncoder(w: Writer) {
  import BorerDatetimeEncoders._
  protected lazy val knownValueEncoder: PartialFunction[Any, Unit] =
    initValueEncoder
  protected lazy val valueEncoder: PartialFunction[Any, Unit] = {
    knownValueEncoder orElse anyValueEncoder
  }
  protected def initValueEncoder: PartialFunction[Any, Unit] = {
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
    case value: sql.Time    => w ~ value
    case value: LocalDate   => w ~ value
    case value: LocalTime   => w ~ value
    case value: LocalDateTime => w ~ value
  }
  private val anyValueEncoder: PartialFunction[Any, Unit] = {
    case x                  => w writeString x.toString
  }
  def writeValue(value: Any):  Unit = valueEncoder(value)
}

class BorerNestedArraysEncoder(
  val w: Writer,
  val wrap: Boolean = false,
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
  override def chunkable(value: Any): Any = value match {
    case null           => value
    case s: String      => value
    case b: Array[Byte] => value
    case _ if knownValueEncoder.isDefinedAt(value) => null
    case x              => x.toString
  }
}

object BorerNestedArraysEncoder {
  def apply(
    outputStream: OutputStream,
    format:       Target  = Cbor,
    wrap:         Boolean = false,
    valueEncoderFactory: BorerNestedArraysEncoder => PartialFunction[Any, Unit] = null,
  ) = {
    if (valueEncoderFactory == null)
      new BorerNestedArraysEncoder(createWriter(outputStream, format), wrap = wrap)
    else
      new BorerNestedArraysEncoder(createWriter(outputStream, format), wrap = wrap) {
        val customValueEncoder = valueEncoderFactory(this)
        override def initValueEncoder = customValueEncoder orElse super.initValueEncoder
      }
  }
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
      case DI.Chars |
           DI.String =>
        val s = readString()
        if (s.length > 10 && s.charAt(10) == 'T')
          sql.Timestamp.from(ZonedDateTime.parse(s).toLocalDateTime.atZone(ZoneId.systemDefault()).toInstant) // drops zone info!
        else
          sql.Timestamp.valueOf(s)
      case _ if tryReadTag(Tag.DateTimeString)  =>
        new sql.Timestamp(Format.jsIsoDateTime.parse(readString()).getTime)
      case _ if tryReadTag(Tag.EpochDateTime)   => new Timestamp((readDouble() * 1000).toLong)
      case DI.Double                            => new Timestamp((readDouble() * 1000).toLong)
      case _                                    => unexpectedDataItem(expected = "Timestamp")
    }
  }
  implicit val javaSqlDateDecoder: Decoder[sql.Date] = Decoder { reader =>
    import reader._
    dataItem() match {
      case DI.Chars |
           DI.String => sql.Date.valueOf(readString())
      case _ if tryReadTag(Tag.DateTimeString)  =>
        new sql.Date(Format.jsIsoDateTime.parse(readString()).getTime)
      case _ if tryReadTag(Tag.EpochDateTime)   => new sql.Date(readLong() * 1000)
      case DI.Int | DI.Long                     => new sql.Date(readLong() * 1000)
      case _                                    => unexpectedDataItem(expected = "Date")
    }
  }
  private val hh_mm_regex = """^\d\d?:\d\d?$""".r
  def toSqlTime(timeString: String) =
    if  (hh_mm_regex.pattern.matcher(timeString).matches)
         sql.Time.valueOf(s"$timeString:00")
    else sql.Time.valueOf(timeString)
  implicit val javaSqlTimeDecoder: Decoder[sql.Time] = Decoder { reader =>
    import reader._
    dataItem() match {
      case DI.Chars |
           DI.String => toSqlTime(readString())
      case _ if tryReadTag(Tag.DateTimeString)  =>
        new sql.Time(Format.jsIsoDateTime.parse(readString()).getTime)
      case _ if tryReadTag(BorerDatetimeEncoders.TimeTag)
                                                => new sql.Time((readDouble() * 1000).toLong)
      case _ if tryReadTag(Tag.EpochDateTime)   => new sql.Time((readDouble() * 1000).toLong)
      case DI.Double                            => new sql.Time((readDouble() * 1000).toLong)
      case _                                    => unexpectedDataItem(expected = "Time")
    }
  }
  implicit val javaTimeInstantDecoder: Decoder[Instant] =
    Decoder { r => r[ZonedDateTime].toInstant }
  implicit val localDateDecoder: Decoder[LocalDate] =
    Decoder { r => r[sql.Date].toLocalDate() }
  implicit val localTimeDecoder: Decoder[LocalTime] =
    Decoder { r => r[sql.Time].toLocalTime() }
  implicit val localDateTimeDecoder: Decoder[LocalDateTime] =
    Decoder { r => r[sql.Timestamp].toLocalDateTime() }
  implicit val offsetDateTimeDecoder: Decoder[OffsetDateTime] = Decoder { reader =>
    import reader._
    dataItem() match {
      case DI.Chars | DI.String               => OffsetDateTime.parse(readString())
      case _ if tryReadTag(Tag.DateTimeString)=> OffsetDateTime.parse(readString())
      case _ if tryReadTag(Tag.EpochDateTime) => OffsetDateTime.from(Instant.ofEpochMilli((readDouble() * 1000).toLong))
      case DI.Double                          => OffsetDateTime.from(Instant.ofEpochMilli((readDouble() * 1000).toLong))
      case _                                  => unexpectedDataItem(expected = "OffsetDateTime")
    }
  }
  implicit val zonedDateTimeDecoder: Decoder[ZonedDateTime] = Decoder { reader =>
    import reader._
    dataItem() match {
      case DI.Chars | DI.String               => ZonedDateTime.parse(readString())
      case _ if tryReadTag(Tag.DateTimeString)=> ZonedDateTime.parse(readString())
      case _ if tryReadTag(Tag.EpochDateTime) => ZonedDateTime.from(Instant.ofEpochMilli((readDouble() * 1000).toLong))
      case DI.Double                          => ZonedDateTime.from(Instant.ofEpochMilli((readDouble() * 1000).toLong))
      case _                                  => unexpectedDataItem(expected = "ZonedDateTime")
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
          else if (hasTag(BorerDatetimeEncoders.TimeTag)) read[sql.Time]()
          else readString()                                   // unexpected
        }
        case DI.SimpleValue   => writeValue(readInt())        // unexpected
        case DI.EndOfInput    => readEndOfInput(); writeEndOfInput()
      }
    }
    di != DI.EndOfInput
  }

  def transform(): Unit = {
    handler.writeStartOfInput()
    while (transformNext()) {}
  }
}

import io.bullet.borer.compat.akka.ByteStringProvider
object BorerNestedArraysTransformer {

  private def borerReader[T: Input.Provider](source: T, format: Target) = format match {
    case _: Cbor.type => Cbor.reader(source)
    case _: Json.type => Json.reader(source)
  }

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
        borerReader(createTransformable(), transformFrom),
        encoder,
      )
      override def preStart(): Unit = {
        buf.sizeHint(bufferSizeHint)
        encoder.writeStartOfInput()
      }
      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
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
      private val inMinByteCount    = bufferSizeHint
      private val outBuf            = new ByteStringBuilder
      private val transformable     = new Iterator[ByteString] {
        override def hasNext  = inQueue.nonEmpty || !isClosed(in)
        override def next()   = {
          if (inQueue.isEmpty) sys.error(
            "TransformerFlow input buffer underflow. " +
            "Please use output of chunking serializer with matching buffer size" +
            " or split at (atomic) data item boundaries"
          )
          val elem = inQueue.dequeue()
          inQueueByteCount -= elem.size
          elem
        }
      }
      private val encoder     = createEncoder(outBuf.asOutputStream)
      private val transformer = new BorerNestedArraysTransformer(
        borerReader(transformable, transformFrom),
        encoder,
      )
      override def preStart(): Unit = {
        outBuf.sizeHint(bufferSizeHint * 2)
        encoder.writeStartOfInput()
      }
      private def transform(): Unit = {
        while ((inQueueByteCount >= inMinByteCount || isClosed(in)) &&
               outBuf.length < bufferSizeHint && transformer.transformNext()) {}
        if (inQueueByteCount < inMinByteCount && !isClosed(in) && !hasBeenPulled(in))
          pull(in)
        if (outBuf.nonEmpty) {
          if (isAvailable(out)) {
            val chunk = outBuf.result()
            outBuf.clear()
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

  /** This method is wabase private since returned flow's incoming cbor or json data item (token)
   * size must not exceed parameter [[bufferSizeHint]] value, otherwise flow will fail with
   * [[RuntimeException]] - see [[TransformerFlow.createLogic]].
   *
   * Produced flow is used for performance reasons as an alternative to method [[blockingTransform]]
   * */
  private [wabase] def flow(
    createEncoder:  EncoderFactory,
    transformFrom:  Target = Cbor,
    bufferSizeHint: Int = 1024,
  ): Flow[ByteString, ByteString, NotUsed] = Flow.fromGraph(
    new TransformerFlow(createEncoder, transformFrom, bufferSizeHint)
  )

  def transform[T: Input.Provider](
    transformable:  T,
    createEncoder:  EncoderFactory,
    transformFrom:  Target = Cbor,
  ): ByteString = {
    val buf = new ByteStringBuilder
    val encoder = createEncoder(buf.asOutputStream)
    val transformer = new BorerNestedArraysTransformer(
      borerReader(transformable, transformFrom),
      encoder,
    )
    transformer.transform()
    buf.result()
  }

  private val logger = Logger("borer-nested-arrays-transformer")

  def blockingTransform(
    src: Source[ByteString, _],
    createEncoder: EncoderFactory,
    transformFrom: Target = Cbor,
  )(implicit ec: ExecutionContext, mat: Materializer): Source[ByteString, Future[Done]] = {
    val in = src.runWith(StreamConverters.asInputStream())
    StreamConverters.asOutputStream()
      .mapMaterializedValue { out =>
        Future {
          try new BorerNestedArraysTransformer(borerReader(in, transformFrom), createEncoder(out)).transform()
          finally out.close()
          Done
        }.andThen {
          case Failure(e) => logger.error("Blocking serialized transform error", e)
        }
      }
  }
}

object DataSerializer {
  /** Iterator wrapper for data serialization (without field names) */
  private class DtoDataIterator(
    items:  Iterator[_],
    asRows: Boolean,
    includeHeaders: Boolean,
  ) extends Iterator[Any] with AutoCloseable {
    private  var isBeforeFirst = true
    private  var headering = false
    private  var nextItem: Any = null
    override def hasNext: Boolean = headering || items.hasNext
    override def next() = {
      if (!headering)
        nextItem = items.next()
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
  ): Source[ByteString, NotUsed] = {
    ResultSerializer.source(
      () => new DtoDataIterator(createResult(), asRows = true, includeHeaders),
      createEncoder(_),
      bufferSizeHint,
    )
  }
}

import org.tresql.{Result, RowLike}
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
      if  (headering)
           headerIterator(rows, includeHeaders)
      else valuesIterator(rows, includeHeaders, false)
    }
    override def close(): Unit = rows.close()
  }
  private class TresqlColsIterator(cols: Iterator[_], includeHeaders: Boolean, close: () => Unit) extends Iterator[Any] {
    override def hasNext: Boolean =
      if (cols.hasNext)
        true
      else {
        close()
        false
      }
    override def next(): Any = cols.next() match {
      case rows: Result[_] => new TresqlRowsIterator(rows, includeHeaders)
      case value => value
    }
  }
  private def headerIterator(row: RowLike, includeHeaders: Boolean) =
    new TresqlColsIterator(row.columns.map(_.name).toVector.iterator, includeHeaders, () => {})
  private def valuesIterator(row: RowLike, includeHeaders: Boolean, closeAfterUse: Boolean) = {
    new TresqlColsIterator(row.values.iterator, includeHeaders, () => if (closeAfterUse) row.close())
  }

  def rowSource(
    createResult:   () => RowLike,
    includeHeaders: Boolean = true,
    bufferSizeHint: Int     = 1024,
    createEncoder:  EncoderFactory = BorerNestedArraysEncoder(_),
  ): Source[ByteString, NotUsed] = {
    def createEncodable() = {
      val row = createResult()
      if  (includeHeaders)
           Seq(headerIterator(row, includeHeaders),
               valuesIterator(row, includeHeaders, true)).iterator
      else Seq(valuesIterator(row, includeHeaders, true)).iterator
    }
    ResultSerializer.source(createEncodable, createEncoder(_), bufferSizeHint)
  }
  def source(
    createResult:   () => Result[_],
    includeHeaders: Boolean = true,
    bufferSizeHint: Int     = 1024,
    createEncoder:  EncoderFactory = BorerNestedArraysEncoder(_),
  ): Source[ByteString, NotUsed] = {
    def createEncodable() = new TresqlRowsIterator(createResult(),  includeHeaders)
    ResultSerializer.source(createEncodable, createEncoder(_), bufferSizeHint)
  }
}
