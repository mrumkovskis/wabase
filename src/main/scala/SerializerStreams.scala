package org.wabase

import akka.stream.scaladsl.Source
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import akka.stream.{Attributes, Outlet, SourceShape}
import akka.util.{ByteString, ByteStringBuilder}

import java.io.OutputStream
import java.lang.{Boolean => JBoolean, Byte => JByte, Double => JDouble, Float => JFloat, Long => JLong, Short => JShort}
import java.math.{BigDecimal => JBigDecimal, BigInteger => JBigInteger}

trait NestedArraysEncoder {
  def writeStartOfInput():    Unit
  def writeArrayStart():      Unit
  def writeValue(value: Any): Unit
  def writeChunk( // for blob / clob etc support
    chunk:   Any,
    isFirst: Boolean,
    isLast:  Boolean):        Unit
  def writeArrayBreak():      Unit
  def writeEndOfInput():      Unit
}

/** Serializes nested iterators as nested arrays. To serialize tresql Result, use TresqlRowsIterator */
class NestedArraysSerializer(
  createEncodable: () => Iterator[_],
  createEncoder:  OutputStream => NestedArraysEncoder,
  bufferSizeHint: Int,
) extends GraphStage[SourceShape[ByteString]] {
  val out = Outlet[ByteString]("SerializedArraysTresqlResultSource")
  override val shape: SourceShape[ByteString] = SourceShape(out)
  override def createLogic(attrs: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    var buf:        ByteStringBuilder   = _
    var encodable:  Iterator[_]         = _
    var encoder:    NestedArraysEncoder = _
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
class BorerNestedArraysEncoder(w: Writer, wrap: Boolean = false) extends NestedArraysEncoder {
  override def writeStartOfInput():     Unit = { if (wrap) w.writeArrayStart() }
  override def writeArrayStart():       Unit = w.writeArrayStart()
  override def writeValue(value: Any):  Unit = value match {
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
    case x                  => w writeString x.toString
  }
  override def writeChunk(chunk: Any, isFirst: Boolean, isLast: Boolean): Unit = ??? // TODO blob / clob etc support
  override def writeArrayBreak(): Unit = w.writeBreak()
  override def writeEndOfInput(): Unit = { if (wrap) w.writeBreak() }
}

import org.tresql.Result
object TresqlResultSerializer {
  /** Tresql Result wrapper for serialization - returns column iterator instead of self */
  class TresqlRowsIterator(rows: Result[_]) extends Iterator[TresqlColsIterator] with AutoCloseable {
    override def hasNext: Boolean = rows.hasNext
    override def next(): TresqlColsIterator = {
      rows.next()
      new TresqlColsIterator(rows.values.iterator)
    }
    override def close(): Unit = rows.close()
  }
  class TresqlColsIterator(cols: Iterator[_]) extends Iterator[Any] {
    override def hasNext: Boolean = cols.hasNext
    override def next(): Any = cols.next() match {
      case rows: Result[_] => new TresqlRowsIterator(rows)
      case value => value
    }
  }
  def apply(
    createResult: () => Result[_],
    format: Target = Cbor,
    bufferSizeHint: Int = 1024,
    createEncoder: Writer => NestedArraysEncoder = new BorerNestedArraysEncoder(_),
  ): Source[ByteString, _] = {
    Source.fromGraph(new NestedArraysSerializer(
      () => new TresqlRowsIterator(createResult()),
      outputStream => createEncoder(format match {
        case _: Json.type => Json.writer(Output.ToOutputStreamProvider(outputStream, 0, allowBufferCaching = true))
        case _: Cbor.type => Cbor.writer(Output.ToOutputStreamProvider(outputStream, 0, allowBufferCaching = true))
      }),
      bufferSizeHint,
    ))
  }
}
