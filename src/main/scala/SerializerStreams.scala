package org.wabase

import akka.stream.scaladsl.Source
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import akka.stream.{Attributes, Outlet, SourceShape}
import akka.util.{ByteString, ByteStringBuilder}
import io.bullet.borer._
import org.tresql.Result
import org.tresql.RowLike

import java.io.OutputStream
import java.lang.{Boolean => JBoolean, Byte => JByte, Double => JDouble, Float => JFloat, Long => JLong, Short => JShort}
import java.math.{BigDecimal => JBigDecimal, BigInteger => JBigInteger}

object SerializerStreams {

  trait ArrayTreeEncoder {
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

  class BorerArrayTreeEncoder(w: Writer, wrap: Boolean = false) extends ArrayTreeEncoder {
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

  trait ValuesAccessor {
    /** Returns values iterator or null, if item is not values container (is not row) */
    def values(item: Any): Iterator[_]
  }

  object TresqlResultValuesAccessor extends ValuesAccessor {
    override def values(item: Any): Iterator[_] = item match {
      case row: RowLike => row.values.iterator
      case _ => null
    }
  }

  /** Tresql result serialization Source - serializes as nested arrays, column names are not serialized */
  class SerializedArraysTresqlResultSource(
    createEncodable: () => Iterator[_],
    createEncoder: OutputStream => ArrayTreeEncoder,
    valuesAccessor: ValuesAccessor,
    bufferSizeHint: Int,
  ) extends GraphStage[SourceShape[ByteString]] {
    val out = Outlet[ByteString]("SerializedArraysTresqlResultSource")
    override val shape: SourceShape[ByteString] = SourceShape(out)
    override def createLogic(attrs: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
      var buf:        ByteStringBuilder = _
      var encodable:  Iterator[_]       = _
      var encoder:    ArrayTreeEncoder  = _
      var iterators:  List[Iterator[_]] = _
      override def preStart(): Unit = {
        buf       = new ByteStringBuilder
        encodable = createEncodable()
        encoder   = createEncoder(buf.asOutputStream)
        iterators = null
        buf.sizeHint(bufferSizeHint)
      }
      def encodeNext(): Unit = {
        if (iterators == null) {
          iterators = encodable :: Nil
          encoder.writeStartOfInput()
        } else if (iterators.nonEmpty) {
          val iterator = iterators.head
          if (iterator.hasNext) iterator.next() match {
            case row if row == iterator => // hack for tresql result
              iterators = valuesAccessor.values(row) :: iterators
              encoder.writeArrayStart()
            case children: Iterator[_] =>
              iterators = children :: iterators
              encoder.writeArrayStart()
            // TODO blob / clob etc support
            case item =>
              valuesAccessor.values(item) match {
                case null =>
                  encoder.writeValue(item)
                case values =>
                  iterators = values :: iterators
                  encoder.writeArrayStart()
              }
          } else {
            iterators = iterators.tail
            if (iterators.nonEmpty)
              encoder.writeArrayBreak()
            else
              encoder.writeEndOfInput()
          }
        }
      }
      override def postStop(): Unit = encodable match {
        case closeable: AutoCloseable => closeable.close()
        case _ =>
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
  def createBorerSerializedArraysTresqlResultSource(
    createResult: () => Result[_],
    format: Target = Cbor,
    bufferSizeHint: Int = 1024,
    createEncoder: Writer => ArrayTreeEncoder = new BorerArrayTreeEncoder(_),
  ): Source[ByteString, _] = {
    Source.fromGraph(new SerializedArraysTresqlResultSource(
      createResult,
      outputStream => createEncoder(format match {
        case _: Json.type => Json.writer(Output.ToOutputStreamProvider(outputStream, 0, allowBufferCaching = true))
        case _: Cbor.type => Cbor.writer(Output.ToOutputStreamProvider(outputStream, 0, allowBufferCaching = true))
      }),
      TresqlResultValuesAccessor,
      bufferSizeHint,
    ))
  }
}
