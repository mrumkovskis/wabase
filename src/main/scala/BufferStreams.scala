package org.wabase

import scala.concurrent.{Future, Promise}
import akka.stream._
import akka.stream.scaladsl._
import akka.stream.stage._
import akka.util.{ByteString, ByteStringBuilder}
import java.io.File
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption

import akka.NotUsed

case class InsufficientStorageException(msg: String) extends Exception(msg)

/** Creates {{{FileBufferedFlowStage}}} and sets async boundary around. This is necessary so upstream can
  * bet consumed asynchronously.
  * */
object FileBufferedFlow {
  def create(bufferSize: Int, maxFileSize: Long, outBufferSize: Int = 1024 * 8): Graph[FlowShape[ByteString, ByteString], NotUsed] =
    Flow.fromGraph(new FileBufferedFlow(bufferSize, maxFileSize, outBufferSize)).async
}
/** Creates flow with non blocking pulling from upstream regardless of downstream demand.
  * Pulled data are stored in buffer of {{{bufferSize}}}. If buffer is full and there is no downstream demand
  * data are stored in file. If file size exceeds {{{maxFileSize}}} {{{InsufficientStorageException}}} is thrown.
  * */
class FileBufferedFlow private (bufferSize: Int, maxFileSize: Long, outBufferSize: Int)
  extends GraphStage[FlowShape[ByteString, ByteString]] {
  private val in = Inlet[ByteString]("in")
  private val out = Outlet[ByteString]("out")
  override val shape = FlowShape(in, out)

  override def createLogic(attrs: Attributes): GraphStageLogic = new GraphStageLogic(shape) with StageLogging {
    private var file: File = _
    private var channel: FileChannel = _
    private var inBytes: ByteStringBuilder = _
    private var writePos = 0L
    private var readPos = 0L
    private var outBytes: ByteBuffer = _

    override def preStart(): Unit = {
      //generate initial demand
      pull(in)
      inBytes = new ByteStringBuilder
    }

    setHandlers(in, out, new InHandler with OutHandler {
      private def getChannel =
        if (channel == null) {
          file = File.createTempFile("buffered_flow", ".data")
          channel = FileChannel.open(file.toPath, StandardOpenOption.READ, StandardOpenOption.WRITE)
          outBytes = ByteBuffer.allocate(outBufferSize)
          log.debug(s"File data buffer created $file")
          channel
        } else channel

      private def checkFileSize(length: Long) =
        if (length > maxFileSize) {
          failStage(new InsufficientStorageException(
            s"Too many bytes '$length'. Max file size limit '$maxFileSize bytes' exceeded for buffer"))
          false
        } else true

      override def onPush(): Unit = {
        inBytes ++= grab(in)
        val l = inBytes.length
        if (l >= bufferSize) {
          if (isAvailable(out) && writePos == readPos) {
            push(out, inBytes.result())
          } else if (checkFileSize(l + writePos)) {
            val bytes = inBytes.result().asByteBuffer
            while(bytes.hasRemaining) writePos += getChannel.write(bytes, writePos)
          }
          inBytes.clear()
        }
        //check whether port is open because checkFileSize method can fail stage
        if (!isClosed(in)) pull(in)
      }

      override def onPull(): Unit = {
        def byteStringFromBuffer(buf: ByteBuffer) = {
          buf.limit(buf.position())
          buf.position(0)
          ByteString(buf)
        }
        if (readPos < writePos) {
          outBytes.clear
          val diff = writePos - readPos
          if (diff < outBytes.capacity()) {
            outBytes.limit(diff.toInt)
          }
          val r = channel.read(outBytes, readPos)
          push(out, byteStringFromBuffer(outBytes))
          readPos += r
        } else {
          readPos = 0
          writePos = 0
          if (inBytes.nonEmpty) {
            push(out, inBytes.result())
            inBytes.clear()
          } else if (isClosed(in)) completeStage()
        }
      }

      override def onUpstreamFinish(): Unit = {
        if (readPos == writePos)
          if (inBytes.isEmpty) completeStage()
          else if (isAvailable(out)) {
            push(out, inBytes.result())
            inBytes.clear()
          }
        log.debug("Upstream finished")
      }
    })

    override def postStop(): Unit = {
      try if (channel != null) channel.close() finally {
        if (file != null) {
          val s = file.length
          if (file.delete) log.debug(s"File data buffer (${s} bytes) deleted $file")
          else log.error(s"File data buffer $file delete attempt failed")
        }
        inBytes.clear()
      }
    }
  }
}

import scala.util.{Success, Failure}

sealed trait SerializedResult
/** Value of this class can be materialized to {{{HttpEntity.Strict}}} */
case class CompleteResult(result: ByteString) extends SerializedResult
/** Value of this class can be materialized to {{{HttpEntity.Chunked}}} */
case class IncompleteResultSource[Mat](result: Source[ByteString, Mat]) extends SerializedResult

/**
 * Sink materializes to {{{CompleteResult}}} if one and only one element passes from upstream before it is finished.
 * Otherwise produces {{{IncompleteResultSource}}}. Running of {{{IncompleteResultSource}}} source will consume
 * this {{{ResultCompletionSink}}} upstream.
 *
 * @param cleanupFun    cleanupFun is invoked on onUpstreamFinish() and postStop() method calls.
 * @param resultCount   indicates how many copies of SerializedResult sink should materialize. This is useful for
 *                      IncompleteResultSource's result: Source[ByteString, _] so that they can be consumed for various
 *                      purposes. Value must be greater than zero.
 * */
class ResultCompletionSink(cleanupFun: Option[Throwable] => Unit = null, resultCount: Int = 1)(implicit ec: scala.concurrent.ExecutionContext)
  extends GraphStageWithMaterializedValue[SinkShape[ByteString], Future[Seq[SerializedResult]]] {
  require(resultCount > 0, s"Result count must be greater than zero. ($resultCount < 1)")
  val in = Inlet[ByteString]("in")
  override val shape = SinkShape(in)
  override def createLogicAndMaterializedValue(attrs: Attributes) = {
    val result = Promise[Seq[SerializedResult]]()
    new GraphStageLogic(shape) {
      private var upstreamError: Throwable = _
      private var downStreamError: Throwable = _ // can happen on IncompleteResultSource downstream failure
      private var byteString: ByteString = _
      private def setDownstreamError(e: Throwable) = this.downStreamError = e

      private def cleanup(): Unit = Option(cleanupFun).foreach(_(Option(upstreamError) orElse Option(downStreamError)))

      override def preStart() = {
        //generate initial demand since this is sink
        pull(in)
      }
      override def postStop() = cleanup()

      setHandler(in, new InHandler {
        override def onPush() = {
          val bs = grab(in)
          if (byteString == null) {
            byteString = bs
            pull(in)
          } else {
            //set new handler and create source
            setHandler(in, streamingHandler(byteString ++ bs))
          }
        }
        override def onUpstreamFinish() = {
          // call cleanup fun also here to ensure that immediate next operations can have cleaned up state.
          cleanup()
          result.success(List.fill(resultCount)(CompleteResult(if (byteString == null) ByteString.empty else byteString)))
        }
        override def onUpstreamFailure(ex: Throwable) = {
          upstreamError = ex
          result.failure(ex)
        }
      })
      def streamingHandler(init: ByteString) = new InHandler {
        object Sources {
          val sources: Vector[SourceState] = (0 until resultCount map (new SourceState(_))).toVector
          val demandCallback = getAsyncCallback[Int] { idx =>
            sources(idx).demand = true
            if (sources.forall(_.demand)) {
              pull(in)
              sources.foreach(_.demand = false)
            }
          }
          val downstreamErrorCallback = getAsyncCallback(setDownstreamError)
          def push(elem: ByteString) = sources.foreach(_.pushCallback.invoke(elem))
          def upstreamFinish() = sources.foreach(_.sourceCompleted.success(()))
          def upstreamFailure(err: Throwable) = sources.foreach(_.sourceCompleted.failure(err))

          class SourceState(idx: Int) {
            val sourceCompleted = Promise[Unit]()
            var demand = false
            @volatile var pushCallback: AsyncCallback[ByteString] = _

            val source = Source.fromGraph(new GraphStage[SourceShape[ByteString]] {
              val out = Outlet[ByteString]("out")
              override val shape = SourceShape(out)

              override def createLogic(attrs: Attributes) = new GraphStageLogic(shape) {
                override def preStart() = {
                  emit(out, init)
                  pushCallback = getAsyncCallback[ByteString] { elem => push(out, elem) }
                  sourceCompleted.future.onComplete {
                    case Success(_) => getAsyncCallback[Unit](_ => completeStage()).invoke(())
                    case Failure(ex) => getAsyncCallback[Unit](_ => failStage(ex)).invoke(())
                  }
                }

                setHandler(out, new OutHandler {
                  override def onPull() = Sources.demandCallback.invoke(idx)

                  override def onDownstreamFinish(cause: Throwable): Unit = {
                    Sources.downstreamErrorCallback.invoke(cause)
                    super.onDownstreamFinish(cause)
                  }
                })
              }
            })
          }
        }

        result.success(Sources.sources.map(s => IncompleteResultSource(s.source)))

        override def onPush() = {
          Sources.push(grab(in))
        }
        override def onUpstreamFinish() = {
          // call cleanup fun also here to ensure that immediate next operations can have cleaned up state.
          cleanup()
          Sources.upstreamFinish()
        }
        override def onUpstreamFailure(ex: Throwable) = {
          upstreamError = ex
          Sources.upstreamFailure(ex)
        }
      }
    } -> result.future
  }
}
