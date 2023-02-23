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
import akka.Done

import scala.util.{Failure, Success, Try}

case class InsufficientStorageException(msg: String) extends Exception(msg)

/** Creates {{{FileBufferedFlowStage}}} and sets async boundary around. This is necessary so upstream can
  * bet consumed asynchronously.
  * */
object FileBufferedFlow {
  def create(bufferSize: Int,
             maxFileSize: Long,
             outBufferSize: Int = 1024 * 8): Graph[FlowShape[ByteString, ByteString], Future[IOResult]] =
    Flow.fromGraph(new FileBufferedFlow(bufferSize, maxFileSize, outBufferSize)).async
}

/** Creates flow with non blocking pulling from upstream regardless of downstream demand.
  * Pulled data are stored in buffer of {{{bufferSize}}}. If buffer is full and there is no downstream demand
  * data are stored in file. If file size exceeds {{{maxFileSize}}} {{{InsufficientStorageException}}} is thrown.
  * Flow materializes to {{{Future[IOResult]}}} which completes when upstream is finished.
  * */
class FileBufferedFlow private (bufferSize: Int, maxFileSize: Long, outBufferSize: Int)
  extends GraphStageWithMaterializedValue[FlowShape[ByteString, ByteString], Future[IOResult]] {
  private val in = Inlet[ByteString]("in")
  private val out = Outlet[ByteString]("out")
  override val shape = FlowShape(in, out)

  override def createLogicAndMaterializedValue(attrs: Attributes) = {
    val completionPromise = Promise[IOResult]()
    new GraphStageLogic(shape) with StageLogging {
      private var file: File = _
      private var channel: FileChannel = _
      private var inBytes: ByteStringBuilder = _
      private var writePos = 0L
      private var readPos = 0L
      private var outBytes: ByteBuffer = _
      private var count = 0L

      private def signalUpstreamCompletion(ex: Throwable) =
        if (!completionPromise.isCompleted)
          Option(ex)
            .map(e => completionPromise.success(IOResult(count, Failure(e))))
            .getOrElse(completionPromise.success(IOResult(count, Try(Done))))

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
            val ex = InsufficientStorageException(
              s"Too many bytes '$length'. Max file size limit '$maxFileSize bytes' exceeded for buffer")
            failStage(ex)
            signalUpstreamCompletion(ex)
            false
          } else true

        private def pushBytes(bs: ByteString) = {
          push(out, bs)
        }

        override def onPush(): Unit = {
          val bs = grab(in)
          inBytes ++= bs
          count += bs.length
          val l = inBytes.length
          if (l >= bufferSize) {
            if (isAvailable(out) && writePos == readPos) {
              pushBytes(inBytes.result())
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
            pushBytes(byteStringFromBuffer(outBytes))
            readPos += r
          } else {
            readPos = 0
            writePos = 0
            if (inBytes.nonEmpty) {
              pushBytes(inBytes.result())
              inBytes.clear()
            } else if (isClosed(in)) completeStage()
          }
        }

        override def onUpstreamFinish(): Unit = {
          if (readPos == writePos)
            if (inBytes.isEmpty) completeStage()
            else if (isAvailable(out)) {
              pushBytes(inBytes.result())
              inBytes.clear()
            }
          signalUpstreamCompletion(null)
          log.debug("Upstream finished")
        }

        override def onUpstreamFailure(ex: Throwable): Unit = {
          super.onUpstreamFailure(ex)
          signalUpstreamCompletion(ex)
        }

        override def onDownstreamFinish(cause: Throwable): Unit = {
          super.onDownstreamFinish(cause)
          signalUpstreamCompletion(cause)
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
        signalUpstreamCompletion(null)
      }
    } -> completionPromise.future
  }
}

sealed trait SerializedResult
/** Value of this class can be materialized to {{{HttpEntity.Strict}}} */
case class CompleteResult(result: ByteString) extends SerializedResult
/** Value of this class can be materialized to {{{HttpEntity.Chunked}}} */
case class IncompleteResultSource[Mat](result: Source[ByteString, Mat]) extends SerializedResult

/**
  * Sink materializes to {{{CompleteResult}}} if one and only one element passes from upstream before it is finished.
  * Otherwise produces {{{IncompleteResultSources}}}. Running of {{{IncompleteResultSources}}} source will consume
  * this {{{ResultCompletionSink}}} upstream.
  *
  * @param cleanupFun    cleanupFun is invoked on onUpstreamFinish() and postStop() method calls.
  * @param resultCount   indicates how many copies of {{{SerializedResult}}} sink should materialize.
  *                      This is useful for IncompleteResultSources's result: Source[ByteString, _]
  *                      so that they can be consumed for various
  *                      purposes. Value must be greater than zero.
  * */
class ResultCompletionSink(resultCount: Int = 1)(implicit ec: scala.concurrent.ExecutionContext)
  extends GraphStageWithMaterializedValue[SinkShape[ByteString], Future[Seq[SerializedResult]]] {
  require(resultCount > 0, s"Result count must be greater than zero. ($resultCount < 1)")
  val in = Inlet[ByteString]("in")
  override val shape = SinkShape(in)
  override def createLogicAndMaterializedValue(attrs: Attributes) = {
    val result = Promise[Seq[SerializedResult]]()
    new GraphStageLogic(shape) {
      private var byteString: ByteString = _

      override def preStart() = {
        //generate initial demand since this is sink
        pull(in)
      }

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
          val r = CompleteResult(if (byteString == null) ByteString.empty else byteString)
          result.success(List.fill(resultCount)(r))
        }
        override def onUpstreamFailure(ex: Throwable) = {
          result.failure(ex)
        }
      })
      def streamingHandler(init: ByteString) = new InHandler {
        object Sources {
          val sources: Vector[SourceState] = (0 until resultCount map (new SourceState(_))).toVector
          val demandCallback = getAsyncCallback[Int] { idx =>
            sources(idx).demand = true
            if (sources.forall(s => s.demand || s.completionPromise.isCompleted)) {
              if (!hasBeenPulled(in)) pull(in)
              sources.foreach(_.demand = false)
            }
          }
          def push(elem: ByteString) =
            sources.foreach(s => if (!s.completionPromise.isCompleted) s.pushCallback.invoke(elem))
          def upstreamFinish() = sources.foreach(_.dataCompleted.success(()))
          def upstreamFailure(err: Throwable) = sources.foreach(_.dataCompleted.failure(err))

          class SourceState(idx: Int) {
            val dataCompleted = Promise[Unit]()
            val completionPromise = Promise[Done]()
            var demand = false
            @volatile var pushCallback: AsyncCallback[ByteString] = _

            val source = Source.fromGraph(new GraphStage[SourceShape[ByteString]] {
              val out = Outlet[ByteString]("out")
              override val shape = SourceShape(out)

              override def createLogic(attrs: Attributes) = new GraphStageLogic(shape) {
                override def preStart() = {
                  emit(out, init)
                  pushCallback = getAsyncCallback[ByteString] { elem => push(out, elem) }
                  dataCompleted.future.onComplete {
                    case Success(_) => getAsyncCallback[Unit](_ => completeStage()).invoke(())
                    case Failure(ex) => getAsyncCallback[Unit](_ => failStage(ex)).invoke(())
                  }
                }

                override def postStop(): Unit = {
                  Sources.demandCallback.invoke(idx)
                  completionPromise.success(Done)
                }

                setHandler(out, new OutHandler {
                  override def onPull() = Sources.demandCallback.invoke(idx)
                })
              }
            })
          }
        }

        result.success(Sources.sources.map(s => IncompleteResultSource(s.source)))
        // upon completion of all sources complete stage, this is necessaray in the case all sources are
        // finished before result completion sink upstream is consumed
        Future.foldLeft[Done, Done](Sources.sources
          .map(s => s.completionPromise.future))(Done) { (r, _) => r }
          .onComplete { case _ => getAsyncCallback[Unit](_ => completeStage()).invoke(()) }

        override def onPush() = {
          Sources.push(grab(in))
        }
        override def onUpstreamFinish() = {
          // call cleanup fun also here to ensure that immediate next operations can have cleaned up state.
          Sources.upstreamFinish()
        }
        override def onUpstreamFailure(ex: Throwable) = {
          Sources.upstreamFailure(ex)
        }
      }
    } -> result.future
  }
}
