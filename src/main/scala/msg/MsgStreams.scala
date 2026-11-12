package org.wabase.msg

import org.apache.pekko.{Done, NotUsed}
import org.apache.pekko.stream.RestartSettings
import org.apache.pekko.stream.scaladsl.{Flow, Keep, RestartSource, Sink, Source}
import org.apache.pekko.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler, TimerGraphStageLogic}
import org.apache.pekko.stream.{Attributes, FlowShape, Inlet, Materializer, Outlet}

import org.wabase.msg.MsgParams.MsgParamException

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.Failure
import scala.util.control.{NoStackTrace, NonFatal}

/** Stream stages and helpers of msg operations, provider independent. */
object MsgStreams {

  /** Termination of msg read stream - after message marked as last (`to` matched) is emitted,
    * or after `within` (total duration) or `idle` (max gap between messages, restarted on each message) expires.
    *
    * Stage must be placed before any stage with side effects (message processing, sending, committing),
    * so that messages dropped after termination are neither processed nor committed.
    * Idle timer runs only while stage waits for upstream message, time waiting for downstream demand (slow
    * message processing) is not idle time.
    *
    * @param drain  `false` - plain or at most once source: on termination stage completes, upstream is cancelled.
    *               `true` - committable source: on termination stage completes only downstream, upstream is not
    *               cancelled, elements are pulled and dropped (not committed, delivered again by next read),
    *               so that consumer is running while downstream processes and commits emitted messages.
    *               Downstream cancellation is handled the same way. Stage completes when upstream completes -
    *               consumer must be stopped with [[Control.stop]] after stream completes, see [[run]].
    * @param onStop  called once on termination (from stage thread), for example so that upstream stages can skip
    *                processing of elements which will be dropped
    */
  def termination[T](
    within: Option[FiniteDuration],
    idle: Option[FiniteDuration],
    drain: Boolean,
    onStop: () => Unit = () => (),
  ): Flow[(T, Boolean), T, NotUsed] =
    Flow.fromGraph(new Termination[T](within, idle, drain, onStop))

  /** Runs reader source into sink, after stream completes (sink's materialized future, all commits are done)
    * stops consumer with [[Control.stop]] (required for sources terminated by [[termination]] with `drain = true`).
    * Returned future completes after consumer is stopped, with sink's result or stream failure.
    */
  def run[T, M](source: Source[T, Control], sink: Sink[T, Future[M]])(implicit mat: Materializer): Future[M] = {
    val (control, done) = source.toMat(sink)(Keep.both).run()
    done.transformWith { result =>
      control.stop().transform {
        case Failure(stopError) if result.isSuccess => Failure(stopError)
        case _ => result
      }(MsgProvider.syncEc)
    }(MsgProvider.syncEc)
  }

  /** Stops consumer ([[Control.stop]]) after source terminates (completes, fails or is cancelled), for sources run
    * by others (for example, inside [[RestartSource]]), where materialized control is not accessible.
    */
  def stopOnTermination[T](source: Source[T, Control]): Source[T, NotUsed] =
    source.watchTermination()(Keep.both).mapMaterializedValue { case (control, terminated) =>
      terminated.onComplete(_ => control.stop())(MsgProvider.syncEc)
      NotUsed
    }

  /** Permanent failure - repeated if stream is restarted, so it is not restarted: parameter error
    * ([[MsgParamException]]), decoding failure ([[MsgDecodingException]] - the same message would fail again).
    */
  def isPermanent(e: Throwable): Boolean = e.isInstanceOf[MsgParamException] || e.isInstanceOf[MsgDecodingException]

  /** Restarts source on failure with backoff ([[RestartSource.onFailuresWithBackoff]]), source completion is not
    * restarted, permanent failure ([[isPermanent]]) is not restarted.
    * Materialized control stops current source and prevents further restarts.
    * Source can be materialized once (materialized control is shared).
    */
  def restartOnFailure[T](settings: RestartSettings)(source: () => Source[T, Control]): Source[T, Control] = {
    val control = new RestartControl
    val restartSettings = settings.withRestartOn((e: Throwable) => !isPermanent(e))
    RestartSource.onFailuresWithBackoff(restartSettings)(() => source().mapMaterializedValue(control.set))
      .mapMaterializedValue(_ => control)
  }

  /** Failure of reader (consumer), marked by [[readerFailures]] - restarted by [[restartPipeline]] */
  final case class ReaderFailure(cause: Throwable) extends RuntimeException(String.valueOf(cause), cause)
    with NoStackTrace
  /** Failure of message processing (stages between reader and commit), marked by [[processingFailures]] -
    * not restarted by [[restartPipeline]]
    */
  final case class ProcessingFailure(cause: Throwable) extends RuntimeException(String.valueOf(cause), cause)
    with NoStackTrace

  /** Marks failures as [[ReaderFailure]], must be placed directly after reader source of restarted pipeline,
    * see [[restartPipeline]]. Permanent failures ([[isPermanent]]) and [[ProcessingFailure]] (per message
    * evaluation done by the reader) are not reader failures.
    */
  def readerFailures[T]: Flow[T, T, NotUsed] = Flow[T].mapError {
    case e: ProcessingFailure => e
    case e if isPermanent(e) => e
    case NonFatal(e) => ReaderFailure(e)
  }

  /** Marks failures which are not reader failures as [[ProcessingFailure]], must be placed directly before commit
    * stage of restarted pipeline, see [[restartPipeline]].
    */
  def processingFailures[T]: Flow[T, T, NotUsed] = Flow[T].mapError {
    case e@(_: ReaderFailure | _: ProcessingFailure) => e
    case NonFatal(e) => ProcessingFailure(e)
  }

  /** Restarts pipeline (reader source, processing, commit) on failure with backoff - committables of failed
    * consumer cannot be committed, so reading and committing are restarted together, new consumer continues from
    * the committed offset. Pipeline is built for each attempt, its consumer is stopped ([[Control.stop]]) when the
    * attempt terminates.
    *
    * Restarted are reader failures ([[readerFailures]]) and failures of stages after [[processingFailures]]
    * (commit). [[ProcessingFailure]] and pipeline build failure are permanent - stream fails with the cause.
    * Completion is not restarted. After max restarts stream fails with the last failure.
    * Materialized control stops current pipeline and prevents further restarts.
    */
  def restartPipeline[T](settings: RestartSettings)(pipeline: () => Source[T, Control]): Source[T, Control] = {
    val control = new RestartControl
    val restartSettings = settings.withRestartOn((e: Throwable) => !e.isInstanceOf[ProcessingFailure])
    RestartSource.onFailuresWithBackoff(restartSettings) { () =>
      if (control.isStopped) Source.empty[T]
      else try stopOnTermination(pipeline().mapMaterializedValue(control.set))
      catch { case NonFatal(e) => Source.failed[T](ProcessingFailure(e)) }
    }
      .mapError {
        case ReaderFailure(e) => e
        case ProcessingFailure(e) => e
      }
      .mapMaterializedValue(_ => control)
  }

  /** Runs asynchronous calls one after another - next call starts after the previous one completes.
    * For calls of `mapAsync(1)` stage of restarted stream: when stream fails, the call in flight continues,
    * so the first call of the restarted stream would run concurrently with it (on the same db connection).
    */
  final class Sequential {
    private var last: Future[Any] = Future.unit
    def apply[T](call: => Future[T]): Future[T] = synchronized {
      val next = last.transformWith(_ => try call catch { case NonFatal(e) => Future.failed(e) })(MsgProvider.syncEc)
      last = next
      next
    }
  }

  /** Control of restarted source - delegates to control of current source */
  private final class RestartControl extends Control {
    private val current = new AtomicReference[Control]()
    private val completion = new AtomicReference[Future[Any]]()
    @volatile private var stopped = false
    def isStopped: Boolean = stopped
    /** Sets control of (re)started source, stops it if stop was requested */
    def set(c: Control): Control = {
      Option(completion.get).foreach(c.drainOn)
      current.set(c)
      if (stopped) c.stop()
      c
    }
    def drainOn(streamCompletion: Future[Any]): Unit = {
      completion.set(streamCompletion)
      Option(current.get).foreach(_.drainOn(streamCompletion))
    }
    def stop(): Future[Done] = {
      stopped = true
      Option(current.get).map(_.stop()).getOrElse(Future.successful(Done))
    }
  }

  private final class Termination[T](
    within: Option[FiniteDuration],
    idle: Option[FiniteDuration],
    drain: Boolean,
    onStop: () => Unit,
  ) extends GraphStage[FlowShape[(T, Boolean), T]] {
    private val in  = Inlet[(T, Boolean)]("MsgStreams.termination.in")
    private val out = Outlet[T]("MsgStreams.termination.out")
    override val shape: FlowShape[(T, Boolean), T] = FlowShape(in, out)
    override def initialAttributes: Attributes = Attributes.name("msgReadTermination")

    override def createLogic(attrs: Attributes): GraphStageLogic =
      new TimerGraphStageLogic(shape) with InHandler with OutHandler {
        private val WithinTimer = "within"
        private val IdleTimer   = "idle"
        private var stopped     = false // only in drain mode, elements are dropped

        override def preStart(): Unit =
          within.foreach(scheduleOnce(WithinTimer, _))

        private def stop(): Unit = {
          onStop()
          if (drain) {
            stopped = true
            cancelTimer(WithinTimer)
            cancelTimer(IdleTimer)
            if (!isClosed(out)) complete(out)
            pullIfNeeded()
          } else completeStage()
        }

        /** Pulls upstream, idle timer runs while waiting for upstream message (not after termination) */
        private def pullIfNeeded(): Unit =
          if (!isClosed(in) && !hasBeenPulled(in)) {
            pull(in)
            if (!stopped) idle.foreach(scheduleOnce(IdleTimer, _))
          }

        override def onPush(): Unit = {
          val (el, isLast) = grab(in)
          if (stopped) pull(in) // drop
          else {
            cancelTimer(IdleTimer) // message arrived, downstream demand is not idle time
            push(out, el)
            if (isLast) stop()
          }
        }

        override def onPull(): Unit = if (!stopped) pullIfNeeded()

        override def onDownstreamFinish(cause: Throwable): Unit =
          if (drain) { if (!stopped) stop() }
          else super.onDownstreamFinish(cause)

        override protected def onTimer(timerKey: Any): Unit = if (!stopped) stop()

        setHandlers(in, out, this)
      }
  }
}
