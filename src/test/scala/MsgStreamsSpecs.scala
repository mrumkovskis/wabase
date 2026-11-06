package org.wabase

import org.apache.pekko.Done
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.scalatest.matchers.should.Matchers
import org.wabase.msg._

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class MsgStreamsSpecs extends FlatSpec with Matchers with BeforeAndAfterAll {
  implicit val system: ActorSystem = ActorSystem("msg-streams-specs")
  override def afterAll(): Unit = Await.result(system.terminate(), 10.seconds)

  private def await[T](f: Future[T]): T = Await.result(f, 10.seconds)

  /** infinite source of (n, n == last), counts pulled elements, completion of upstream is observed */
  private class Upstream(last: Int = -1) {
    val pulled = new AtomicInteger
    val finished = new AtomicBoolean
    val source: Source[(Int, Boolean), Future[Done]] =
      Source.fromIterator(() => Iterator.from(1)).map { n =>
        pulled.incrementAndGet()
        Thread.sleep(1)
        (n, n == last)
      }.watchTermination()(Keep.right)
  }

  behavior of "MsgStreams.termination"

  it should "emit messages until last one and cancel upstream" in {
    val up = new Upstream(last = 3)
    val (upDone, res) = up.source
      .via(MsgStreams.termination[Int](None, None, drain = false))
      .toMat(Sink.seq)(Keep.both).run()
    await(res) shouldBe List(1, 2, 3)
    await(upDone) shouldBe Done
  }

  it should "in drain mode complete downstream after last one, drop further elements until upstream completes" in {
    val up = new Upstream(last = 3)
    val (upDone, res) = up.source
      .take(1000) // upstream completion, like consumer stop
      .via(MsgStreams.termination[Int](None, None, drain = true))
      .toMat(Sink.seq)(Keep.both).run()
    await(res) shouldBe List(1, 2, 3)
    await(upDone) shouldBe Done
    up.pulled.get shouldBe 1000 // upstream was not cancelled - elements were pulled and dropped
  }

  it should "in drain mode keep upstream running after downstream cancellation" in {
    val up = new Upstream()
    val (upDone, res) = up.source
      .take(100)
      .via(MsgStreams.termination[Int](None, None, drain = true))
      .take(2)
      .toMat(Sink.seq)(Keep.both).run()
    await(res) shouldBe List(1, 2)
    await(upDone) shouldBe Done
    up.pulled.get shouldBe 100
  }

  it should "stop after within duration" in {
    val res = Source.repeat((1, false)).throttle(1, 100.millis)
      .via(MsgStreams.termination[Int](Some(550.millis), None, drain = false))
      .runWith(Sink.seq)
    await(res).size should (be >= 3 and be <= 7)
  }

  it should "stop after idle timeout, also if no message arrives" in {
    await(Source.never[(Int, Boolean)]
      .via(MsgStreams.termination[Int](None, Some(200.millis), drain = false))
      .runWith(Sink.seq)) shouldBe Nil
    // idle timer is restarted on each message
    val res = (Source(List((1, false), (2, false), (3, false))).throttle(1, 100.millis) ++ Source.never)
      .via(MsgStreams.termination[Int](None, Some(300.millis), drain = false))
      .runWith(Sink.seq)
    await(res) shouldBe List(1, 2, 3)
  }

  it should "not count slow downstream processing as idle time" in {
    // messages are available, downstream takes longer than idle timeout for each
    val res = (Source(List((1, false), (2, false), (3, false))) ++ Source.never)
      .via(MsgStreams.termination[Int](None, Some(200.millis), drain = false))
      .mapAsync(1)(n => org.apache.pekko.pattern.after(400.millis)(Future.successful(n)))
      .runWith(Sink.seq)
    await(res) shouldBe List(1, 2, 3) // idle timeout after the last message
  }

  it should "call onStop once on termination" in {
    Seq(false, true).foreach { drain =>
      val stops = new AtomicInteger
      val up = new Upstream(last = 2)
      await(up.source.take(50)
        .via(MsgStreams.termination[Int](None, None, drain, () => stops.incrementAndGet()))
        .runWith(Sink.seq)) shouldBe List(1, 2)
      stops.get shouldBe 1
    }
  }

  behavior of "MsgStreams.run"

  private class TestControl(stopResult: Future[Done] = Future.successful(Done)) extends Control {
    @volatile var stopped = false
    def drainOn(streamCompletion: Future[Any]): Unit = ()
    def stop(): Future[Done] = { stopped = true; stopResult }
  }

  it should "stop consumer after stream completes" in {
    val control = new TestControl
    val res = MsgStreams.run(Source(List(1, 2, 3)).mapMaterializedValue(_ => control: Control), Sink.seq[Int])
    await(res) shouldBe List(1, 2, 3)
    control.stopped shouldBe true
  }

  it should "stop consumer after stream failure and report stop failure" in {
    val control = new TestControl
    val failed = MsgStreams.run(
      Source.failed[Int](new RuntimeException("stream failed")).mapMaterializedValue(_ => control: Control),
      Sink.seq[Int])
    intercept[RuntimeException](await(failed)).getMessage shouldBe "stream failed"
    control.stopped shouldBe true
    val stopFailure = new TestControl(Future.failed(new RuntimeException("stop failed")))
    intercept[RuntimeException](await(MsgStreams.run(
      Source(List(1)).mapMaterializedValue(_ => stopFailure: Control), Sink.seq[Int]))).getMessage shouldBe "stop failed"
  }

  behavior of "MsgStreams.restartOnFailure"

  it should "restart failed source, delegate stop to current source control" in {
    import org.apache.pekko.stream.RestartSettings
    val attempts = new AtomicInteger
    val controls = new java.util.concurrent.ConcurrentLinkedQueue[TestControl]
    val source = MsgStreams.restartOnFailure(RestartSettings(10.millis, 20.millis, 0)) { () =>
      val control = new TestControl
      controls.add(control)
      // failure may overtake elements emitted before it, so the failing source emits nothing
      (if (attempts.incrementAndGet() == 1) Source.failed[Int](new RuntimeException("failed"))
       else Source(List(2, 3)) ++ Source.never).mapMaterializedValue(_ => control: Control)
    }
    val (control, res) = source.take(2).toMat(Sink.seq)(Keep.both).run()
    await(res) shouldBe List(2, 3)
    attempts.get shouldBe 2
    await(control.stop())
    import scala.jdk.CollectionConverters._
    controls.asScala.toList.map(_.stopped) shouldBe List(false, true) // current (second) source is stopped
  }

  it should "stop restarting after max restarts" in {
    import org.apache.pekko.stream.RestartSettings
    val attempts = new AtomicInteger
    val res = MsgStreams.restartOnFailure(RestartSettings(10.millis, 20.millis, 0).withMaxRestarts(2, 10.millis)) { () =>
      attempts.incrementAndGet()
      Source.failed[Int](new RuntimeException("failed")).mapMaterializedValue(_ => new TestControl: Control)
    }.runWith(Sink.seq)
    intercept[RuntimeException](await(res)).getMessage shouldBe "failed"
    attempts.get shouldBe 3 // initial + 2 restarts
  }
}
