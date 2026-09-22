package org.wabase

import org.apache.pekko.actor.{ActorRef, ActorSystem, Props}
import org.apache.pekko.testkit.{TestKit, TestProbe}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.{AnyFlatSpecLike => FlatSpecLike}
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.wabase.WabaseJobQueue.{Disabled, Duplicate, Full, Queued}
import org.wabase.WabaseScheduler.{JobNotFound, JobQueued, JobRunning, JobStarted, Tick}

import java.util.concurrent.CopyOnWriteArrayList
import scala.concurrent.Promise
import scala.concurrent.duration._

class JobQueueSpecs
  extends TestKit(ActorSystem("job-queue-specs"))
    with FlatSpecLike
    with Matchers
    with BeforeAndAfterAll
    with Eventually {

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(5, Seconds), interval = Span(15, Millis))

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  private val app = new TestApp {
    override protected def initQuerease = new TestQuerease("/querease-action-specs-metadata.yaml")
  }

  // The actor calls isJobNameValid on its thread before it replies. The first call compiles
  // view metadata, which on a cold Scala 3 run takes longer than expectMsg's 3 seconds.
  require(
    WabaseScheduler.isJobNameValid("test_job1", Map.empty[String, Any])(app),
    "test_job1 must be a defined job")

  private class RecordingScheduler(wabase: AppBase[_], actorSystem: ActorSystem)
    extends WabaseScheduler(wabase, actorSystem) {
    private val runs = new CopyOnWriteArrayList[(String, Map[String, Any], Promise[Any])]()
    override def doJob(jobName: String, params: Map[String, Any]): scala.concurrent.Future[Any] = {
      val p = Promise[Any]()
      runs.add((jobName, params, p))
      p.future
    }
    def size: Int = runs.size
    def jobName(i: Int): String = runs.get(i)._1
    def params(i: Int): Map[String, Any] = runs.get(i)._2
    def succeed(i: Int, value: Any = "ok"): Unit = runs.get(i)._3.success(value)
    def failRun(i: Int, error: Throwable = new RuntimeException("boom")): Unit =
      runs.get(i)._3.failure(error)
  }

  /** The status logger logs the failure throwable. Tests cover queueing, not that log line. */
  private class QuietJobStatusLogger extends NoOpWabaseJobStatusLogger {
    override def jobFinished(uuid: String, name: String, status: String, details: String, error: Throwable): Unit = ()
  }

  private class SizedJobActor(
    wabase: AppBase[_],
    scheduler: WabaseScheduler,
    statusController: WabaseJobStatusController,
    queueSize: Int,
  ) extends WabaseJobActor(wabase, scheduler, statusController, new QuietJobStatusLogger) {
    override protected def jobQueueSize: Int = queueSize
  }

  private class SwitchableJobStatusController extends NoOpWabaseJobStatusController {
    @volatile var allow: Boolean = true
    override def acquireIsRunningLock(name: String): Boolean = allow
  }

  private def withActor[T](
    queueSize: Int,
    controller: WabaseJobStatusController = new NoOpWabaseJobStatusController,
  )(body: (TestProbe, ActorRef, RecordingScheduler) => T): T = {
    val scheduler = new RecordingScheduler(app, system)
    val actor = system.actorOf(Props(new SizedJobActor(app, scheduler, controller, queueSize)))
    val probe = TestProbe()
    try body(probe, actor, scheduler)
    finally {
      watch(actor)
      system.stop(actor)
      expectTerminated(actor, 3.seconds)
    }
  }

  private def tick(probe: TestProbe, actor: ActorRef, jobName: String, params: Map[String, Any], expected: Any): Unit = {
    probe.send(actor, Tick(jobName, params))
    probe.expectMsg(3.seconds, expected)
  }

  private def awaitRuns(scheduler: RecordingScheduler, n: Int): Unit =
    eventually { scheduler.size shouldBe n }

  /** Send the same tick until the actor starts it. Rejected ticks are not queued when the queue is disabled. */
  private def untilStarted(probe: TestProbe, actor: ActorRef, jobName: String, params: Map[String, Any]): Unit = {
    val deadline = 3.seconds.fromNow
    var started = false
    while (!started && deadline.hasTimeLeft()) {
      probe.send(actor, Tick(jobName, params))
      probe.expectMsgPF(1.second) {
        case JobStarted => started = true
        case JobRunning =>
        case JobQueued =>
      }
    }
    started shouldBe true
  }

  behavior of "WabaseJobQueue"

  it should "keep distinct parameters in order up to max size and ignore duplicates" in {
    val q = new WabaseJobQueue(2)
    q.offer(Map("n" -> 1)) shouldBe Queued
    q.offer(Map("n" -> 1)) shouldBe Duplicate
    q.offer(Map("n" -> 2, "m" -> "a")) shouldBe Queued
    q.offer(Map("m" -> "a", "n" -> 2)) shouldBe Duplicate
    q.offer(Map("n" -> 3)) shouldBe Full
    q.offer(Map("n" -> 1)) shouldBe Duplicate
    q.size shouldBe 2
    q.poll() shouldBe Some(Map("n" -> 1))
    q.poll() shouldBe Some(Map("n" -> 2, "m" -> "a"))
    q.poll() shouldBe None
  }

  it should "compare nested parameter values" in {
    val q = new WabaseJobQueue(2)
    q.offer(Map("ids" -> Seq(1, 2), "child" -> Map("a" -> 1))) shouldBe Queued
    q.offer(Map("child" -> Map("a" -> 1), "ids" -> Seq(1, 2))) shouldBe Duplicate
    q.offer(Map("ids" -> Seq(1, 3), "child" -> Map("a" -> 1))) shouldBe Queued
    q.size shouldBe 2
  }

  it should "store nothing when max size is not positive" in {
    val q = new WabaseJobQueue(0)
    q.offer(Map("n" -> 1)) shouldBe Disabled
    q.size shouldBe 0
    q.poll() shouldBe None
    new WabaseJobQueue(-1).offer(Map.empty) shouldBe Disabled
  }

  it should "put a run back at the front without duplicating it" in {
    val q = new WabaseJobQueue(2)
    q.offer(Map("n" -> 1)) shouldBe Queued
    q.offer(Map("n" -> 2)) shouldBe Queued
    q.poll() shouldBe Some(Map("n" -> 1))
    q.offerFront(Map("n" -> 1))
    q.offerFront(Map("n" -> 1))
    q.size shouldBe 2
    q.poll() shouldBe Some(Map("n" -> 1))
    q.poll() shouldBe Some(Map("n" -> 2))
  }

  behavior of "WabaseJobActor job queue"

  it should "queue one extra run per distinct parameters and drop the rest" in {
    withActor(2) { (probe, actor, scheduler) =>
      val job = "test_job1"
      tick(probe, actor, job, Map("n" -> 1), JobStarted)
      scheduler.size shouldBe 1
      tick(probe, actor, job, Map("n" -> 1), JobQueued)
      tick(probe, actor, job, Map("n" -> 1), JobQueued)
      tick(probe, actor, job, Map("n" -> 2), JobQueued)
      tick(probe, actor, job, Map("n" -> 3), JobRunning)
      scheduler.size shouldBe 1

      scheduler.succeed(0)
      awaitRuns(scheduler, 2)
      scheduler.params(1) shouldBe Map("n" -> 1)

      scheduler.succeed(1)
      awaitRuns(scheduler, 3)
      scheduler.params(2) shouldBe Map("n" -> 2)

      scheduler.succeed(2)
      probe.send(actor, Tick(job, Map("n" -> 9)))
      probe.expectMsgPF(3.seconds) {
        case JobStarted =>
        case JobQueued =>
      }
      awaitRuns(scheduler, 4)
      (0 until 4).map(scheduler.params) shouldBe Seq(
        Map("n" -> 1), Map("n" -> 1), Map("n" -> 2), Map("n" -> 9))
    }
  }

  it should "reject a tick while the job is running when the queue is disabled" in {
    withActor(0) { (probe, actor, scheduler) =>
      val job = "test_job1"
      tick(probe, actor, job, Map("n" -> 1), JobStarted)
      tick(probe, actor, job, Map("n" -> 1), JobRunning)
      tick(probe, actor, job, Map("n" -> 2), JobRunning)
      scheduler.size shouldBe 1
      scheduler.succeed(0)
      untilStarted(probe, actor, job, Map("n" -> 2))
      awaitRuns(scheduler, 2)
      scheduler.params(0) shouldBe Map("n" -> 1)
      scheduler.params(1) shouldBe Map("n" -> 2)
    }
  }

  it should "start the queued run after the running job fails" in {
    withActor(1) { (probe, actor, scheduler) =>
      val job = "test_job1"
      tick(probe, actor, job, Map("n" -> 1), JobStarted)
      tick(probe, actor, job, Map("n" -> 2), JobQueued)
      scheduler.failRun(0)
      awaitRuns(scheduler, 2)
      scheduler.jobName(1) shouldBe job
      scheduler.params(1) shouldBe Map("n" -> 2)
    }
  }

  it should "queue each job name on its own" in {
    withActor(1) { (probe, actor, scheduler) =>
      tick(probe, actor, "test_job1", Map("n" -> 1), JobStarted)
      tick(probe, actor, "test_job2", Map("n" -> 1), JobStarted)
      awaitRuns(scheduler, 2)
      tick(probe, actor, "test_job1", Map("n" -> 1), JobQueued)
      tick(probe, actor, "test_job1", Map("n" -> 1), JobQueued)
      tick(probe, actor, "test_job2", Map("n" -> 1), JobQueued)
      tick(probe, actor, "test_job1", Map("n" -> 2), JobRunning)
      tick(probe, actor, "test_job2", Map("n" -> 2), JobRunning)

      scheduler.succeed(0)
      awaitRuns(scheduler, 3)
      scheduler.jobName(2) shouldBe "test_job1"
      scheduler.params(2) shouldBe Map("n" -> 1)

      scheduler.succeed(1)
      awaitRuns(scheduler, 4)
      scheduler.jobName(3) shouldBe "test_job2"
      scheduler.params(3) shouldBe Map("n" -> 1)
    }
  }

  it should "report an unknown job and not run it" in {
    withActor(1) { (probe, actor, scheduler) =>
      tick(probe, actor, "missing_job", Map.empty, JobNotFound)
      scheduler.size shouldBe 0
    }
  }

  it should "keep a queued run when the lock cannot be taken and start it ahead of a new request" in {
    val controller = new SwitchableJobStatusController
    withActor(1, controller) { (probe, actor, scheduler) =>
      val job = "test_job1"
      tick(probe, actor, job, Map("n" -> 1), JobStarted)
      tick(probe, actor, job, Map("n" -> 2), JobQueued)
      controller.allow = false
      scheduler.succeed(0)
      tick(probe, actor, job, Map("n" -> 3), JobRunning)
      scheduler.size shouldBe 1

      controller.allow = true
      probe.send(actor, Tick(job, Map("n" -> 4)))
      probe.expectMsgPF(3.seconds) {
        case JobRunning =>
        case JobQueued =>
      }
      awaitRuns(scheduler, 2)
      (0 until scheduler.size).map(scheduler.params) shouldBe Seq(Map("n" -> 1), Map("n" -> 2))
    }
  }
}
