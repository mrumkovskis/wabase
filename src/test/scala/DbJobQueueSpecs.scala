package org.wabase

import org.apache.pekko.actor.{ActorRef, ActorSystem, Props}
import org.apache.pekko.testkit.{TestKit, TestProbe}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.{AnyFlatSpecLike => FlatSpecLike}
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.tresql.{Query, convInt}
import org.wabase.WabaseJobQueue.{Disabled, Duplicate, Full, Queued}
import org.wabase.WabaseScheduler.{JobNotFound, JobQueued, JobRunning, JobStarted, Tick}

import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import scala.concurrent.Promise
import scala.concurrent.duration._

class DbJobQueueSpecs
  extends TestKit(ActorSystem("db-job-queue-specs"))
    with FlatSpecLike
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with Eventually {

  import DbJobQueueSpecs._
  import JobStatusControllerSpecsHelper._

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(5, Seconds), interval = Span(15, Millis))

  override def beforeEach(): Unit = clearDb()

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  private val app = new TestApp {
    override protected def initQuerease = new TestQuerease("/querease-action-specs-metadata.yaml")
  }

  require(
    WabaseScheduler.isJobNameValid("test_job1", Map.empty[String, Any])(app),
    "test_job1 must be a defined job")

  private class RecordingScheduler(wabase: AppBase[_], actorSystem: ActorSystem)
    extends WabaseScheduler(wabase, actorSystem) {
    private val runs = new java.util.concurrent.CopyOnWriteArrayList[(String, Map[String, Any], Promise[Any])]()
    override def doJob(jobName: String, params: Map[String, Any]): scala.concurrent.Future[Any] = {
      val p = Promise[Any]()
      runs.add((jobName, params, p))
      p.future
    }
    def size: Int = runs.size
    def params(i: Int): Map[String, Any] = runs.get(i)._2
    def succeed(i: Int, value: Any = "ok"): Unit = runs.get(i)._3.success(value)
  }

  private class QuietJobStatusLogger extends NoOpWabaseJobStatusLogger {
    override def jobFinished(uuid: String, name: String, status: String, details: String, error: Throwable): Unit = ()
  }

  /** Asks a [[SharedQueueActor]] to start waiting runs. The actor reference is not the instance. */
  private case object Drain

  private class SharedQueueActor(
    wabase: AppBase[_],
    scheduler: WabaseScheduler,
    queueSize: Int,
  ) extends WabaseJobActor(wabase, scheduler, controller, new QuietJobStatusLogger) {
    override protected def jobQueueSize: Int = queueSize
    override protected def queuePollInterval: FiniteDuration = Duration.Zero
    override protected def jobQueueFactory: WabaseJobQueueFactory = QueueFactory
    override def receive: Receive = {
      case Drain => self ! DrainJobQueue
      case msg => super.receive(msg)
    }
  }

  private def queue(jobName: String, maxSize: Int): WabaseDbJobQueue =
    new WabaseDbJobQueue(db, jobStatusCp, jobName, maxSize)

  @annotation.nowarn("msg=Manifest")
  private def queueCount(jobName: String): Int = newTransaction { implicit res =>
    Query.unique[Int]("cron_job_queue[job_name = ?]{count(1)}", jobName)
  }

  private def withNodes[T](queueSize: Int)(
    body: (TestProbe, ActorRef, RecordingScheduler, TestProbe, ActorRef, RecordingScheduler) => T
  ): T = {
    val schedulerA = new RecordingScheduler(app, system)
    val schedulerB = new RecordingScheduler(app, system)
    val actorA = system.actorOf(Props(new SharedQueueActor(app, schedulerA, queueSize)))
    val actorB = system.actorOf(Props(new SharedQueueActor(app, schedulerB, queueSize)))
    val probeA = TestProbe()
    val probeB = TestProbe()
    try body(probeA, actorA, schedulerA, probeB, actorB, schedulerB)
    finally {
      Seq(actorA, actorB).foreach { actor =>
        watch(actor)
        system.stop(actor)
        expectTerminated(actor, 3.seconds)
      }
    }
  }

  private def tick(probe: TestProbe, actor: ActorRef, jobName: String, params: Map[String, Any], expected: Any): Unit = {
    probe.send(actor, Tick(jobName, params))
    probe.expectMsg(3.seconds, expected)
  }

  private def awaitRuns(scheduler: RecordingScheduler, n: Int): Unit =
    eventually { scheduler.size shouldBe n }

  private class OpenQueue extends WabaseDbJobQueue(db, jobStatusCp, "codec", 1) {
    def enc(params: Map[String, Any]): Array[Byte] = encodeParams(params)
    def dec(encoded: Array[Byte]): Map[String, Any] = decodeParams(encoded)
  }

  private def sameBytes(a: Array[Byte], b: Array[Byte]): Unit =
    java.util.Arrays.equals(a, b) shouldBe true

  behavior of "database job parameter serialization"

  it should "round-trip serializable values and ignore map key order" in {
    val codec = new OpenQueue
    val params: Map[String, Any] = Map(
      "s" -> "a",
      "b" -> false,
      "i" -> 1,
      "l" -> 2L,
      "d" -> 1.5d,
      "bd" -> BigDecimal("1.50"),
      "when" -> java.time.LocalDate.parse("2020-01-02"),
      "nil" -> null,
      "ids" -> Seq(1, 2),
      "bytes" -> Array[Byte](1, 2, 3),
      "child" -> Map("z" -> "q", "a" -> 3),
    )
    val encoded = codec.enc(params)
    sameBytes(codec.enc(params.toSeq.reverse.toMap), encoded)
    val decoded = codec.dec(encoded)
    java.util.Arrays.equals(
      decoded("bytes").asInstanceOf[Array[Byte]], Array[Byte](1, 2, 3)) shouldBe true
    (decoded - "bytes") shouldBe (params - "bytes")
    sameBytes(codec.enc(decoded), encoded)
    val boxed: Map[String, Any] = Map("n" -> Integer.valueOf(1), "ok" -> java.lang.Boolean.TRUE)
    codec.dec(codec.enc(boxed)) shouldBe Map("n" -> 1, "ok" -> true)
  }

  it should "reject a map key that is not a string" in {
    val codec = new OpenQueue
    intercept[IllegalArgumentException] {
      codec.enc(Map("x" -> Map(1 -> "a")))
    }.getMessage should include("must be a string")
  }

  it should "reject a value that is not serializable" in {
    val codec = new OpenQueue
    intercept[java.io.NotSerializableException] {
      codec.enc(Map("x" -> new Object))
    }.getMessage should include("java.lang.Object")
  }

  behavior of "WabaseDbJobQueue"

  it should "be the default queue" in {
    config.getString("app.job.queue") shouldBe "org.wabase.WabaseDbJobQueueFactory"
    new InMemoryWabaseJobQueueFactory().acceptsWhenLockHeldElsewhere shouldBe false
  }

  it should "keep distinct parameters in order, once, up to max size" in {
    val q = queue("job_a", 2)
    q.offer(Map("n" -> 1, "m" -> "a")) shouldBe Queued
    q.offer(Map("m" -> "a", "n" -> 1)) shouldBe Duplicate
    q.offer(Map("n" -> 2)) shouldBe Queued
    q.offer(Map("n" -> 3)) shouldBe Full
    q.size shouldBe 2
    q.poll() shouldBe Some(Map("n" -> 1, "m" -> "a"))
    q.poll() shouldBe Some(Map("n" -> 2))
    q.poll() shouldBe None
    queueCount("job_a") shouldBe 0
  }

  it should "put a run back ahead of the others and share that queue across instances" in {
    val first = queue("job_a", 2)
    val second = queue("job_a", 2)
    val other = queue("job_b", 2)
    first.offer(Map("n" -> 1)) shouldBe Queued
    other.offer(Map("n" -> 9)) shouldBe Queued
    first.offer(Map("n" -> 2)) shouldBe Queued
    new WabaseDbJobQueue(db, jobStatusCp, "", 1).jobNames.toSet shouldBe Set("job_a", "job_b")
    first.poll() shouldBe Some(Map("n" -> 1))
    second.offerFront(Map("n" -> 1))
    second.offerFront(Map("n" -> 1))
    second.size shouldBe 2
    second.poll() shouldBe Some(Map("n" -> 1))
    second.poll() shouldBe Some(Map("n" -> 2))
    other.poll() shouldBe Some(Map("n" -> 9))
    new WabaseDbJobQueue(db, jobStatusCp, "", 1).jobNames shouldBe Nil
  }

  it should "store nothing when max size is not positive" in {
    val q = queue("job_a", 0)
    q.offer(Map("n" -> 1)) shouldBe Disabled
    q.size shouldBe 0
    q.poll() shouldBe None
    q.offerFront(Map("n" -> 1))
    queueCount("job_a") shouldBe 0
  }

  it should "not grow past max size when several nodes enqueue at once" in {
    val q = queue("job_a", 3)
    val n = 8
    val start = new CountDownLatch(1)
    val done = new CountDownLatch(n)
    val errors = new ConcurrentLinkedQueue[Throwable]()
    (0 until n).foreach { i =>
      val thread = new Thread(() => {
        try {
          start.await(5, TimeUnit.SECONDS)
          q.offer(Map("n" -> i))
        } catch {
          case e: Throwable => errors.add(e)
        } finally done.countDown()
        ()
      })
      thread.start()
    }
    start.countDown()
    done.await(10, TimeUnit.SECONDS) shouldBe true
    errors shouldBe empty
    q.size shouldBe 3
  }

  it should "keep one copy when several nodes enqueue the same parameters" in {
    val q = queue("job_a", 5)
    val n = 6
    val start = new CountDownLatch(1)
    val done = new CountDownLatch(n)
    val results = new ConcurrentLinkedQueue[WabaseJobQueue.Result]()
    (0 until n).foreach { _ =>
      val thread = new Thread(() => {
        try {
          start.await(5, TimeUnit.SECONDS)
          results.add(q.offer(Map("n" -> 1)))
        } finally done.countDown()
        ()
      })
      thread.start()
    }
    start.countDown()
    done.await(10, TimeUnit.SECONDS) shouldBe true
    q.size shouldBe 1
    results.toArray.count(_ == Queued) shouldBe 1
    results.toArray.count(_ == Duplicate) shouldBe n - 1
  }

  behavior of "database job queue on two nodes"

  it should "start an idle run on this node and not leave it queued" in {
    withNodes(2) { (probeA, actorA, schedulerA, _, _, _) =>
      val params = Map("n" -> 1, "ids" -> Seq(1, 2), "child" -> Map("a" -> "x"))
      tick(probeA, actorA, "test_job1", params, JobStarted)
      schedulerA.size shouldBe 1
      schedulerA.params(0) shouldBe params
      queueCount("test_job1") shouldBe 0
    }
  }

  it should "queue a run on another node and start it here when this run finishes" in {
    withNodes(2) { (probeA, actorA, schedulerA, probeB, actorB, schedulerB) =>
      val job = "test_job1"
      tick(probeA, actorA, job, Map("n" -> 1), JobStarted)
      tick(probeB, actorB, job, Map("n" -> 2), JobQueued)
      tick(probeB, actorB, job, Map("n" -> 2), JobQueued)
      tick(probeB, actorB, job, Map("n" -> 3), JobQueued)
      tick(probeB, actorB, job, Map("n" -> 4), JobRunning)
      queueCount(job) shouldBe 2
      schedulerB.size shouldBe 0

      schedulerA.succeed(0)
      awaitRuns(schedulerA, 2)
      schedulerA.params(1) shouldBe Map("n" -> 2)
      schedulerB.size shouldBe 0

      schedulerA.succeed(1)
      awaitRuns(schedulerA, 3)
      schedulerA.params(2) shouldBe Map("n" -> 3)
      queueCount(job) shouldBe 0
    }
  }

  it should "let the other node start a queued run when this node no longer holds the lock" in {
    withNodes(1) { (probeA, actorA, schedulerA, probeB, actorB, schedulerB) =>
      val job = "test_job1"
      tick(probeA, actorA, job, Map("n" -> 1), JobStarted)
      tick(probeB, actorB, job, Map("n" -> 2), JobQueued)
      controller.updateCronJobStatus(job, "success", null)
      actorB ! Drain
      awaitRuns(schedulerB, 1)
      schedulerB.params(0) shouldBe Map("n" -> 2)
      schedulerA.size shouldBe 1
      queueCount(job) shouldBe 0
    }
  }

  it should "reject a second start when the database queue is disabled" in {
    withNodes(0) { (probeA, actorA, schedulerA, probeB, _, _) =>
      val job = "test_job1"
      tick(probeA, actorA, job, Map("n" -> 1), JobStarted)
      tick(probeB, actorA, job, Map("n" -> 2), JobRunning)
      schedulerA.size shouldBe 1
      queueCount(job) shouldBe 0
    }
  }

  it should "report an unknown job and not queue it" in {
    withNodes(1) { (probeA, actorA, schedulerA, _, _, _) =>
      tick(probeA, actorA, "missing_job", Map("n" -> 1), JobNotFound)
      schedulerA.size shouldBe 0
      queueCount("missing_job") shouldBe 0
    }
  }
}

object DbJobQueueSpecs {
  /** Shared by both test actors. Talks to the job-status-test pool. */
  object QueueFactory extends WabaseJobQueueFactory {
    import JobStatusControllerSpecsHelper._
    def create(jobName: String, maxSize: Int): WabaseJobQueue =
      new WabaseDbJobQueue(db, jobStatusCp, jobName, maxSize)
    override def acceptsWhenLockHeldElsewhere: Boolean = true
    override def pendingJobNames: Seq[String] =
      new WabaseDbJobQueue(db, jobStatusCp, "", 1).jobNames
  }
}
