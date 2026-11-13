package org.wabase

import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.testkit.TestKit
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.{AnyFlatSpecLike => FlatSpecLike}
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}

class OnStartJobSpecs
  extends TestKit(ActorSystem("on-start-job-specs"))
    with FlatSpecLike
    with Matchers
    with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  private def names(value: String) =
    WabaseScheduler.onStartJobNames(ConfigFactory.parseString(s"app.job.on-start-job = $value"))

  /** Records started jobs, each job finishes when the test completes its promise */
  private class RecordingScheduler(jobNames: Seq[String]) extends WabaseScheduler(null, system) {
    val runs = jobNames.map(_ -> Promise[Any]()).toMap
    @volatile var started = List.empty[String]
    override protected def onStartJobs: Seq[String] = jobNames
    override def doJob(jobName: String, params: Map[String, Any]): Future[Any] = synchronized {
      started = started :+ jobName
      runs(jobName).future
    }
    val failures = new java.util.concurrent.LinkedBlockingQueue[(String, String)]
    override protected def onStartJobFailed(jobName: String, error: Throwable): Unit = {
      super.onStartJobFailed(jobName, error)
      failures.put(jobName -> error.getMessage)
    }
    def nextFailure(): (String, String) = failures.poll(3, SECONDS)
    def run(): Future[Any] = runOnStartJobs()
  }

  behavior of "WabaseScheduler.onStartJobNames"

  it should "return no jobs for null" in {
    names("null") shouldBe Nil
  }

  it should "return one job for a single name" in {
    names("job1") shouldBe List("job1")
  }

  it should "split a string by whitespace and comma" in {
    names("\"job1, job2 job3\"") shouldBe List("job1", "job2", "job3")
    names("\"\"") shouldBe Nil
  }

  it should "return jobs from a list in order" in {
    names("[job1, job2]") shouldBe List("job1", "job2")
    names("[]") shouldBe Nil
  }

  behavior of "WabaseScheduler on-start jobs"

  it should "succeed without jobs" in {
    Await.result(new RecordingScheduler(Nil).run(), 3.seconds) shouldBe NoResult
  }

  it should "start all jobs in parallel" in {
    val scheduler = new RecordingScheduler(List("job1", "job2", "job3"))
    val result = scheduler.run()
    scheduler.started shouldBe List("job1", "job2", "job3")
    result.isCompleted shouldBe false
    scheduler.runs("job2").success("ok2")
    scheduler.runs("job1").success("ok1")
    scheduler.runs("job3").success("ok3")
    Await.result(result, 3.seconds) shouldBe List("ok1", "ok2", "ok3")
  }

  it should "fail when a job fails without affecting other jobs" in {
    val scheduler = new RecordingScheduler(List("job1", "job2"))
    val result = scheduler.run()
    scheduler.started shouldBe List("job1", "job2")
    scheduler.runs("job1").failure(new RuntimeException("boom"))
    the[RuntimeException] thrownBy Await.result(result, 3.seconds) should have message "boom"
    scheduler.runs("job2").success("ok2")
    scheduler.runs("job2").future.value.get.get shouldBe "ok2"
    scheduler.nextFailure() shouldBe ("job1" -> "boom")
    scheduler.failures.isEmpty shouldBe true
  }

  it should "report job failure while jobs before it are still running" in {
    val scheduler = new RecordingScheduler(List("job1", "job2", "job3"))
    val result = scheduler.run()
    scheduler.runs("job3").failure(new RuntimeException("boom3"))
    scheduler.nextFailure() shouldBe ("job3" -> "boom3")
    scheduler.runs("job2").failure(new RuntimeException("boom2"))
    scheduler.nextFailure() shouldBe ("job2" -> "boom2")
    scheduler.runs("job1").isCompleted shouldBe false
    scheduler.runs("job1").success("ok1")
    // which failure is reported by the result depends on scala version
    (the[RuntimeException] thrownBy Await.result(result, 3.seconds)).getMessage should (be ("boom2") or be ("boom3"))
    scheduler.failures.isEmpty shouldBe true
  }

  it should "report failure of a job which fails to start" in {
    val scheduler = new RecordingScheduler(List("job1")) {
      override def doJob(jobName: String, params: Map[String, Any]): Future[Any] =
        Future.failed(new RuntimeException("no start"))
    }
    the[RuntimeException] thrownBy Await.result(scheduler.run(), 3.seconds) should have message "no start"
    scheduler.nextFailure() shouldBe ("job1" -> "no start")
  }
}
