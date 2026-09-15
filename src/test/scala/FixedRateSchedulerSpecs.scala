package org.wabase

import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.testkit.{TestKit, TestProbe}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.{AnyFlatSpecLike => FlatSpecLike}
import org.scalatest.matchers.should.Matchers
import org.wabase.WabaseScheduler.Tick
import org.wabase.scheduler.FixedRateScheduler

import scala.concurrent.duration._

class FixedRateSchedulerSpecs
  extends TestKit(ActorSystem("fixed-rate-scheduler-specs"))
    with FlatSpecLike
    with Matchers
    with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  private val app = new TestApp {
    override protected def initQuerease = new TestQuerease("/querease-action-specs-metadata.yaml")
  }

  behavior of "FixedRateScheduler"

  it should "schedule enabled jobs at a fixed rate" in {
    val probe = TestProbe()
    val cfg = ConfigFactory.parseString(
      """
        |app.job.schedules {
        |  test_job1 {
        |    enabled = true
        |    interval = 10m
        |    initial-delay = 0s
        |    params { name = "fixed-rate" }
        |  }
        |  missing_job {
        |    interval = 1s
        |    initial-delay = 0s
        |  }
        |  disabled_job {
        |    enabled = false
        |    interval = 1s
        |    initial-delay = 0s
        |  }
        |}
        |""".stripMargin)
    FixedRateScheduler.scheduleJobs(cfg, app, system, probe.ref)
    probe.expectMsg(2.seconds, Tick("test_job1", Map("name" -> "fixed-rate")))
    probe.expectNoMessage(200.millis)
  }

  it should "skip jobs without a positive interval" in {
    val probe = TestProbe()
    val cfg = ConfigFactory.parseString(
      """
        |app.job.schedules {
        |  test_job1 {
        |    interval = 0s
        |    initial-delay = 0s
        |  }
        |  no_interval_job {
        |  }
        |}
        |""".stripMargin)
    FixedRateScheduler.scheduleJobs(cfg, app, system, probe.ref)
    probe.expectNoMessage(200.millis)
  }

  it should "not schedule when app.job.schedules is missing" in {
    val probe = TestProbe()
    FixedRateScheduler.scheduleJobs(ConfigFactory.empty, app, system, probe.ref)
    probe.expectNoMessage(200.millis)
  }
}
