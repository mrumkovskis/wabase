package org.wabase

import org.mojoz.metadata.in.YamlMd
import org.mojoz.metadata.out.DdlGenerator
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.scalatest.matchers.should.Matchers
import org.tresql.{Query, Resources, convLong}
import org.wabase.ds.PoolName

class JobStatusControllerSpecs extends FlatSpec with Matchers with BeforeAndAfterEach {
  behavior of "DefaultWabaseJobStatusController"

  import JobStatusControllerSpecsHelper._

  override protected def beforeEach(): Unit = clearDb()

  it should "insert a row and take the running lock" in {
    controller.acquireIsRunningLock("job_a") shouldBe true
    val row = jobRow("job_a")
    row("status") shouldBe "running"
    longVal(row("start_count")) shouldBe 1L
    longVal(row("success_count")) shouldBe 0L
    longVal(row("error_count")) shouldBe 0L
    longVal(row("collision_count")) shouldBe 0L
    row("last_start_time") != null shouldBe true
  }

  it should "reject a second lock and count the collision" in {
    controller.acquireIsRunningLock("job_a") shouldBe true
    controller.acquireIsRunningLock("job_a") shouldBe false
    val row = jobRow("job_a")
    row("status") shouldBe "running"
    longVal(row("start_count")) shouldBe 1L
    longVal(row("collision_count")) shouldBe 1L
  }

  it should "store success, not the legacy SUCC code, and keep details" in {
    controller.acquireIsRunningLock("job_a") shouldBe true
    controller.updateCronJobStatus("job_a", "SUCC", "all good")
    val row = jobRow("job_a")
    row("status") shouldBe "success"
    row("last_run_status") shouldBe "success"
    longVal(row("success_count")) shouldBe 1L
    row("last_success_details") shouldBe "all good"
    row("last_success_time") != null shouldBe true
  }

  it should "store error, not the legacy ERR code, and keep details" in {
    controller.acquireIsRunningLock("job_a") shouldBe true
    controller.updateCronJobStatus("job_a", "ERR", "boom")
    val row = jobRow("job_a")
    row("status") shouldBe "error"
    row("last_run_status") shouldBe "error"
    longVal(row("error_count")) shouldBe 1L
    row("last_error_details") shouldBe "boom"
    row("last_error_time") != null shouldBe true
  }

  it should "accept new status codes success and error" in {
    controller.acquireIsRunningLock("job_a") shouldBe true
    controller.updateCronJobStatus("job_a", "success", "ok")
    jobRow("job_a")("status") shouldBe "success"
    controller.acquireIsRunningLock("job_a") shouldBe true
    controller.updateCronJobStatus("job_a", "error", "fail")
    val row = jobRow("job_a")
    row("status") shouldBe "error"
    row("last_error_details") shouldBe "fail"
    longVal(row("success_count")) shouldBe 1L
    longVal(row("error_count")) shouldBe 1L
  }

  it should "map legacy RUN to running and not save RUN" in {
    controller.acquireIsRunningLock("job_a") shouldBe true
    controller.updateCronJobStatus("job_a", "success", null)
    controller.updateCronJobStatus("job_a", "RUN", null)
    jobRow("job_a")("status") shouldBe "running"
  }

  it should "allow a new run after success and increment start_count" in {
    controller.acquireIsRunningLock("job_a") shouldBe true
    controller.updateCronJobStatus("job_a", "success", null)
    controller.acquireIsRunningLock("job_a") shouldBe true
    val row = jobRow("job_a")
    row("status") shouldBe "running"
    longVal(row("start_count")) shouldBe 2L
    longVal(row("success_count")) shouldBe 1L
  }

  it should "delete finished rows on init and keep a running job" in {
    controller.acquireIsRunningLock("done_job") shouldBe true
    controller.updateCronJobStatus("done_job", "success", null)
    controller.acquireIsRunningLock("running_job") shouldBe true
    controller.init()
    jobExists("done_job") shouldBe false
    jobExists("running_job") shouldBe true
    jobRow("running_job")("status") shouldBe "running"
  }

  it should "reject an unsupported status" in {
    controller.acquireIsRunningLock("job_a") shouldBe true
    intercept[IllegalArgumentException] {
      controller.updateCronJobStatus("job_a", "NOPE", null)
    }.getMessage should include("NOPE")
  }

  it should "trim details to 2000 characters" in {
    val longDetails = "x" * 2500
    controller.acquireIsRunningLock("job_a") shouldBe true
    controller.updateCronJobStatus("job_a", "success", longDetails)
    jobRow("job_a")("last_success_details").asInstanceOf[String] shouldBe longDetails.take(2000)
    controller.acquireIsRunningLock("job_a") shouldBe true
    controller.updateCronJobStatus("job_a", "error", longDetails)
    jobRow("job_a")("last_error_details").asInstanceOf[String] shouldBe longDetails.take(2000)
  }

  it should "not write cron_job_history" in {
    controller.acquireIsRunningLock("job_a") shouldBe true
    controller.updateCronJobStatus("job_a", "success", "ok")
    historyRows("job_a") shouldBe empty
  }

  behavior of "WabaseJobStatusHistoryLogger"

  it should "insert and finish cron_job_history by uuid" in {
    val uuid1 = "11111111-1111-1111-1111-111111111111"
    val uuid2 = "22222222-2222-2222-2222-222222222222"
    jobStatusLogger.jobStarted(uuid1, "job_a")
    val running = historyRows("job_a")
    running.size shouldBe 1
    running.head("uuid") shouldBe uuid1
    running.head("status") shouldBe "running"
    running.head("end_time") == null shouldBe true
    jobStatusLogger.jobFinished(uuid1, "job_a", "SUCC", "done")
    val done = historyRows("job_a")
    done.size shouldBe 1
    done.head("status") shouldBe "success"
    done.head("details") shouldBe "done"
    done.head("end_time") != null shouldBe true
    jobStatusLogger.jobStarted(uuid2, "job_a")
    jobStatusLogger.jobFinished(uuid2, "job_a", "ERR", "boom")
    val all = historyRows("job_a")
    all.size shouldBe 2
    all.map(_("status")) shouldBe List("error", "success")
    all.head("uuid") shouldBe uuid2
    all.head("details") shouldBe "boom"
  }

  it should "trim history details to 2000 characters" in {
    val uuid = "11111111-1111-1111-1111-111111111111"
    val longDetails = "x" * 2500
    jobStatusLogger.jobStarted(uuid, "job_a")
    jobStatusLogger.jobFinished(uuid, "job_a", "success", longDetails)
    historyRows("job_a").head("details").asInstanceOf[String] shouldBe longDetails.take(2000)
  }
}

object JobStatusControllerSpecsHelper {
  DbDrivers.loadDrivers

  object JobStatusSpecsQuerease extends AppQuerease {
    override lazy val yamlMetadata = YamlMd.fromResource("/job-status-specs-metadata.yaml")
  }

  class JobStatusDbAccess extends DbAccess with QuereaseProvider with Loggable {
    override protected def tresqlMetadata = JobStatusSpecsQuerease.tresqlMetadata
    override protected def initQuereaseIo: AppQuereaseIo[Dto] = new AppQuereaseIo[Dto](JobStatusSpecsQuerease)
  }

  val db = new JobStatusDbAccess
  val jobStatusCp = PoolName("job-status-test")
  val controller = new DefaultWabaseJobStatusController(db) {
    override val jobStatusCp: PoolName = PoolName("job-status-test")
  }
  val jobStatusLogger = new WabaseJobStatusHistoryLogger(db) {
    override val jobStatusCp: PoolName = PoolName("job-status-test")
  }

  val schemaSql: String = DdlGenerator.hsqldb().schema(JobStatusSpecsQuerease.tableMetadata.tableDefs)
  executeStatements(schemaSql.split(";\\s+").filter(_.trim.nonEmpty).map(_.trim.stripSuffix(";") + ";").toIndexedSeq: _*)

  def newTransaction[A](f: Resources => A): A = db.newTransaction(jobStatusCp)(f)

  def executeStatements(statements: String*): Unit = newTransaction { res =>
    val statement = res.conn.createStatement
    try statements foreach { statement.execute } finally statement.close()
  }

  def clearDb(): Unit = newTransaction { implicit res =>
    Query("cron_job_status - []")
    Query("cron_job_history - []")
  }

  def jobRow(name: String): Map[String, Any] = newTransaction { implicit res =>
    Query(
      """cron_job_status[job_name = ?]{
        |  job_name, status, start_count, success_count, error_count, collision_count,
        |  last_run_status, last_start_time, last_success_time, last_success_details,
        |  last_error_time, last_error_details
        |}""".stripMargin, name
    ).toListOfMaps.head
  }

  def historyRows(name: String): List[Map[String, Any]] = newTransaction { implicit res =>
    Query(
      "cron_job_history[job_name = ?]{uuid, job_name, start_time, end_time, status, details}",
      name
    ).toListOfMaps.sortBy(r => String.valueOf(r("start_time"))).reverse
  }

  def longVal(v: Any): Long = v.asInstanceOf[Number].longValue

  @annotation.nowarn("msg=Manifest")
  def jobExists(name: String): Boolean = newTransaction { implicit res =>
    Query("cron_job_status[job_name = ?]{count(1)}", name).unique[Long] == 1
  }
}
