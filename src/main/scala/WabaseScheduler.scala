package org.wabase

import com.typesafe.scalalogging.Logger
import org.apache.pekko.actor.{Actor, ActorRef, ActorSystem, Props}
import org.slf4j.LoggerFactory
import org.wabase.WabaseScheduler.{JobRunning, JobStarted, JobNotFound, Tick}
import org.tresql._
import org.wabase.AppMetadata.Action
import org.wabase.ds.PoolName

import scala.concurrent.{ExecutionContext, Future}
import scala.language.existentials
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

class WabaseScheduler(wabase: AppBase[_], system: ActorSystem) extends Loggable {
  def init(): Future[Any] = {
    val jobStatusController = createJobStatusController
    val jobStatusLogger = createJobStatusLogger
    if (config.getBoolean("app.job.clean-jobs-on-start"))
      jobStatusController.init()
    val wabaseJobActor = if (config.getIsNull("app.job.actor")) null else try {
      val jobActorClass = Class.forName(config.getString("app.job.actor"))
      system.actorOf(Props(jobActorClass, wabase, this, jobStatusController, jobStatusLogger), config.getString("app.job.actor-name"))
    } catch {
      case NonFatal(ex) => throw new RuntimeException(s"Failed to start job actor", ex)
    }
    if (!config.getIsNull("app.job.scheduler-initializer")) {
      if (wabaseJobActor != null) {
        invokeFunction(config.getString("app.job.scheduler-initializer"), Seq(
          (classOf[AppBase[_]], () => wabase),
          (classOf[ActorSystem], () => system),
          (classOf[ActorRef], () => wabaseJobActor),
        ))(system.dispatcher)
      } else logger.warn("Cannot schedule jobs, see that parameter app.job.actor is not null")
    }
    if (!config.getIsNull("app.job.on-start-job")) {
      val jobName = config.getString("app.job.on-start-job")
      doJob(jobName, Map())
    } else Future.successful(NoResult)
  }

  def doJob(jobName: String, params: Map[String, Any]): Future[Any] = {
    try invokeFunction(WabaseScheduler.executor, Seq(
      (classOf[String], () => jobName),
      (classOf[Map[String, Any]], () => params),
      (classOf[AppBase[_]], () => wabase),
      (classOf[ActorSystem], () => system),
    ))(system.dispatcher) match {
      case f: Future[_] => f
      case x => Future.successful(x)
    } catch { case NonFatal(e) => Future.failed(e) }
  }

  @annotation.nowarn("msg=Manifest")
  protected def createJobStatusController: WabaseJobStatusController =
    getObjectOrNewInstance[WabaseJobStatusController](
      config, "app.job.status-controller", "job status controller",
      Seq(wabase.dbAccess), Seq(classOf[DbAccess])
    )

  @annotation.nowarn("msg=Manifest")
  protected def createJobStatusLogger: WabaseJobStatusLogger =
    getObjectOrNewInstance[WabaseJobStatusLogger](
      config, "app.job.status-logger", "job status logger",
      Seq(wabase.dbAccess), Seq(classOf[DbAccess])
    )
}

object WabaseScheduler {
  /** Message sent to WabaseJobActor to ask to start job execution */
  case class Tick(jobName: String, params: Map[String, Any])
  sealed trait Messages
  /** message to inform sender that job has been started */
  case object JobStarted extends Messages
  /** message to inform sender that job could not be started because it is already running */
  case object JobRunning extends Messages
  /** message to inform sender that no such job exists */
  case object JobNotFound extends Messages

  def loggerName(jobName: String): String = s"job.$jobName"

  /** Job executor function - see app.job.executor parameter */
  lazy val executor: String =
    if (config.getIsNull("app.job.executor"))
      sys.error("Configuration parameter 'app.job.executor' cannot be null")
    else config.getString("app.job.executor")

  private val nameValidator = config.getString("app.job.name-validator")
  // same thread execution context for synchronous function invocation,
  // can be replaced with ExecutionContext.parasitic when scala 2.12 support is dropped
  private val syncEc: ExecutionContext = ExecutionContext.fromExecutor((r: Runnable) => r.run())

  def isJobNameValid(jobName: String, params: Map[String, Any])(wabase: AppBase[_]): Boolean =
    invokeFunction(nameValidator, Seq(
      (classOf[String], () => jobName),
      (classOf[Map[String, Any]], () => params),
      (classOf[AppBase[_]], () => wabase),
    ))(syncEc) match {
      case b: Boolean => b
      case x => sys.error(s"Job name validator '$nameValidator' must return Boolean, got '$x'")
    }

  def isJobDefined(jobName: String)(wabase: AppBase[_]): Boolean =
    wabase.qe.viewDefOption(jobName).isDefined

  def doJob(jobName: String, params: Map[String, Any])(wabase: AppBase[_], as: ActorSystem): Future[Any] = {
    val qe = wabase.qe
    val job = qe.viewDef(jobName)
    val dbAccess = wabase.dbAccess
    val loggerName = WabaseScheduler.loggerName(job.name)

    val resourcesFactory: ResourcesFactory = {
      val resTempl = dbAccess
        .withDbAccessLogger(dbAccess.tresqlResources.resourcesTemplate, loggerName)
      ResourcesFactory(dbAccess.initResources, dbAccess.closeResources)(resTempl)
    }
    implicit val executionContext: ExecutionContext = as.dispatcher
    implicit val actorSystem: ActorSystem = as
    val logger = Logger(LoggerFactory.getLogger(loggerName))

    qe.QuereaseAction(job.name, Action.Job, params, Map())(
        resourcesFactory, httpReq = null, qio = wabase.qio,
        fileStreamers = wabase.fileStreamers,
        httpClients = wabase.httpClients,
        parameterProvider = wabase.injectionParametersProvider, logger)
      .run(executionContext, actorSystem)
      .flatMap { res =>
        // consume result in the case QuereaseResultWithCleanup is returned
        qe.consumeResult(res)(QuereaseResources()(
          resourcesFactory, executionContext, actorSystem,
          httpReq = null, wabase.qio, wabase.fileStreamers,
          wabase.httpClients, wabase.injectionParametersProvider,
          logger,
        ))
      }
  }
}

class WabaseJobActor(
  wabase: AppBase[_],
  scheduler: WabaseScheduler,
  jobStatusController: WabaseJobStatusController,
  jobStatusLogger: WabaseJobStatusLogger,
) extends Actor {
  override def preStart(): Unit = {
    context.system.log.info(s"Wabase job control actor started...")
  }
  override def receive: Receive = {
    case Tick(jobName, params) =>
      if (WabaseScheduler.isJobNameValid(jobName, params)(wabase)) {
        if (jobStatusController.acquireIsRunningLock(jobName)) {
          val uuid = java.util.UUID.randomUUID().toString
          jobStatusLogger.jobStarted(uuid, jobName)
          val rF = scheduler.doJob(jobName, params)
          rF.onComplete {
            case Success(result) =>
              val details = result match {
                case s: String => s
                case StringResult(s) => s
                case _ => null
              }
              jobStatusController.updateCronJobStatus(jobName, "SUCC", details)
              jobStatusLogger.jobFinished(uuid, jobName, "SUCC", details)
            case Failure(e) =>
              jobStatusController.updateCronJobStatus(jobName, "ERR", e.getMessage)
              jobStatusLogger.jobFinished(uuid, jobName, "ERR", e.getMessage, e)
          }(context.dispatcher)
          sender() ! JobStarted
        } else sender() ! JobRunning
      } else sender() ! JobNotFound
  }

  override def postStop(): Unit = {
    context.system.log.info(s"Wabase job control actor stopped")
  }
}

/** Controls whether a job may start and records its outcome.
  *
  * [[WabaseJobActor]] calls [[acquireIsRunningLock]] before running a job and
  * [[updateCronJobStatus]] when it finishes. Configure the implementation with
  * `app.job.status-controller`.
  */
trait WabaseJobStatusController {
  /** Called on scheduler start when `app.job.clean-jobs-on-start` is true. */
  def init(): Unit
  /** Try to take the running lock for `name`. `true` — start the job; `false` — already running. */
  @annotation.nowarn("cat=deprecation")
  def acquireIsRunningLock(name: String): Boolean = acquireIsRunnningLock(name)
  @deprecated("Use acquireIsRunningLock instead", "9.0.0")
  def acquireIsRunnningLock(name: String): Boolean = acquireIsRunningLock(name)
  /** Record job outcome. `status` is `"success"` or `"error"` (aliases `"SUCC"`, `"ERR"`). */
  @deprecated("Use updateCronJobStatus(name, status, details) instead", "9.0.0")
  def updateCronJobStatus(name: String, status: String): Unit =
    updateCronJobStatus(name, status, null)
  /** Record job outcome with optional details (last success or error details). */
  def updateCronJobStatus(name: String, status: String, details: String): Unit
}

/** Database lock and status for jobs that may run on more than one node.
  *
  * Uses table `cron_job_status` (pool `app.job.job-status-cp`). A job starts only
  * if its status is not `running`, or the `running` lock is older than `app.job.max-time`
  * (node died without releasing it). Status values stored are `running`, `success`
  * and `error`. Legacy codes `RUN`, `SUCC` and `ERR` are accepted and stored as
  * those values.
  *
  * [[init]] deletes rows whose status is not `running`. For a single node with no
  * need for this table, use [[NoOpWabaseJobStatusController]]. For the previous
  * table schema (`cron_name`, `RUN` / `SUCC` / `ERR`), use
  * [[LegacyWabaseJobStatusController]]. Job run history is recorded by
  * [[WabaseJobStatusLogger]].
  */
class DefaultWabaseJobStatusController(dbAccess: DbAccess) extends WabaseJobStatusController with Loggable {

  val job_max_time = config.getDuration("app.job.max-time").toSeconds
  val jobStatusCp  = PoolName(config.getString("app.job.job-status-cp"))

  override def loggerName: String = "wabase.job-status-controller"

  private def db[A]: (Resources => A) => A =
    dbAccess.newTransaction(
      poolName = jobStatusCp,
      template = dbAccess.withDbAccessLogger(
        dbAccess.tresqlResources.resourcesTemplate,
        loggerName
      )
    )

  private def storedJobStatus(status: String): String = status match {
    case "running" | "RUN"  => "running"
    case "success" | "SUCC" => "success"
    case "error"   | "ERR"  => "error"
    case other =>
      throw new IllegalArgumentException(
        s"Unsupported job status '$other'. Use running, success, error (or RUN, SUCC, ERR).")
  }

  def init(): Unit = db { implicit res =>
    Query("-cron_job_status[status != 'running']")
  }

  /** Truncate job status details to 2000 characters. `null` is left unchanged. */
  protected def trimDetails(details: String): String =
    if (details == null || details.length <= 2000) details
    else details.substring(0, 2000)

  override def updateCronJobStatus(name: String, status: String, details: String): Unit = db {
    implicit res =>
      lazy val trimmedDetails = trimDetails(details)
      storedJobStatus(status) match {
        case "success" =>
          Query(
            """=cron_job_status[job_name = ?]
              |{ status, last_run_status, last_success_time, last_success_details, success_count }
              |[ 'success', 'success', now(), ?, success_count + 1 ]""".stripMargin, name, trimmedDetails)
        case "error" =>
          Query(
            """=cron_job_status[job_name = ?]
              |{ status, last_run_status, last_error_time, last_error_details, error_count }
              |[ 'error', 'error', now(), ?, error_count + 1 ]""".stripMargin, name, trimmedDetails)
        case "running" =>
          Query(
            """=cron_job_status[job_name = ?]
              |{ status, last_start_time }
              |[ 'running', now() ]""".stripMargin, name)
      }
  }

  override def acquireIsRunningLock(name: String): Boolean = db { implicit res =>
    Query(
      """+cron_job_status
        |{job_name, stats_since, status}
        |{?, now(), 'success'}
        |[!(cron_job_status existing[job_name = ?])]""".stripMargin, name, name)
    // Single statement to do it properly - for 'Read Committed' transaction isolation level (default in postgres)
    // Because of multiple nodes and shutdowns - ignore 'running' lock held for too long:
    if (Query(s"""=cron_job_status[
                    job_name = ? &
                    (status != 'running' | last_start_time < now() - seconds_to_interval($job_max_time))
                  ] {status, last_start_time, start_count} ['running', now(), start_count + 1]""", name)
      .affectedRowCount > 0)
      true
    else {
      Query("=cron_job_status[job_name = ?]{collision_count}[collision_count + 1]", name)
      false
    }
  }

  @deprecated("Use acquireIsRunningLock instead", "9.0.0")
  override def acquireIsRunnningLock(name: String): Boolean = acquireIsRunningLock(name)
}

/** Previous `cron_job_status` schema (`cron_name`, status `RUN` / `SUCC` / `ERR`).
  *
  * Uses table `cron_job_status` (pool `app.job.job-status-cp`). A job starts only
  * if its status is not `RUN`, or the `RUN` lock is older than `app.job.max-time`
  * (node died without releasing it).
  *
  * [[init]] deletes rows whose status is not `RUN`. For the current table schema,
  * use [[DefaultWabaseJobStatusController]].
  */
class LegacyWabaseJobStatusController(dbAccess: DbAccess) extends WabaseJobStatusController with Loggable {

  val job_max_time = config.getDuration("app.job.max-time").toSeconds
  val jobStatusCp  = PoolName(config.getString("app.job.job-status-cp"))

  override def loggerName: String = "wabase.job-status-controller"

  private def db[A]: (Resources => A) => A =
    dbAccess.newTransaction(
      poolName = jobStatusCp,
      template = dbAccess.withDbAccessLogger(
        dbAccess.tresqlResources.resourcesTemplate,
        loggerName
      )
    )

  def init(): Unit = db { implicit res =>
    Query("-cron_job_status[status != 'RUN']")
  }

  def updateCronJobStatus(name: String, status: String, details: String): Unit = db {
    implicit res => status match {
      case "SUCC" =>
        Query(
          """=cron_job_status[cron_name = ?]
            |{ status, report_time, succ_down_count }
            |[ ?, now(), succ_down_count + 1 ]""".stripMargin, name, status)
      case "ERR" =>
        Query(
          """=cron_job_status[cron_name = ?]
            |{ status, report_time, err_down_count }
            |[ ?, now(), err_down_count + 1 ]""".stripMargin, name, status)
      case _ =>
        Query("=cron_job_status[cron_name = ?]{status, report_time}[?, now()]", name, status)
    }
  }

  override def acquireIsRunningLock(name: String): Boolean = db { implicit res =>
    Query(
      """+cron_job_status
        |{id, cron_name, status, report_time}
        |{nextval('seq'), ?, 'SUCC', now()}
        |[!(cron_job_status existing[cron_name = ?])]""".stripMargin, name, name)
    // Single statement to do it properly - for 'Read Committed' transaction isolation level (default in postgres)
    // Because of multiple nodes and shutdowns - ignore 'RUN' lock held for too long:
    if (Query(s"""=cron_job_status[
                    cron_name = ? &
                    (status != 'RUN' | report_time < now() - seconds_to_interval($job_max_time))
                  ] {status, report_time, up_count} ['RUN', now(), up_count + 1]""", name)
      .affectedRowCount > 0)
      true
    else {
      Query("=cron_job_status[cron_name = ?]{collision_count}[collision_count + 1]", name)
      false
    }
  }

  @deprecated("Use acquireIsRunningLock instead", "9.0.0")
  override def acquireIsRunnningLock(name: String): Boolean = acquireIsRunningLock(name)
}

/** Always allows the job to start and does not record status.
  *
  * No lock and no `cron_job_status` table. Use when jobs run on a single node
  * and overlapping runs are acceptable or prevented by the schedule. Does not
  * stop the same job from overlapping on this node if a new tick arrives while
  * a previous run is still in progress — use [[DefaultWabaseJobStatusController]]
  * or synchronization when a job must not overlap.
  *
  * Enable with:
  * {{{
  * app.job.status-controller = org.wabase.NoOpWabaseJobStatusController
  * app.job.clean-jobs-on-start = false
  * }}}
  */
class NoOpWabaseJobStatusController extends WabaseJobStatusController {
  def init(): Unit = ()
  override def acquireIsRunningLock(name: String): Boolean = true
  def updateCronJobStatus(name: String, status: String, details: String): Unit = ()
}

/** Records each job run. [[WabaseJobActor]] calls [[jobStarted]] after taking the
  * running lock and [[jobFinished]] when the job completes. Configure with
  * `app.job.status-logger`.
  */
trait WabaseJobStatusLogger extends Loggable {
  override def loggerName: String = "wabase.job-status-logger"
  def jobStarted(uuid: String, name: String): Unit =
    logger.info(name + " started")
  def jobFinished(uuid: String, name: String, status: String, details: String): Unit =
    jobFinished(uuid, name, status, details, null)
  def jobFinished(uuid: String, name: String, status: String, details: String, error: Throwable): Unit = {
    if (error != null)
      logger.error(name, error)
    status match {
      case "ERR" | "error" =>
        logger.info(name + " failed")
      case _ =>
        logger.info(name + " completed")
    }
  }
}

/** Writes job runs to `cron_job_history` (pool `app.job.job-status-cp`).
  *
  * Columns: `uuid`, `job_name`, `start_time`, `end_time`, `status`, `details`.
  * Status values stored are `running`, `success` and `error`. Legacy codes
  * `RUN`, `SUCC` and `ERR` are accepted and stored as those values. Details are
  * trimmed to 2000 characters.
  */
class WabaseJobStatusHistoryLogger(dbAccess: DbAccess) extends WabaseJobStatusLogger {

  val jobStatusCp = PoolName(config.getString("app.job.job-status-cp"))

  private def db[A]: (Resources => A) => A =
    dbAccess.newTransaction(
      poolName = jobStatusCp,
      template = dbAccess.withDbAccessLogger(
        dbAccess.tresqlResources.resourcesTemplate,
        loggerName
      )
    )

  private def storedJobStatus(status: String): String = status match {
    case "running" | "RUN"  => "running"
    case "success" | "SUCC" => "success"
    case "error"   | "ERR"  => "error"
    case other =>
      throw new IllegalArgumentException(
        s"Unsupported job status '$other'. Use running, success, error (or RUN, SUCC, ERR).")
  }

  /** Truncate job status details to 2000 characters. `null` is left unchanged. */
  protected def trimDetails(details: String): String =
    if (details == null || details.length <= 2000) details
    else details.substring(0, 2000)

  override def jobStarted(uuid: String, name: String): Unit = {
    super.jobStarted(uuid, name)
    db { implicit res =>
      Query("+cron_job_history{uuid, job_name, start_time, status}{?, ?, now(), 'running'}", uuid, name)
    }
  }

  override def jobFinished(uuid: String, name: String, status: String, details: String, error: Throwable): Unit = {
    super.jobFinished(uuid, name, status, details, error)
    db { implicit res =>
      Query(
        """=cron_job_history[uuid = ?]
          |{end_time, status, details}[now(), ?, ?]""".stripMargin,
        uuid, storedJobStatus(status), trimDetails(details))
    }
  }
}

/** Does not record job run history. Default [[WabaseJobStatusLogger]]. */
class NoOpWabaseJobStatusLogger extends WabaseJobStatusLogger
