package org.wabase

import com.typesafe.scalalogging.Logger
import org.apache.pekko.actor.{Actor, ActorRef, ActorSystem, Props}
import org.slf4j.LoggerFactory
import org.wabase.WabaseScheduler.{JobRunning, JobStarted, NoJob, Tick}
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
    if (config.getBoolean("app.job.clean-jobs-on-start"))
      jobStatusController.init()
    val wabaseJobActor = if (config.getIsNull("app.job.actor")) null else try {
      val jobActorClass = Class.forName(config.getString("app.job.actor"))
      system.actorOf(Props(jobActorClass, wabase, this, jobStatusController), config.getString("app.job.actor-name"))
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
  case object NoJob extends Messages

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

  def isJobNameValid(jobName: String)(wabase: AppBase[_]): Boolean =
    invokeFunction(nameValidator, Seq(
      (classOf[String], () => jobName),
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
) extends Actor {
  override def preStart(): Unit = {
    context.system.log.info(s"Wabase job control actor started...")
  }
  override def receive: Receive = {
    case Tick(jobName, params) =>
      if (WabaseScheduler.isJobNameValid(jobName)(wabase)) {
        if (jobStatusController.acquireIsRunnningLock(jobName)) {
          context.system.log.info(jobName + " started")
          val rF = scheduler.doJob(jobName, params)
          rF.onComplete {
            case Success(_) =>
              jobStatusController.updateCronJobStatus(jobName, "SUCC")
              context.system.log.info(jobName + " ended")
            case Failure(e) =>
              context.system.log.error(e, jobName)
              jobStatusController.updateCronJobStatus(jobName, "ERR")
              context.system.log.info(jobName + " ended with error")
          }(context.dispatcher)
          sender() ! JobStarted
        } else sender() ! JobRunning
      } else sender() ! NoJob
  }

  override def postStop(): Unit = {
    context.system.log.info(s"Wabase job control actor stopped")
  }
}

trait WabaseJobStatusController {
  def init(): Unit
  def acquireIsRunnningLock(name: String): Boolean
  def updateCronJobStatus(name: String, status: String): Unit
}

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

  def init(): Unit = db { implicit res =>
    Query("-cron_job_status[status != 'RUN']")
  }

  def updateCronJobStatus(name: String, status: String): Unit = db {
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

  def acquireIsRunnningLock(name: String): Boolean = db { implicit res =>
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
}
