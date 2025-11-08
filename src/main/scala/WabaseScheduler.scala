package org.wabase

import com.typesafe.scalalogging.Logger
import org.apache.pekko.actor.{Actor, ActorRef, ActorSystem, Props}
import org.mojoz.metadata.ViewDef
import org.slf4j.LoggerFactory
import org.wabase.WabaseScheduler.{JobRunning, JobStarted, Tick}
import org.tresql._
import org.wabase.AppMetadata.Action

import scala.concurrent.{ExecutionContext, Future}
import scala.language.existentials
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

class WabaseScheduler(wabase: AppBase[_], system: ActorSystem) extends Loggable {
  def init(): Future[QuereaseResult] = {
    if (config.getBoolean("app.job.clean-jobs-on-start"))
      WabaseJobStatusController.init(wabase.dbAccess)
    val wabaseJobActor = if (config.getIsNull("app.job.actor")) null else try {
      val jobActorClass = Class.forName(config.getString("app.job.actor"))
      system.actorOf(Props(jobActorClass, wabase, this), config.getString("app.job.actor-name"))
    } catch {
      case NonFatal(ex) => throw new RuntimeException(s"Failed to start job actor", ex)
    }
    if (!config.getIsNull("app.job.scheduler-initializer")) {
      if (wabaseJobActor != null) {
        val (clazz, initFun) = OpParser.classNameFunctionName(config.getString("app.job.scheduler-initializer"))
        invokeFunction(clazz, initFun, Seq(
          (classOf[AppBase[_]], () => wabase),
          (classOf[ActorSystem], () => system),
          (classOf[ActorRef], () => wabaseJobActor),
        ))(system.dispatcher)
      } else logger.warn("Cannot schedule jobs, see that parameter app.job.actor is not null")
    }
    if (!config.getIsNull("app.job.on-start-job")) {
      val jobDef = wabase.qe.viewDef(config.getString("app.job.on-start-job"))
      doJob(jobDef, Map())
    } else Future.successful(NoResult)
  }

  def doJob(job: ViewDef, params: Map[String, Any]): Future[QuereaseResult] = {
    val qe = wabase.qe
    val dbAccess = wabase.dbAccess
    val loggerName = s"${job.name}.job"

    val resourcesFactory: ResourcesFactory = {
      val resTempl = dbAccess
        .withDbAccessLogger(dbAccess.tresqlResources.resourcesTemplate, loggerName)
      val initRes = dbAccess.initResources(resTempl)
        ResourcesFactory(initRes, dbAccess.closeResources)(resTempl)
    }
    implicit val executionContext: ExecutionContext = system.dispatcher
    implicit val actorSystem: ActorSystem = system
    val logger = Logger(LoggerFactory.getLogger(loggerName))

    qe.QuereaseAction(job.name, Action.Job, params, Map(), doCleanup = true)(
        resourcesFactory, httpReq = null, qio = wabase.qio,
        fileStreamers = wabase.fileStreamers,
        httpClients = wabase.httpClients,
        parameterProvider = wabase.injectionParametersProvider, logger)
      .run(executionContext, actorSystem)
  }
}

object WabaseScheduler {
  /** Message sent to WabaseJobActor to ask to start job execution */
  case class Tick(job: ViewDef, params: Map[String, Any])
  /** message to inform sender that job has been started */
  case object JobStarted
  /** message to inform sender that job could not be started because it is already running */
  case object JobRunning
}

class WabaseJobActor(wabase: AppBase[_], scheduler: WabaseScheduler) extends Actor {
  override def preStart(): Unit = {
    context.system.log.info(s"Wabase job control actor started...")
  }
  override def receive: Receive = {
    case Tick(jd, params) =>
      val jobName = jd.name
      val dbAccess = wabase.dbAccess
      try {
        if (WabaseJobStatusController.acquireIsRunnningLock(jobName)(dbAccess)) {
          context.system.log.info(jobName + " started")
          scheduler.doJob(jd, params).onComplete {
            case Success(_) =>
              WabaseJobStatusController.updateCronJobStatus(jobName, "SUCC")(dbAccess)
              context.system.log.info(jobName + " ended")
            case Failure(e) =>
              context.system.log.error(e, jobName)
              WabaseJobStatusController.updateCronJobStatus(jobName, "ERR")(dbAccess)
              context.system.log.info(jobName + " ended with error")
          }(context.dispatcher)
          sender() ! JobStarted
        } else sender() ! JobRunning
      } catch {
        case NonFatal(e) =>
          throw e
      }
  }

  override def postStop(): Unit = {
    context.system.log.info(s"Wabase job control actor stopped")
  }
}

object WabaseJobStatusController {

  val job_max_time = config.getString("app.job.max-time")
  val jobStatusCp  = PoolName(config.getString("app.job.job-status-cp"))

  def init(dbAccess: DbAccess): Unit = dbAccess.newTransaction(jobStatusCp) { implicit res =>
    Query("-cron_job_status[status != 'RUN']")
  }

  def updateCronJobStatus(name: String, status: String)(dbAccess: DbAccess): Unit = dbAccess.newTransaction(jobStatusCp) {
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

  def acquireIsRunnningLock(name: String)(dbAccess: DbAccess): Boolean = dbAccess.newTransaction(jobStatusCp) { implicit res =>
    Query(
      """+cron_job_status
        |{id, cron_name, status, report_time}
        |{nextval('seq'), ?, 'SUCC', now()}
        |[!(cron_job_status existing[cron_name = ?])]""".stripMargin, name, name)
    // Single statement to do it properly - for 'Read Committed' transaction isolation level (default in postgres)
    // Because of multiple nodes and shutdowns - ignore 'RUN' lock held for too long:
    if (Query(s"""=cron_job_status[
                    cron_name = ? &
                    (status != 'RUN' | report_time < now() - `$job_max_time`)
                  ] {status, report_time, up_count} ['RUN', now(), up_count + 1]""", name)
      .affectedRowCount > 0)
      true
    else {
      Query("=cron_job_status[cron_name = ?]{collision_count}[collision_count + 1]", name)
      false
    }
  }
}
