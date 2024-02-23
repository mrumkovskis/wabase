package org.wabase

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.http.scaladsl.server.RequestContext
import com.typesafe.akka.extension.quartz.QuartzSchedulerExtension
import org.wabase.WabaseScheduler.{JobResponse, Tick}

import scala.concurrent.{ExecutionContext, Future}
import org.tresql._
import org.wabase.AppMetadata.JobDef

import scala.util.control.NonFatal
import scala.util.{Failure, Success}
import scala.jdk.CollectionConverters._


class WabaseScheduler extends Loggable {
  def init(service: AppServiceBase[_]): Future[QuereaseResult] = {
    WabaseJobStatusController.init(service.app.dbAccess)

    if (config.hasPath("akka.quartz.schedules")) {
      val system = ActorSystem("wabase-cron-jobs")
      val scheduler = QuartzSchedulerExtension(system)
      val jobActorClass = try Class.forName(config.getString("app.job.actor")) catch {
        case util.control.NonFatal(ex) => throw new RuntimeException(s"Failed to get job actor class", ex)
      }
      val wabaseJobActor = system.actorOf(Props(jobActorClass, service))
      config
        .getConfig("akka.quartz.schedules")
        .root().asScala.keys
        .foreach { jobName => schedule(jobName)(service, scheduler, wabaseJobActor) }
    } else {
      logger.debug(s"No schedules found for background jobs.")
    }

    if (config.hasPath("app.init.job")) {
      val jobDef = service.app.qe.jobDef(config.getString("app.init.job"))
      doJob(jobDef)(service)
    } else Future.successful(NoResult)
  }

  protected def schedule(jobName: String)(
    service: AppServiceBase[_],
    scheduler: QuartzSchedulerExtension,
    wabaseJobActor: ActorRef
  ): Unit = {
    service.app.qe.jobDefOption(jobName).map { job =>
      scheduler.schedule(jobName, wabaseJobActor, Tick(job, this))
    }.getOrElse {
      logger.warn(s"Job definition for schedule $jobName not found." +
        s"If you would like to schedule please override this method or define wabase job.")
    }
  }

  def doJob(job: JobDef)(service: AppServiceBase[_]): Future[QuereaseResult] = {
    val qe = service.app.qe
    val dbAccess = service.app.asInstanceOf[DbAccess]

    val emptyResFactory: ResourcesFactory = {
      val resTempl = dbAccess
        .withDbAccessLogger(dbAccess.tresqlResources.resourcesTemplate, s"${job.name}.job")
      val poolName = Option(job.db).map(PoolName) getOrElse dbAccess.DefaultCp
      val extraDbs = dbAccess.extraDb(job.dbAccessKeys)
      val initRes = () => dbAccess.initResources(resTempl)(poolName, extraDbs)
        ResourcesFactory(initRes, dbAccess.closeResources)(resTempl)
    }
    implicit val resourcesFactory: ResourcesFactory =
      if (job.explicitDb) emptyResFactory
      else emptyResFactory.copy()(resources = emptyResFactory.initResources())
    implicit val executionContext: ExecutionContext = service.asInstanceOf[Execution].executor
    implicit val actorSystem: ActorSystem = service.asInstanceOf[Execution].system
    implicit val fileStreamer: FileStreamer = service match {
      case s: AppFileServiceBase[_] => s.fileStreamer.fileStreamer
      case _ => null
    }
    implicit val httpCtx: RequestContext = null
    implicit val qio: AppQuereaseIo[Dto] = service.app.qio

    val ctx =
      qe.ActionContext(job.name, "job", Map(), None, qe.quereaseActionLogger(s"${job.name}.job"))
    qe.doSteps(job.action.steps, ctx, Future.successful(Map()))
  }
}

object WabaseScheduler {
  case class Tick(job: JobDef, executor: WabaseScheduler)
  case class JobResponse(data: Any) // for testing purposes, scheduler.schedule will ignore these messages
}

class WabaseJobActor(service: AppServiceBase[_]) extends Actor {
  override def receive: Receive = {
    case Tick(jd, scheduler) =>
      val s = sender()
      val jobName = jd.name
      val dbAccess = service.app.asInstanceOf[DbAccess]

      try {
        if (WabaseJobStatusController.acquireIsRunnningLock(jobName)(dbAccess)) {
          context.system.log.info(jobName + " started")
          scheduler.doJob(jd)(service).onComplete {
            case Success(r) =>
              WabaseJobStatusController.updateCronJobStatus(jobName, "SUCC")(dbAccess)
              context.system.log.info(jobName + " ended")
              s ! JobResponse(r)
            case Failure(e) =>
              context.system.log.error(e, jobName)
              WabaseJobStatusController.updateCronJobStatus(jobName, "ERR")(dbAccess)
              context.system.log.info(jobName + " ended with error")
              s ! JobResponse(e)
          }(context.dispatcher)
        } else s ! JobResponse(())
      } catch {
        case NonFatal(e) =>
          s ! JobResponse(e)
          throw e
      }
  }
}

object WabaseJobStatusController {

  val job_max_time = config.getString("app.job.max-time")

  def init(dbAccess: DbAccess) = dbAccess.transaction() { implicit res =>
    Query("-cron_job_status[status != 'RUN']")
  }

  def updateCronJobStatus(name: String, status: String)(dbAccess: DbAccess): Unit = dbAccess.transaction() {
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

  def acquireIsRunnningLock(name: String)(dbAccess: DbAccess): Boolean = dbAccess.transaction() { implicit res =>
    Query(
      """+cron_job_status
        |{id, cron_name, status, report_time}
        |{nextval('seq'), ?, 'SUCC', now()}
        |[!(cron_job_status existing[cron_name = ?])]""".stripMargin, name, name)
    // Single statement to do it properly - for 'Read Committed' transaction isolation level (default in postgres)
    // Because of multiple nodes and shutdowns - ignore 'RUN' lock held for too long:
    if (Query(s"""=cron_job_status[
                    cron_name = ? &
                    (status != 'RUN' | report_time < now() - '$job_max_time'::interval)
                  ] {status, report_time, up_count} ['RUN', now(), up_count + 1]""", name)
      .affectedRowCount > 0)
      true
    else {
      Query("=cron_job_status[cron_name = ?]{collision_count}[collision_count + 1]", name)
      false
    }
  }
}
