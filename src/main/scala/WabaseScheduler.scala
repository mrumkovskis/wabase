package org.wabase

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.http.scaladsl.model.HttpRequest
import com.typesafe.akka.extension.quartz.QuartzSchedulerExtension
import org.tresql.Resources
import org.wabase.WabaseScheduler.{JobResponse, Tick}

import scala.concurrent.{ExecutionContext, Future}
import org.tresql._
import org.wabase.AppMetadata.JobDef

import scala.util.control.NonFatal
import scala.util.{Failure, Success}
import scala.jdk.CollectionConverters._


trait WabaseScheduler extends Loggable {
  def init(service: AppServiceBase[_]): Unit
  def scheduleJob(jobName: String)(service: AppServiceBase[_],
                                   scheduler: QuartzSchedulerExtension,
                                   wabaseCronActor: ActorRef): Unit = {
    service.app.qe.jobDefOption(jobName).map { job =>
      scheduler.schedule(jobName, wabaseCronActor, Tick(job))
    }.getOrElse {
      logger.warn(s"Job definition for schedule $jobName not found." +
        s"If you would like to schedule please override this method or define wabase job.")
    }
  }
}

object WabaseScheduler extends WabaseScheduler {
  case class Tick(job: JobDef)
  case class JobResponse(data: Any) // for testing purposes, scheduler.schedule will ignore these messages

  def init(service: AppServiceBase[_]): Unit = {
    val system = ActorSystem("wabase-cron-jobs")
    if (config.hasPath("akka.quartz.schedules")) {
      val scheduler = QuartzSchedulerExtension(system)
      val wabaseJobActor = system.actorOf(Props(classOf[WabaseJobActor], service))
      config
        .getConfig("akka.quartz.schedules")
        .root().asScala.keys
        .foreach(scheduleJob(_)(service, scheduler, wabaseJobActor))
    } else {
      logger.debug(s"No schedules found for background jobs.")
    }
  }
}

class WabaseJobActor(service: AppServiceBase[_]) extends Actor {
  override def receive: Receive = {
    case Tick(jd) =>
      val s = sender()
      val jobName = jd.name
      val dbAccess = service.app.asInstanceOf[DbAccess]

      try {
        if (WabaseJobStatusController.acquireIsRunnningLock(jobName)(dbAccess)) {
          context.system.log.info(jobName + " started")
          job(jd).onComplete {
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

  def job(jd: JobDef): Future[QuereaseResult] = {
    def resourceFactory(jd: AppMetadata.JobDef, dbAccess: DbAccess): () => Resources = {
      val poolName = Option(jd.db).map(PoolName) getOrElse dbAccess.DefaultCp
      val extraDbs = dbAccess.extraDb(jd.dbAccessKeys)
      () => dbAccess.initResources(dbAccess.tresqlResources.resourcesTemplate)(poolName, extraDbs)
    }

    val qe = service.app.qe
    val dbAccess = service.app.asInstanceOf[DbAccess]

    val emptyResFactory: ResourcesFactory =
      ResourcesFactory(resourceFactory(jd, dbAccess), dbAccess.closeResources)(dbAccess.tresqlResources)
    implicit val resourcesFactory: ResourcesFactory =
      if (jd.explicitDb) emptyResFactory
      else emptyResFactory.copy()(resources = emptyResFactory.initResources())
    implicit val executionContext: ExecutionContext = service.asInstanceOf[Execution].executor
    implicit val actorSystem: ActorSystem = service.asInstanceOf[Execution].system
    implicit val fileStreamer: FileStreamer = service match {
      case s: AppFileServiceBase[_] => s.fileStreamer.fileStreamer
      case _ => null
    }
    implicit val httpRequest: HttpRequest = null
    implicit val qio: AppQuereaseIo[Dto] = service.app.qio

    val ctx = qe.ActionContext(jd.name, "job", Map(), None)
    qe.doSteps(jd.steps.steps, ctx, Future.successful(Map()))
  }
}

object WabaseJobStatusController {
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
    if (Query("""=cron_job_status[
                    cron_name = ? &
                    (status != 'RUN' | report_time < now() - '1 hour'::interval)
                  ] {status, report_time, up_count} ['RUN', now(), up_count + 1]""", name)
      .affectedRowCount > 0)
      true
    else {
      Query("=cron_job_status[cron_name = ?]{collision_count}[collision_count + 1]", name)
      false
    }
  }
}
