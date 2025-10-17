package org.wabase
package scheduler

import org.apache.pekko.actor.{ActorRef, ActorSystem}
import org.apache.pekko.extension.quartz.QuartzSchedulerExtension
import org.wabase.WabaseScheduler.Tick

import scala.jdk.CollectionConverters._

object QuartzScheduler extends Loggable {

  def init(wabase: AppBase[_], as: ActorSystem, jobControlActor: ActorRef): Unit = {
    def schedule(jobName: String, params: Map[String, Any])(
      scheduler: QuartzSchedulerExtension,
      wabaseJobActor: ActorRef
    ): Unit = {
      wabase.qe.viewDefOption(jobName).map { job =>
        scheduler.schedule(jobName, wabaseJobActor, Tick(job, params))
      }.getOrElse {
        logger.warn(s"Job definition for schedule $jobName not found." +
          s"If you would like to schedule please override this method or define wabase job.")
      }
    }

    if (config.hasPath("pekko.quartz.schedules")) {
      val scheduler = QuartzSchedulerExtension(as)
      config
        .getConfig("pekko.quartz.schedules")
        .root().asScala.keys
        .foreach { jobName =>
          val enabled =
            Option(s"pekko.quartz.schedules.$jobName.enabled")
              .filter(config.hasPath).forall(config.getBoolean)
          if (enabled) {
            logger.debug(s"Scheduling job '$jobName'")
            val params: Map[String, Any] = Option(s"pekko.quartz.schedules.$jobName.params")
              .filter(config.hasPath)
              .map(config.getValue)
              .map(_.unwrapped())
              .map(AppQuerease.configValueAsScala)
              .collect { case p: Map[String, Any]@unchecked => p }
              .getOrElse(Map())
            schedule(jobName, params)(scheduler, jobControlActor)
          } else {
            logger.info(s"Job '$jobName' is disabled in configuration, will not be scheduled")
          }
        }
    } else {
      logger.debug(s"No schedules found for background jobs.")
    }
  }
}
