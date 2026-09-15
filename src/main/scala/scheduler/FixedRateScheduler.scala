package org.wabase
package scheduler

import com.typesafe.config.Config
import org.apache.pekko.actor.{ActorRef, ActorSystem}
import org.wabase.WabaseScheduler.Tick

import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.jdk.CollectionConverters._
import scala.util.Random

/** Fixed-rate job scheduler using Pekko's built-in scheduler.
  *
  * Simpler to configure than [[QuartzScheduler]] and avoids the extra
  * `pekko-quartz-scheduler` dependency. Use this for jobs that run at a constant
  * interval. For cron expressions and other complex schedules, use [[QuartzScheduler]].
  *
  * Enable with:
  * {{{
  * app.job.scheduler-initializer = org.wabase.scheduler.FixedRateScheduler.init
  * app.job.schedules {
  *   my_job {
  *     enabled  = true   // optional, default true
  *     interval = 1m     // required
  *     // initial-delay = 5s  // optional; default random 1..min(interval, 59) seconds
  *     // params { key = value }
  *   }
  * }
  * }}}
  *
  * Unless `initial-delay` is set, the first run is delayed by a random 1 to
  * min(interval, 59) seconds so jobs do not all start at once after process start.
  */
object FixedRateScheduler extends Loggable {

  val SchedulesPath = "app.job.schedules"

  /** Reads `app.job.schedules` and starts enabled jobs on `jobControlActor`. */
  def init(wabase: AppBase[_], as: ActorSystem, jobControlActor: ActorRef): Unit =
    scheduleJobs(config, wabase, as, jobControlActor)

  private[wabase] def scheduleJobs(
    cfg: Config,
    wabase: AppBase[_],
    as: ActorSystem,
    jobControlActor: ActorRef,
  ): Unit = {
    implicit val ec: ExecutionContext = as.dispatcher
    def schedule(jobName: String, interval: FiniteDuration, initialDelay: FiniteDuration, params: Map[String, Any])(
      wabaseJobActor: ActorRef
    ): Unit = {
      if (WabaseScheduler.isJobNameValid(jobName, params)(wabase)) {
        logger.info(s"Scheduling job '$jobName' every $interval (initial delay $initialDelay)")
        as.scheduler.scheduleAtFixedRate(initialDelay, interval, wabaseJobActor, Tick(jobName, params))
      } else
        logger.warn(s"Job definition for schedule $jobName not found. " +
          s"If you would like to schedule please override this method or define wabase job.")
    }

    if (cfg.hasPath(SchedulesPath)) {
      cfg
        .getConfig(SchedulesPath)
        .root().asScala.keys
        .foreach { jobName =>
          val jobPath = s"$SchedulesPath.$jobName"
          val jobCfg = cfg.getConfig(jobPath)
          val enabled =
            Option(s"$jobPath.enabled")
              .filter(cfg.hasPath).forall(cfg.getBoolean)
          if (!enabled)
            logger.info(s"Job '$jobName' is disabled in configuration, will not be scheduled")
          else if (!jobCfg.hasPath("interval"))
            logger.warn(s"Job '$jobName' has no interval, will not be scheduled")
          else {
            val interval: FiniteDuration = toFiniteDuration(jobCfg.getDuration("interval"))
            if (interval <= Duration.Zero)
              logger.warn(s"Job '$jobName' interval $interval is not positive, will not be scheduled")
            else {
              val initialDelay: FiniteDuration =
                if (jobCfg.hasPath("initial-delay"))
                  toFiniteDuration(jobCfg.getDuration("initial-delay"))
                else
                  randomInitialDelay(interval)
              if (initialDelay < Duration.Zero)
                logger.warn(s"Job '$jobName' initial-delay $initialDelay is negative, will not be scheduled")
              else {
                val params: Map[String, Any] = Option(s"$jobPath.params")
                  .filter(cfg.hasPath)
                  .map(cfg.getValue)
                  .map(_.unwrapped())
                  .map(AppQuerease.configValueAsScala)
                  .collect { case p: Map[String, Any]@unchecked => p }
                  .getOrElse(Map())
                schedule(jobName, interval, initialDelay, params)(jobControlActor)
              }
            }
          }
        }
    } else {
      logger.warn(s"No $SchedulesPath configured, no jobs will be scheduled")
    }
  }

  /** Random delay of 1 to min(interval, 59) seconds so jobs do not all start at once. */
  private def randomInitialDelay(interval: FiniteDuration): FiniteDuration = {
    val initialDelayMax = math.max(2L, math.min(interval.toSeconds, 59L))
    FiniteDuration(1L + Random.nextInt((initialDelayMax - 1L).toInt), TimeUnit.SECONDS)
  }
}
