package org.wabase

import akka.actor.{Actor, Props}
import akka.http.scaladsl.server.Route

import scala.concurrent.Future
import scala.concurrent.duration._
import akka.stream.scaladsl.{Sink, Source}
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.language.postfixOps

trait ServerStatistics extends Loggable {
  def registerTimeout: Unit
  // TODO fix namespace problem and method names
  def statsRegisterDeferredRequest: Unit
  def statsRegisterDeferredResult: Unit
  def registerStats(stats: Statistics): Unit = { sys.error ("Unimplemented") }
}

object ServerStatistics {
 trait NoServerStatistics extends ServerStatistics {
  def registerTimeout: Unit = {}
  def statsRegisterDeferredRequest: Unit = {}
  def statsRegisterDeferredResult: Unit = {}
 }

 trait DefaultServerStatistics extends ServerStatistics { this: Execution =>

  import DefaultServerStatistics._

  private val statsActor = system.actorOf(Props(classOf[StatsActor], this))

  val startTime = currentTime

  def statsTimerRefreshInterval: FiniteDuration = 1 minute

  Source
   .tick(statsTimerRefreshInterval, statsTimerRefreshInterval, GetStats)
   .runWith(Sink.actorRef(statsActor, GetStats,
     (e) => { logger.error("Failed to registers server statistics", e) }))

  def statsRoute(route: Route): Route = {ctx =>
    statsActor ! RequestNotification
    val result = route(ctx)
    result.onComplete (_ => statsActor ! ResponseNotification)
    result
  }

  def registerTimeout = statsActor ! RegisterTimeout
  def statsRegisterDeferredRequest = statsActor ! RegisterDeferredRequest
  def statsRegisterDeferredResult = statsActor ! RegisterDeferredResult
 }

 case class Statistics(
  uptime: Long,
  acceptedRequests: Long,
  processedRequests: Long,
  timedOutRequests: Long,
  maxConcurrentRequests: Long,
  maxConcurrentRequestsTime: Long,
  acceptedDeferredRequests: Long,
  processedDeferredRequests: Long,
  maxConcurrentDeferredRequests: Long)

 object DefaultServerStatistics {
  case object GetStats
  case object RegisterTimeout
  case object RegisterDeferredRequest
  case object RegisterDeferredResult
  private case object RequestNotification
  private case object ResponseNotification

  class StatsActor(stats: DefaultServerStatistics) extends Actor with Loggable {
    var acceptedRequests: Long = _
    var processedRequests: Long = _
    var timedOutRequests: Long = _
    var concurrentRequests: Long = _
    var maxConcurrentRequests: Long = _
    var maxConcurrentRequestsTime: Long = _

    var acceptedDeferredRequests: Long = _
    var processedDeferredRequests: Long = _
    var concurrentDeferredRequests: Long = _
    var maxConcurrentDeferredRequests: Long = _

    override def preStart = {
      acceptedRequests = 0
      processedRequests = 0
      timedOutRequests = 0
      maxConcurrentRequests = 0
      concurrentRequests = 0
      maxConcurrentRequestsTime = 0
      acceptedDeferredRequests = 0
      processedDeferredRequests = 0
      concurrentDeferredRequests = 0
      maxConcurrentDeferredRequests = 0
      logger.info("Stats actor started")
    }

    def receive = {
      case GetStats => Future { stats.registerStats(Statistics(
        currentTime - stats.startTime,
        acceptedRequests,
        processedRequests,
        timedOutRequests,
        maxConcurrentRequests,
        maxConcurrentRequestsTime,
        // deferred
        acceptedDeferredRequests,
        processedDeferredRequests,
        maxConcurrentDeferredRequests))
      } (context.dispatcher)
      case RegisterTimeout =>
        timedOutRequests += 1
      case RegisterDeferredRequest =>
        acceptedDeferredRequests += 1
        concurrentDeferredRequests += 1
        if (concurrentDeferredRequests > maxConcurrentDeferredRequests) {
          maxConcurrentDeferredRequests = concurrentDeferredRequests
        }
      case RegisterDeferredResult =>
        processedDeferredRequests += 1
        concurrentDeferredRequests -= 1
      case RequestNotification =>
        acceptedRequests += 1
        concurrentRequests += 1
        if (concurrentRequests > maxConcurrentRequests) {
          maxConcurrentRequests  = concurrentRequests
          maxConcurrentRequestsTime = currentTime
        }
      case ResponseNotification =>
        processedRequests += 1
        concurrentRequests -= 1
    }

    override def postStop = {
      logger.info("Stats actor stopped")
    }
  }
 }
}
