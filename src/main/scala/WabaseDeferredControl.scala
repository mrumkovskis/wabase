package org.wabase

import io.bullet.borer.Json
import org.apache.pekko.actor.{ActorSystem, Props}
import org.apache.pekko.http.scaladsl.model.{ContentTypes, HttpEntity, HttpRequest, HttpResponse, Uri}
import org.apache.pekko.http.scaladsl.server.PathMatcher.Matched
import org.apache.pekko.http.scaladsl.server.PathMatchers._
import org.apache.pekko.util.ByteString
import org.wabase.DeferredControl.`X-Deferred`

import scala.concurrent.Future

class WabaseDeferredControl(wabase: WabaseService.Wabase)(implicit as: ActorSystem)
  extends DeferredControl.DeferredStatusPublisher {

  protected def initDeferredStorage: DeferredControl.DeferredStorage = {
    val className = config.getString("app.deferred-request.storage-factory-class")
    val factory = getObjectOrNewInstance(className, "Deferred storage factory")
    factory.asInstanceOf[DeferredStorageFactory].initialize(wabase)
  }
  private val deferredStorage: DeferredControl.DeferredStorage = initDeferredStorage
  val moduleId: String = java.util.UUID.randomUUID.toString

  protected val cleanupActor = as.actorOf(Props(classOf[DeferredControl.DeferredCleanup], deferredStorage))

  def publishUserDeferredStatuses(user: String): Unit = {
    val deferredRequests = deferredStorage.getUserDeferredStatuses(user)
    import EventBus._
    deferredRequests.foreach { ctx =>
      if (ctx.userIdString == user) publish(Message(ServerNotifications.UserAddresseeMsg(user), ctx))
    }
  }

  //Start deferred request processing flow - subscribe entry actor to DeferredRequestArrived message
  DeferredControl.startDeferredGraph(moduleId, deferredStorage, this, DeferredControl.deferredWorkerCount)
  DeferredControl.deferredModules.foreach { case (mod, workerCount) =>
    DeferredControl.startDeferredGraph(mod, deferredStorage, this, workerCount)
  }
}

object WabaseDeferredControl {
  def isDeferredPath(uri: Uri): Boolean = {
    val pm = Slash.? ~ Segment
    pm(uri.path) match {
      case Matched(_, segment) => DeferredControl.deferredUris contains segment._1
      case _ => false
    }
  }

  def hasDeferredHeader(req: HttpRequest): Boolean = {
    WabaseService.optionalHttpHeaderValuePF(req) {
      case `X-Deferred`(timeoutString) => `X-Deferred`(timeoutString).timeout != Left(false)
    } getOrElse(false)
  }

  def extractTimeout(ctx: WabaseRequestContext, req: HttpRequest): QueryTimeout = {
    def deferredTimeout(viewName: Option[String], timeout: Option[Int]): QueryTimeout =
      DeferredControl.deferredTimeout(viewName, timeout,
        DeferredControl.deferredTimeouts, DeferredControl.defaultTimeout)

    WabaseService.optionalHttpHeaderValuePF(req) {
      case `X-Deferred`(timeoutString) => `X-Deferred`(timeoutString).timeout
    } map {
      case Right(timeoutDuration) => deferredTimeout(None, Option(timeoutDuration.toSeconds.toInt))
      case Left(true) if ctx.viewName != null => deferredTimeout(Option(ctx.viewName), None)
      case _ => deferredTimeout(None, None)
    } getOrElse(QueryTimeout(config.getDuration("jdbc.query-timeout").toSeconds.toInt))
  }

  def enableDeferred(ctx: WabaseRequestContext, req: HttpRequest): WabaseRequestContext = {
    if (ctx.user != null && (isDeferredPath(req.uri) || hasDeferredHeader(req))) {
      val timeout = extractTimeout(ctx, req)
      if (ctx.deferredModule == null) ctx.copy(deferredModule = ctx.deferredControl.moduleId, queryTimeout = timeout)
      else ctx.copy(queryTimeout = timeout)
    }
    else ctx
  }

  def doDeferred(ctx: WabaseRequestContext, routeFun: WabaseRequestContext => Future[HttpResponse]): HttpResponse = {
    import EventBus._
    val user = ctx.user.name
    val hash = DeferredControl.requestHash(user, ctx.req, WabaseAuthentication.removeSessionInfoFromRequest)
    val deferredCtx = DeferredControl.DeferredContext(user, hash, ctx, routeFun)
    publish(Message(DeferredControl.DeferredRequestArrived(ctx.deferredModule), deferredCtx))
    HttpResponse(
      entity = HttpEntity.Strict(ContentTypes.`application/json`,
        ByteString(Json.encode(Map("deferred" -> hash)).toUtf8String))
    )
  }
}

trait DeferredStorageFactory {
  def initialize(wabase: WabaseService.Wabase)(implicit as: ActorSystem): DeferredControl.DeferredStorage
}

object DbDeferredStorageFactory extends DeferredStorageFactory {
  override def initialize(wabase: WabaseService.Wabase)(implicit as: ActorSystem): DeferredControl.DeferredStorage = {
    new DeferredControl.DbDeferredStorage(config, wabase, new ServerStatistics.NoServerStatistics {})
  }
}
