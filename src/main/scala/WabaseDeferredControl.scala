package org.wabase

import io.bullet.borer.Json
import org.apache.pekko.actor.{ActorSystem, Props}
import org.apache.pekko.http.scaladsl.model.{ContentTypes, HttpEntity, HttpRequest, HttpResponse, StatusCodes, Uri}
import org.apache.pekko.http.scaladsl.server.PathMatcher.Matched
import org.apache.pekko.http.scaladsl.server.PathMatchers._
import org.apache.pekko.util.ByteString
import org.wabase.DeferredControl.`X-Deferred`
import org.wabase.WabaseService.{RequestHandler, Wabase}

import scala.concurrent.Future

class WabaseDeferredControl(
  wabase: WabaseService.Wabase,
  moduleId: String = WabaseDeferredControl.defaultModuleId,
  handleCustomModules: Boolean = true,
)(implicit as: ActorSystem)
  extends DeferredControl.DeferredStatusPublisher {

  protected def initDeferredStorage: DeferredControl.DeferredStorage = {
    val factory = getObjectOrNewInstance[DeferredStorageFactory](
      config, "app.deferred-requests.storage-factory-class", "deferred storage factory")
    factory.initialize(wabase)
  }
  private val deferredStorage: DeferredControl.DeferredStorage = initDeferredStorage

  protected val cleanupActor = as.actorOf(Props(classOf[DeferredControl.DeferredCleanup], deferredStorage))

  def publishUserDeferredStatuses(user: String): Unit = {
    val deferredRequests = deferredStorage.getUserDeferredStatuses(user)
    import EventBus._
    deferredRequests.foreach { ctx =>
      if (ctx.userIdString == user) publish(Message(ServerNotifications.UserAddresseeMsg(user), ctx))
    }
  }

  def deferredResult(hash: String, user: String): HttpResponse = {
    deferredStorage.getDeferredResult(hash, user)
      .getOrElse(HttpResponse(StatusCodes.NotFound))
  }

  //Start deferred request processing flow - subscribe entry actor to DeferredRequestArrived message
  DeferredControl.startDeferredGraph(moduleId, deferredStorage, this, DeferredControl.deferredWorkerCount)
  if (handleCustomModules) {
    DeferredControl.deferredModules.foreach { case (mod, workerCount) =>
      DeferredControl.startDeferredGraph(mod, deferredStorage, this, workerCount)
    }
  }
}

trait WabaseDeferredControlFactory {
  def initialize(wabase: WabaseService.Wabase)(implicit as: ActorSystem): WabaseDeferredControl
}

object WabaseDeferredControl extends WabaseDeferredControlFactory {

  /** NOTE: Default module id must be used only for one WabaseDeferredControl instance to avoid duplicate
   * deferred routes processing! */
  val defaultModuleId: String = java.util.UUID.randomUUID().toString

  override def initialize(wabase: Wabase)(implicit as: ActorSystem): WabaseDeferredControl = {
    new WabaseDeferredControl(wabase)
  }

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

  /** Enable deferred processing for handler. */
  def maybeDeferred(innerHandler: RequestHandler): RequestHandler = ctx => {
    if (ctx.user != null && (isDeferredPath(ctx.req.uri) || hasDeferredHeader(ctx.req))) {
      doDeferred(innerHandler)(ctx)
    } else innerHandler(ctx)
  }

  def doDeferred(handler: RequestHandler): RequestHandler = ctx => {
    import EventBus._
    val timeout = extractTimeout(ctx, ctx.req)
    val dctx = ctx.copy(queryTimeout = timeout)
    val user = dctx.user.name
    val hash = DeferredControl.requestHash(user, dctx.req, WabaseAuthentication.removeSessionInfoFromRequest)
    val deferredCtx = DeferredControl.DeferredContext(user, hash, dctx, handler)
    publish(Message(DeferredControl.DeferredRequestArrived(dctx.deferred.deferredModule), deferredCtx))
    Future.successful(HttpResponse(
      entity = HttpEntity.Strict(ContentTypes.`application/json`,
        ByteString(Json.encode(Map("deferred" -> hash)).toUtf8String))
    ))
  }

  /** Get deferred request result */
  def deferredResult(deferred_id: String, ctx: WabaseRequestContext): Future[HttpResponse] = {
    Future.successful(ctx.deferred.deferredControl.deferredResult(deferred_id, ctx.user.name))
  }
}

trait DeferredStorageFactory {
  def initialize(wabase: WabaseService.Wabase)(implicit as: ActorSystem): DeferredControl.DeferredStorage
}

object DbDeferredStorageFactory extends DeferredStorageFactory {
  override def initialize(wabase: WabaseService.Wabase)(implicit as: ActorSystem): DeferredControl.DeferredStorage = {
    new DeferredControl.DbDeferredStorage(config.getConfig("app"), wabase, new ServerStatistics.NoServerStatistics {})
  }
}
