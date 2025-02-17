package org.wabase

import io.bullet.borer.Json
import org.apache.pekko.actor.{ActorSystem, Props}
import org.apache.pekko.http.scaladsl.model.{ContentTypes, HttpEntity, HttpRequest, HttpResponse, StatusCodes, Uri}
import org.apache.pekko.http.scaladsl.server.PathMatcher.Matched
import org.apache.pekko.http.scaladsl.server.PathMatchers._
import org.apache.pekko.util.ByteString
import org.wabase.DeferredControl.`X-Deferred`
import org.wabase.WabaseService.Wabase

import scala.concurrent.Future

class WabaseDeferredControl(
  wabase: WabaseService.Wabase,
  moduleId: String = WabaseDeferredControl.defaultModuleId,
  handleCustomModules: Boolean = true,
)(implicit as: ActorSystem)
  extends DeferredControl.DeferredStatusPublisher {

  protected def initDeferredStorage: DeferredControl.DeferredStorage = {
    val factory = getObjectOrNewInstance[DeferredStorageFactory](
      config, "app.deferred-request.storage-factory-class", "deferred storage factory")
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

  /** Request mapper to enable deferred processing for route. NOTE: Request mappers are not processed in deferred
   * mode! */
  def enableDeferred(ctx: WabaseRequestContext, req: HttpRequest): WabaseRequestContext = {
    if (ctx.user != null && (isDeferredPath(req.uri) || hasDeferredHeader(req))) {
      val timeout = extractTimeout(ctx, req)
      ctx.copy(queryTimeout = timeout, deferred = ctx.deferred.copy(isDeferred = true))
    }
    else ctx
  }

  /** Request mapper to get deferred request result. WabaseRequestContext key field must be set to deferred result hash */
  def deferredResult(ctx: WabaseRequestContext): HttpResponse = {
    ctx.deferred.deferredControl.deferredResult(ctx.key.mkString, ctx.user.name)
  }

  def doDeferred(ctx: WabaseRequestContext, routeFun: WabaseRequestContext => Future[HttpResponse]): HttpResponse = {
    import EventBus._
    val user = ctx.user.name
    val hash = DeferredControl.requestHash(user, ctx.req, WabaseAuthentication.removeSessionInfoFromRequest)
    val deferredCtx = DeferredControl.DeferredContext(user, hash, ctx, routeFun)
    publish(Message(DeferredControl.DeferredRequestArrived(ctx.deferred.deferredModule), deferredCtx))
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
    new DeferredControl.DbDeferredStorage(config.getConfig("app"), wabase, new ServerStatistics.NoServerStatistics {})
  }
}
