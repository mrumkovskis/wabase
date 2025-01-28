package org.wabase

import org.apache.pekko.http.scaladsl.model.HttpMethods._
import org.apache.pekko.http.scaladsl.model.Uri.Path
import org.apache.pekko.http.scaladsl.model.Uri.Path.{Empty, Segment, SlashOrEmpty}
import AppMetadata._
import com.typesafe.scalalogging.Logger
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.marshalling.ToResponseMarshallable
import org.apache.pekko.http.scaladsl.model.headers.{Cookie, HttpCookie, `Set-Cookie`}
import org.apache.pekko.http.scaladsl.model.{DateTime, HttpHeader, HttpRequest, HttpResponse}
import org.wabase.AppMetadata.{Action, RouteDef}
import org.wabase.WabaseService.Wabase

import java.util.Locale
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

case class WabaseUser(properties: Map[String, Any]) {
  val id: Long      = properties.get("id").collect { case x: Number => x.longValue }.getOrElse(-1)
  val name: String  = properties.get("name").map(String.valueOf).orNull
  val roles: Set[String] = properties.get("roles")
    .collect { case r: Iterable[String@unchecked] => r.toSet }.getOrElse(Set())
}

case class WabaseRequestContext(
  wabase: Wabase,
  req: HttpRequest,
  logger: Logger,
  route: RouteDef = null,
  viewName: String = null,
  action: String = null,
  key: Seq[Any] = Nil,
  applicationState: ApplicationState = null,
  user: WabaseUser = null,
  as: ActorSystem = null,
)

class WabaseRouteException(message: String) extends Exception(message)

class WabaseService extends Loggable {

  private val CreateCountActionAndView = """(?U)(?:(count|create):)?(\w*)""".r

  def handle(wabase: Wabase)(req: HttpRequest)(implicit as: ActorSystem): Future[HttpResponse] = {
    val ctx = findRoute(WabaseRequestContext(wabase, req, logger))
    doRoute(ctx)
  }

  protected def findRoute(ctx: WabaseRequestContext): WabaseRequestContext = {
    val pathString = ctx.req.uri.path.toString
    val route = ctx.wabase.qe.routeDefs.find(_.path.pattern.matcher(pathString).matches)
      .getOrElse(error(s"Route not found for path '$pathString'"))
    ctx.copy(route = route)
  }

  protected def doRoute(ctx: WabaseRequestContext)(implicit as: ActorSystem): Future[HttpResponse] = {
    def viewActionKey(ctx: WabaseRequestContext): WabaseRequestContext = {
      import ctx._
      val viewDefs = wabase.qe.nameToViewDef
      val pathString = req.uri.path.toString
      val routeRegex = route.path
      val (viewNameAndActionStr, view_name, create_count_action) = routeRegex.unapplySeq(pathString).collect {
        case vna :: _ =>
          val CreateCountActionAndView(cca, vn) = vna
          if (viewDefs.contains(vn)) (vna, vn, cca)
          else (null, null, null)
      }.getOrElse((null, null, null))

      if (viewNameAndActionStr == null) ctx
      else {
        val key = {
          def key_path(path: Path): Path = path match {
            case Segment(head, tail) =>
              if (head contains viewNameAndActionStr) tail
              else key_path(tail)
            case p => key_path(p.tail)
          }
          val keyPath = key_path(req.uri.path)
          def key(path: Path): List[String] = path match {
            case Segment(v, tail) => v :: key(tail)
            case Empty => Nil
            case p: SlashOrEmpty => key(p.tail)
          }
          key(keyPath)
        }

        val action = if (create_count_action != null) create_count_action else req.method match {
          case `GET`    =>
            if (key.nonEmpty || viewDefs.get(view_name)
              .exists(v => v.apiMethodToRoles.contains("get") && !v.apiMethodToRoles.contains("list")))
              Action.Get
            else
              Action.List
          case `POST`   => Action.Insert
          case `PUT`    => Action.Update
          case `DELETE` => Action.Delete
          case x        => error(s"Unsupported http method $x for request '${req.uri}'")
        }
        ctx.copy(viewName = view_name, action = action, key = key)
      }
    }

    implicit val ec: ExecutionContext = as.dispatcher
    def invokeFunction(className: String, function: String, params: Seq[(Class[_], () => Any)]) = {
      val contextParams = Seq[(Class[_], () => Any)](
        (classOf[ExecutionContext], () => ec),
      )
      org.wabase.invokeFunction(className, function, params ++ contextParams)
    }

    def contextInjectableParameters(wrc: WabaseRequestContext): List[(Class[_], () => Any)] = List(
      (classOf[WabaseRequestContext], () => wrc),
      (classOf[HttpRequest], () => wrc.req),
      (classOf[WabaseUser], () => wrc.user),
      (classOf[ApplicationState], () => wrc.applicationState),
    )

    def invokeReqTransChain(inv: Action.Invocation, wrc: WabaseRequestContext): Future[WabaseRequestContext] = {
      def invokeReqTrans(cn: String, fn: String, tctx: WabaseRequestContext): Future[WabaseRequestContext] = {
        def processResult(r: Any): Future[WabaseRequestContext] = r match {
          case c: WabaseRequestContext => Future.successful(c)
          case req: HttpRequest => processResult(tctx.copy(req = req))
          case st: ApplicationState => processResult(tctx.copy(applicationState = st))
          case u: WabaseUser => processResult(tctx.copy(user = u))
          case f: Future[_] => f.flatMap(processResult)
          case x => error(s"Request transformer must return either WabaseRequestContext or HttpRequest or Future of them." +
            s" Instead got: $x")
        }

        processResult(invokeFunction(cn, fn, contextInjectableParameters(tctx)))
      }
      inv.arg match {
        case null => invokeReqTrans(inv.className, inv.function, wrc)
        case i: Action.Invocation => invokeReqTransChain(i, wrc)
          .flatMap(invokeReqTrans(inv.className, inv.function, _))
        case x => throw new IllegalArgumentException(s"Unrecognized request mapper argument $x, must be function call.")
      }
    }

    def invokeRespTransChain(
      inv: Action.Invocation,
      httpResp: HttpResponse,
      wrc: WabaseRequestContext
    ): Future[HttpResponse] = {
      def invokeRespTrans(cn: String, fn: String, resp: HttpResponse, tctx: WabaseRequestContext): Future[HttpResponse] = {
        def processResult(r: Any): Future[HttpResponse] = r match {
          case resp: HttpResponse => Future.successful(resp)
          case f: Future[_] => f.flatMap(processResult)
          case x => error(s"Response transformer must return either HttpResponse or Future of it." +
            s" Instead got: $x")
        }

        processResult(invokeFunction(cn, fn,
          (classOf[HttpResponse], () => httpResp) :: contextInjectableParameters(tctx)
        ))
      }
      inv.arg match {
        case null => invokeRespTrans(inv.className, inv.function, httpResp, wrc)
        case i: Action.Invocation => invokeRespTransChain(i, httpResp, wrc)
          .flatMap(invokeRespTrans(inv.className, inv.function, _, wrc))
        case x => throw new IllegalArgumentException(s"Unrecognized response transformer argument $x, must be function call.")
      }
    }

    def errorHandler(wrc: WabaseRequestContext): PartialFunction[Throwable, Future[HttpResponse]] = {
      def invokeErrorHandler(
        inv: Action.Invocation,
        throwable: Throwable,
      ): Future[HttpResponse] = {
        def processResult(r: Any): Future[HttpResponse] = r match {
          case r: HttpResponse => Future.successful(r)
          case f: Future[_] => f.flatMap(processResult)
          case x => error(s"Error handler must return Future[HttpResponse], instead got $x. Original error: $throwable")
        }
        processResult(invokeFunction(inv.className, inv.function, Seq(
          (classOf[Throwable], () => throwable),
          (classOf[WabaseRequestContext], () => wrc),
        )))
      }
      val errorHandler = wrc.route.errorHandler
      val pf: PartialFunction[Throwable, Future[HttpResponse]] =
        { case NonFatal(e) if errorHandler != null => invokeErrorHandler(errorHandler, e) }
      pf
    }

    def doRequest(reqCtx: WabaseRequestContext): Future[HttpResponse] = try {
      (if (reqCtx.viewName == null)
        if (reqCtx.route.responseTransformer == null)
          error(s"If view name for route ${reqCtx.route.path} not specified, response transformer must be defined!")
        else invokeRespTransChain(reqCtx.route.responseTransformer, HttpResponse(), reqCtx)
      else {
        val httpResponseF = Future.successful(HttpResponse()) // TODO invoke do wabase action
        if (reqCtx.route.responseTransformer != null)
          httpResponseF.flatMap(invokeRespTransChain(reqCtx.route.responseTransformer, _, reqCtx))
        else httpResponseF
      }).recoverWith(errorHandler(reqCtx))
    } catch {
      case NonFatal(e) => errorHandler(reqCtx)(e) // catch and handle exception if current thread throws exception
    }

    try Option(ctx.route.requestMapper)
      .map(invokeReqTransChain(_, ctx))
      .getOrElse(Future.successful(ctx)).flatMap { mappedCtx =>
        val ctxWithView = if (mappedCtx.viewName == null) viewActionKey(mappedCtx) else mappedCtx
        doRequest(ctxWithView)
      }.recoverWith(errorHandler(ctx)) // recover also here in the case request mapper fails
    catch { case NonFatal(e) => errorHandler(ctx)(e) } // catch if request mapper (invokeReqTransChain) in current thread throws exception
  }

  private def error(msg: String) = throw new WabaseRouteException(msg)
}

object WabaseService {

  type Wabase = WabaseApp[WabaseUser] with QuereaseProvider with I18n

  def optionalHttpHeaderValue[T](req: HttpRequest)(extractorF: HttpHeader => Option[T]): Option[T] = {
    req.headers.collectFirst(Function.unlift(extractorF))
  }

  def optionalHttpHeaderValueByName(req: HttpRequest)(name: String): Option[String] = {
    optionalHttpHeaderValue(req)(optionalHttpHeaderValueExtractor(name.toLowerCase))
  }

  def optionalHttpHeaderValuePF[T](req: HttpRequest)(extractorPF: PartialFunction[HttpHeader, T]): Option[T] = {
    optionalHttpHeaderValue(req)(extractorPF.lift)
  }

  def optionalHttpHeaderValueExtractor(lowerCaseName: String): HttpHeader => Option[String] = {
    case h: HttpHeader if h.is(lowerCaseName) => Some(h.value)
    case _                                    => None
  }

  def optionalCookie(req: HttpRequest)(name: String): Option[String] = {
    optionalHttpHeaderValue(req)({
      case Cookie(cookies) => cookies.find(_.name == name).map(_.value)
      case _               => None
    })
  }

  def setCookie(resp: HttpResponse)(first: HttpCookie, more: HttpCookie*): HttpResponse = {
    resp.mapHeaders(_ ++ (first :: more.toList).map(`Set-Cookie`(_)))
  }

  def deleteCookie(resp: HttpResponse)(name: String, domain: String = "", path: String = ""): HttpResponse = {
    val cookie = HttpCookie(name, "",
      domain = Option(domain).filter(_.nonEmpty), path = Option(path).filter(_.nonEmpty))
    resp.mapHeaders(_ ++ Seq(`Set-Cookie`(cookie.withValue("deleted").withExpires(DateTime.MinValue))))
  }

  def pathSegments(path: Path): List[String] = path match {
    case Path.Empty => Nil
    case _: Path.Slash => pathSegments(path.tail)
    case Path.Segment(h, t) => h :: pathSegments(t)
  }

  def complete(ctx: WabaseRequestContext, marshallable: => ToResponseMarshallable): Future[HttpResponse] =
    marshallable(ctx.req)(ctx.as.dispatcher)

}

object ApplicationStateExtractor {
  def extractState(ctx: WabaseRequestContext): ApplicationState =
    extractState(ctx, AppServiceBase.ApplicationStateCookiePrefix)
  def extractState(ctx: WabaseRequestContext, prefix: String): ApplicationState = {
    val state = ctx.req.headers.flatMap {
      case c: Cookie => c.cookies.filter(_.name.startsWith(prefix))
      case _ => Nil
    }.map { c => c.name ->
      AppServiceBase.decodeParam(
        ctx.wabase.qe.metadataConventions,
        AppServiceBase.NamesForInts,
        AppServiceBase.escapeReflectedXss)(c.name, c.value)
    }.toMap
    val langKey = prefix + I18nService.ApplicationLanguageCookiePostfix
    if (state.contains(langKey))
      ApplicationState(state, new Locale(String.valueOf(state(langKey))))
    else
      I18nService.currentLangFromHeader(ctx.req)
        .map(l => ApplicationState(state + (langKey -> l), new Locale(l)))
        .getOrElse(ApplicationState(state))
  }
}
