package org.wabase

import org.apache.pekko.http.scaladsl.model.HttpMethods._
import org.apache.pekko.http.scaladsl.model.Uri.Path
import org.apache.pekko.http.scaladsl.model.Uri.Path.{Empty, Segment, SlashOrEmpty}
import AppMetadata._
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.marshalling.{Marshal, ToResponseMarshallable}
import org.apache.pekko.http.scaladsl.model.headers.{Cookie, HttpCookie, `Set-Cookie`}
import org.apache.pekko.http.scaladsl.model.{ContentType, ContentTypes, DateTime, HttpHeader, HttpRequest, HttpResponse, StatusCodes, Uri}
import org.apache.pekko.http.scaladsl.unmarshalling.PredefinedFromEntityUnmarshallers
import org.wabase.AppMetadata.{Action, RouteDef}
import org.wabase.WabaseService.Wabase

import java.util.Locale
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

case class WabaseUser(properties: Map[String, Any]) {
  val id: Long      = properties.get("id").collect { case x: Number => x.longValue }.getOrElse(-1)
  val name: String  = properties.get("name").map(String.valueOf)
    .orElse(Option(id).filter(_ != -1).map(_.toString)).orNull
  val roles: Set[String] = properties.get("roles")
    .collect { case r: Iterable[String@unchecked] => r.toSet }.getOrElse(Set())
}

case class WabaseRequestContext(
  wabase: Wabase,
  req: HttpRequest,
  deferred: Deferred = null,
  route: RouteDef = null,
  viewName: String = null,
  action: String = null,
  key: Seq[Any] = Nil,
  applicationState: ApplicationState = null,
  user: WabaseUser = null,
  queryTimeout: QueryTimeout = null,
  as: ActorSystem = null,
)

case class Deferred(
  deferredControl: WabaseDeferredControl = null,
  deferredModule: String = WabaseDeferredControl.defaultModuleId
)

class WabaseRouteException(message: String) extends Exception(message)

class WabaseService extends Loggable {
  import WabaseService._

  def handle(
    wabase: Wabase,
    deferredControl: WabaseDeferredControl,
  )(req: HttpRequest)(
    implicit as: ActorSystem): Future[HttpResponse] = {
    val ctx = WabaseRequestContext(wabase, req, Deferred(deferredControl = deferredControl))
    findRoute(ctx).map(doRoute).getOrElse(Future.successful(HttpResponse(status = StatusCodes.NotFound)))
  }

  protected def findRoute(ctx: WabaseRequestContext): Option[WabaseRequestContext] = {
    val pathString = ctx.req.uri.path.toString
    ctx.wabase.qe.routeDefs.find(_.path.pattern.matcher(pathString).matches)
      .map(r => ctx.copy(route = r))
  }

  def doRoute(ctx: WabaseRequestContext)(implicit as: ActorSystem): Future[HttpResponse] = {
    implicit val ec: ExecutionContext = as.dispatcher

    def invokeHandlerBuilderChain(inv: Action.Invocation, innerHandler: RequestHandler): RequestHandler = {
      def buildHandler(cn: String, fn: String, ih: RequestHandler): RequestHandler = wrc => {
        def missingHandlerError = sys.error(s"Handler argument missing for invocation: '$cn.$fn'")
        def invokeHandlerBuilder = {
          def processResult(r: Any): Future[Any] = r match {
            case c: WabaseRequestContext => Future.successful(c)
            case req: HttpRequest => processResult(wrc.copy(req = req))
            case uri: Uri => processResult(wrc.copy(req = wrc.req.withUri(uri)))
            case st: ApplicationState => processResult(wrc.copy(applicationState = st))
            case u: WabaseUser => processResult(wrc.copy(user = u))
            case resp: HttpResponse => Future.successful(resp)
            case f: Future[_] => f.flatMap(processResult)
            case rh: RequestHandler@unchecked => Future.successful(rh)
            case x => error(s"Request transformer must return either WabaseRequestContext or HttpRequest or Future of them." +
              s" Instead got: $x")
          }
          processResult(org.wabase.invokeFunction(cn, fn, List(
            (classOf[Uri], () => wrc.req.uri),
            (classOf[WabaseRequestContext], () => wrc),
            (classOf[HttpRequest], () => wrc.req),
            (classOf[WabaseUser], () => wrc.user),
            (classOf[ApplicationState], () => wrc.applicationState),
            (classOf[HttpResponse], () => if (ih == null) missingHandlerError else ih(wrc)),
            (classOf[Future[HttpResponse]], () => if (ih == null) missingHandlerError else ih(wrc)),
            (classOf[RequestHandler], () => ih),
            (classOf[ActorSystem], () => as),
            (classOf[ExecutionContext], () => ec),
          )))
        }

        invokeHandlerBuilder.flatMap {
          case c: WabaseRequestContext => if (ih == null) missingHandlerError else ih(c)
          case r: HttpResponse => Future.successful(r)
          case h: RequestHandler@unchecked => h(wrc)
        }
      }
      inv.arg match {
        case null => buildHandler(inv.className, inv.function, innerHandler)
        case i: Action.Invocation => buildHandler(inv.className, inv.function,
          invokeHandlerBuilderChain(i, innerHandler))
        case x => throw new IllegalArgumentException(s"Unrecognized request handler argument '$x', must be function call.")
      }
    }

    def errorHandler(wrc: WabaseRequestContext): PartialFunction[Throwable, Future[HttpResponse]] = {
      wrc.route.errorHandler.errorHandler(wrc).orElse {
        case NonFatal(e) =>
          logger.error("Internal server error, sending http 500", e)
          Future.successful(HttpResponse(status = StatusCodes.InternalServerError))
      }
    }

    try {
      val handler = invokeHandlerBuilderChain(ctx.route.requestHandler, null)
      handler(ctx).recoverWith(errorHandler(ctx))
    } catch { case NonFatal(e) => errorHandler(ctx)(e) }
  }
}

object WabaseService {

  type RequestHandler = WabaseRequestContext => Future[HttpResponse]
  type Wabase = WabaseApp[WabaseUser] with QuereaseProvider with I18n with DbAccess with Marshalling with AppProvider[WabaseUser]

  val CreateCountActionAndViewRegex = """(?U)(?:(count|create):)?(\w*)""".r
  val WabaseUserAttributeName = "wabase-user"

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

  def parameterMultiMap(req: HttpRequest): Map[String, List[String]] = req.uri.query().toMultiMap

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

  /** Utility function. Extracts Content-Type from list of headers since pekko renders content type from http entity not from header list */
  def partitionHeaders(headers: List[HttpHeader]): (Option[ContentType], List[HttpHeader]) = {
    headers.partition(_.is("content-type")) match {
      case (cts, h) => cts.map(cth => ContentType.parse(cth.value)).collectFirst {
        case Right(ct) => ct
        case Left(errs) => throw new IllegalArgumentException(s"Error(s) parsing content type:\n${
          errs.map(_.formatPretty).mkString("\n")
        }")
      } -> h
    }
  }

  def viewActionKey(ctx: WabaseRequestContext): WabaseRequestContext = {
    import ctx._
    val viewDefs = wabase.qe.nameToViewDef
    val pathString = req.uri.path.toString
    val routeRegex = route.path
    val (viewNameAndActionStr, view_name, create_count_action) = routeRegex.unapplySeq(pathString).collect {
      case vna :: _ =>
        val CreateCountActionAndViewRegex(cca, vn) = vna
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

  def doAction(reqCtx: WabaseRequestContext): Future[HttpResponse] = {
    def dwa(ctx: WabaseRequestContext) = {
      import ctx._
      if (viewName == null || !wabase.qe.nameToViewDef.contains(viewName))
        if (viewName != null)
          error(s"View '${viewName}' for route ${route.path} not found. Response transformer must be defined!")
        else error(s"If view name for route ${route.path} not specified, response transformer must be defined!")
      else {
        val params = AppServiceBase.filterParams(
          wabase.qe.metadataConventions, AppServiceBase.NamesForInts, AppServiceBase.escapeReflectedXss
        )(WabaseService.parameterMultiMap(req))
        implicit val ec = as.dispatcher
        val valuesF =
          if (Set(Action.Insert, Action.Update, Action.Save).contains(action))
            toMapEntityDecoder(ctx)
          else  Future.successful(Map[String, Any]())
        valuesF.flatMap { values =>
          ctx.wabase.app.doWabaseAction(
            actionName = action,
            viewName = viewName,
            keyValues = key,
            params = params,
            values = values,
          )(user, applicationState, ec, as, req)
        }.flatMap { result =>
          Marshal(result).toResponseFor(req)(wabase.toResponseWabaseResultMarshaller, ec)
        }
      }
    }
    val ctxWithView = if (reqCtx.viewName == null) viewActionKey(reqCtx) else reqCtx
    val ctxWithViewAndState =
      if (ctxWithView.applicationState == null)
        ctxWithView.copy(applicationState = ApplicationStateExtractor.extractState(ctxWithView))
      else ctxWithView
    dwa(ctxWithViewAndState)
  }

  def toMapEntityDecoder(ctx: WabaseRequestContext): Future[Map[String, Any]] = {
    import ctx._
    implicit val mat = as
    implicit val ec = as.dispatcher
    val vd = wabase.qe.viewDef(viewName)
    vd.decoder match {
      case AppMetadata.DefaultDecoder =>
        def defaultContent = wabase.toMapUnmarshallerForView(viewName)(req.entity)
        req.entity.contentType match {
          case ContentTypes.`application/json` => defaultContent
          case ContentTypes.`application/x-www-form-urlencoded` =>
            PredefinedFromEntityUnmarshallers.defaultUrlEncodedFormDataUnmarshaller(req.entity)
              .map(fd => wabase.qe.toCompatibleMap(fd.fields.toMap, vd))
          case _ => defaultContent
        }
      case AppMetadata.CustomDecoder(o, f) =>
        invokeFunction(o, f,
          Seq[(Class[_], () => Any)](
            (classOf[HttpRequest], () => req),
            (classOf[ActorSystem], () => as),
            (classOf[ExecutionContext], () => ec)
          )
        ) match {
          case f: Future[_] => f.mapTo[Map[String, Any]]
          case m: Map[String, Any]@unchecked => Future.successful(m)
          case x => throw new IllegalArgumentException(s"Custom decoder must return Map[String, Any], instead got: $x")
        }
      case AppMetadata.NoneDecoder => Future.successful(Map())
    }
  }

  def error(msg: String) = throw new WabaseRouteException(msg)
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
