package org.wabase

import org.apache.pekko.http.scaladsl.model.HttpMethods._
import org.apache.pekko.http.scaladsl.model.Uri.Path
import org.apache.pekko.http.scaladsl.model.Uri.Path.{Empty, Segment, SlashOrEmpty}
import AppMetadata._
import com.typesafe.scalalogging.Logger
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.marshalling.{Marshal, ToResponseMarshallable}
import org.apache.pekko.http.scaladsl.model.headers.{Cookie, HttpCookie, `Set-Cookie`}
import org.apache.pekko.http.scaladsl.model.{ContentType, ContentTypes, DateTime, HttpEntity, HttpHeader, HttpMessage, HttpRequest, HttpResponse, MediaTypes, StatusCodes, Uri}
import org.apache.pekko.http.scaladsl.server.directives.ContentTypeResolver
import org.apache.pekko.http.scaladsl.server.directives.FileAndResourceDirectives.ResourceFile
import org.apache.pekko.http.scaladsl.unmarshalling.Unmarshaller
import org.apache.pekko.stream.scaladsl.StreamConverters
import org.apache.pekko.util.ByteString
import org.mojoz.metadata.ViewDef
import org.slf4j.LoggerFactory
import org.wabase.AppMetadata.{Action, RouteDef}
import org.wabase.WabaseService.Wabase

import java.lang.reflect.Parameter
import java.util.Locale
import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
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
  logger: Logger = null,
  resultFilter: ResultRenderer.ResultFilter = null,
) {
  def withResultFilter(resFil: ResultRenderer.ResultFilter): WabaseRequestContext =
    copy(resultFilter = resFil)
}

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
    val loggerName = req.method.value.toLowerCase + WabaseService.toReadableString(req.uri.path).replace('/', '.')
    val logger = Logger(LoggerFactory.getLogger(loggerName))
    val ctx = WabaseRequestContext(wabase, req, Deferred(deferredControl = deferredControl), as = as, logger = logger)
    findRoute(ctx).map(doRoute).getOrElse(WabaseService.notFound)
  }

  protected def findRoute(ctx: WabaseRequestContext): Option[WabaseRequestContext] = {
    val pathString = WabaseService.toReadableString(ctx.req.uri.path)
    ctx.wabase.qe.routeDefs
      .find { rd =>
        rd.path.pattern.matcher(pathString).matches && (rd.methods.isEmpty || rd.methods(ctx.req.method))
      }
      .map { r =>
        ctx.logger.debug(s"Route '${r.methods.map(_.value + " ").mkString}${r.path}' matched for request '${
          ctx.req.method.value} ${ctx.req.uri}'")
        ctx.copy(route = r)
      }
  }

  def doRoute(ctx: WabaseRequestContext)(implicit as: ActorSystem): Future[HttpResponse] = {
    implicit val ec: ExecutionContext = as.dispatcher

    def invokeHandlerBuilderChain(inv: Action.Invocation, innerHandler: RequestHandler): RequestHandler = {
      inv.arg match {
        case null => buildRequestHandler(inv.className, inv.function, innerHandler)
        case i: Action.Invocation => buildRequestHandler(inv.className, inv.function,
          invokeHandlerBuilderChain(i, innerHandler))
        case x => throw new IllegalArgumentException(s"Unrecognized request handler argument '$x', must be function call.")
      }
    }

    def errorHandler(wrc: WabaseRequestContext): WabaseService.ErrorHandler = {
      val eh = wrc.route.errorHandler
      invokeFunction(eh.className, eh.function, Seq((classOf[WabaseRequestContext], () => wrc))) match {
        case h: PartialFunction[Throwable@unchecked, Future[HttpResponse]@unchecked] =>
          h.orElse {
            case NonFatal(e) =>
              logger.error("Internal server error, sending http 500", e)
              Future.successful(HttpResponse(status = StatusCodes.InternalServerError))
          }
        case x => sys.error(s"Error handler for route ${wrc.route.path} must return value of type:" +
          s" WabaseService.ErrorHandler, instead got '$x' of type '${x.getClass}'")
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
  type ErrorHandler   = PartialFunction[Throwable, Future[HttpResponse]]
  type Wabase = WabaseApp[WabaseUser] with QuereaseProvider with I18n with DbAccess with Marshalling with AppProvider[WabaseUser] with Execution

  val CreateCountActionAndViewRegex = """(?U)(?:(count|create):)?([_\p{IsLatin}][\-\w]*)""".r
  val WabaseUserAttributeName = "wabase-user"

  val notFound: Future[HttpResponse] = Future.successful(HttpResponse(status = StatusCodes.NotFound))

  def optionalHttpHeaderValue[T](msg: HttpMessage)(extractorF: HttpHeader => Option[T]): Option[T] = {
    msg.headers.collectFirst(Function.unlift(extractorF))
  }

  def optionalHttpHeaderValueByName(msg: HttpMessage)(name: String): Option[String] = {
    optionalHttpHeaderValue(msg)(optionalHttpHeaderValueExtractor(name.toLowerCase))
  }

  def optionalHttpHeaderValuePF[T](msg: HttpMessage)(extractorPF: PartialFunction[HttpHeader, T]): Option[T] = {
    optionalHttpHeaderValue(msg)(extractorPF.lift)
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

  def doRequest(ctx: WabaseRequestContext): Future[HttpResponse] = {
    val pathString = toReadableString(ctx.req.uri.path)
    ctx.route.path.unapplySeq(pathString).map {
      case handlerName :: _ =>
        val (cn, fn) = OpParser.classNameFunctionName(handlerName)
        val key = WabaseService.key(ctx.req.uri.path, handlerName)
        buildRequestHandler(cn, fn, null)(ctx.copy(key = key))
      case _ => notFound
    }.getOrElse(notFound)
  }

  def api(ctx: WabaseRequestContext): HttpResponse = {
    val json = ctx.wabase._api(ctx.user)
    HttpResponse(entity = HttpEntity.Strict(ContentTypes.`application/json`, ByteString(json.compactPrint)))
  }

  def metadata(ctx: WabaseRequestContext): HttpResponse = {
    // TODO ETag for metadata for new flow
    // respondWithHeader(ETag(EntityTag(app.metadataVersionString))) {
    //   conditional(EntityTag(app.metadataVersionString), DateTime.now) {
    implicit val user:  WabaseUser       = ctx.user
    implicit val state: ApplicationState = ctx.applicationState
    import ctx._
    val pathString = toReadableString(req.uri.path)
    val routeRegex = route.path
    val viewName   = routeRegex.unapplySeq(pathString).flatMap(_.headOption).orNull
    val json = if (viewName == "*") wabase._apiMetadata else wabase._metadata(viewName)
    HttpResponse(entity = HttpEntity.Strict(ContentTypes.`application/json`, ByteString(json.compactPrint)))
  }

  private val webResourcesPath = config.getString("app.web-resources-path")
  private val classLoader = this.getClass.getClassLoader
  def getFromResource(ctx: WabaseRequestContext): HttpResponse = {
    val resourceName = s"${webResourcesPath}${ctx.req.uri.path}"
    val contentType = ContentTypeResolver.Default(resourceName)
    if (!resourceName.endsWith("/"))
        Option(classLoader.getResource(resourceName)).flatMap(ResourceFile.apply) match {
          case Some(ResourceFile(url, length, lastModified)) =>
            // TODO conditionalFor(length, lastModified) {
              if (length > 0) {
                // TODO withRangeSupportAndPrecompressedMediaTypeSupport {
                  HttpResponse(entity =
                    HttpEntity.Default(contentType, length,
                      StreamConverters.fromInputStream(() => url.openStream()))
                // }
                  )
              } else HttpResponse(entity = HttpEntity.Empty)
            // }
          case _ => HttpResponse(StatusCodes.NotFound) // not found or directory
        }
    else HttpResponse(StatusCodes.NotFound)
  }

  /** Extract segments as list from path after segment matching prefix */
  def key(path: Path, prefix: String): Seq[String] = {
    def key_path(path: Path): Path = path match {
      case Segment(head, tail) =>
        if (head contains prefix) tail
        else key_path(tail)
      case p => key_path(p.tail)
    }
    val keyPath = key_path(path)
    def key(path: Path): List[String] = path match {
      case Segment(v, tail) => v :: key(tail)
      case Empty => Nil
      case p: SlashOrEmpty => key(p.tail)
    }
    key(keyPath)
  }

  /** Enables alternative URI where row key is in special query string */
  def keyFromQueryToPath(request: HttpRequest): HttpRequest = {
    def decode(s: String) = java.net.URLDecoder.decode(s, "UTF-8")
    request.uri.rawQueryString match {
      case Some(rawQ) if rawQ startsWith "/" =>
        val (p, q) = rawQ.indexOf('?') match {
          case -1 => (rawQ, null)
          case i  => (rawQ.substring(0, i), rawQ.substring(i + 1))
        }
        @annotation.tailrec
        def toPath(path: Uri.Path, p: String): Uri.Path = p.indexOf('/', 1) match {
          case -1 => path / decode(p.substring(1))
          case i  => toPath(path / decode(p.substring(1, i)), p.substring(i))
        }
        val uriWithPath   = request.uri.withPath(toPath(request.uri.path, p))
        val uri =
          if (q == null)
               uriWithPath.withQuery(Uri.Query.Empty)
          else uriWithPath.withRawQueryString(q)
        request.withUri(uri)
      case _ => request
    }
  }

  def viewActionKey(ctx: WabaseRequestContext): WabaseRequestContext = {
    import ctx._
    val viewDefs = wabase.qe.nameToViewDef
    val pathString = toReadableString(req.uri.path)
    val routeRegex = route.path
    val (viewNameAndActionStr, view_name, create_count_action) = routeRegex.unapplySeq(pathString).collect {
      case vna :: _ =>
       try {
        val CreateCountActionAndViewRegex(cca, vn) = vna
        if (viewDefs.contains(vn)) (vna, vn, cca)
        else (null, null, null)
       } catch {
        case ex: scala.MatchError =>
          throw new RuntimeException(s"Unsupported view_name: $vna", ex)
       }
    }.getOrElse((null, null, null))

    if (viewNameAndActionStr == null) ctx
    else {
      val key = WabaseService.key(req.uri.path, viewNameAndActionStr)
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

  private val fieldFilterParameterNameOpt =
    Option("app.field-filter-parameter-name").filter(config.hasPath).map(config.getString)

  def addResultFilter(context: WabaseRequestContext, params: Map[String, Any]): WabaseRequestContext = {
    if (context.resultFilter != null) context
    else context.action match {
      case Action.Get | Action.List | Action.Create =>
        val allowed = fieldFilterParameterNameOpt.flatMap(params.get).map {
          case null => null
          case seq: Seq[_] => seq.map(_.toString).toSet
          case cols => s"$cols".split(",").map(_.trim).toSet
        }.orNull
        context.logger.debug(s"Adding result filter. allowed: ${allowed}")
        if (allowed != null) {
          class ColsFilter(viewName: String, nameToViewDef: Map[String, ViewDef])
            extends ResultRenderer.ViewFieldFilter(viewName, nameToViewDef) {
            override def shouldInclude(field: String) =
              allowed.contains(field) && super.shouldInclude(field)
            override def childFilter(field: String) = viewDef.fieldOpt(field)
              .map(_.type_.name)
              .map(new ColsFilter(_, nameToViewDef))
              .orNull
          }
          context.withResultFilter(new ColsFilter(context.viewName, context.wabase.qe.nameToViewDef))
        } else context
      case _ => context
    }
  }

  def doAction(reqCtx: WabaseRequestContext): Future[HttpResponse] = {
    def extractParams(ctx: WabaseRequestContext) = {
      import ctx._
      AppServiceBase.filterParams(
        wabase.qe.metadataConventions, AppServiceBase.NamesForInts, AppServiceBase.escapeReflectedXss
      )(WabaseService.parameterMultiMap(req))
    }
    def dwa(ctx: WabaseRequestContext, params: Map[String, Any]) = {
      import ctx._
      if (viewName == null || !wabase.qe.nameToViewDef.contains(viewName))
        if (viewName != null)
          error(s"Cannot handle route ${route.path}. View '$viewName' not found!")
        else error(s"Cannot handle route: ${route.path}. View not found!")
      else {
        implicit val ec = as.dispatcher
        val valuesF =
          if (Set(Action.Insert, Action.Update, Action.Save).contains(action))
            toMapForViewEntityDecoder(ctx)
          else  Future.successful(Map[String, Any]())
        valuesF.flatMap { values =>
          ctx.wabase.app.doAction(
            actionName = action,
            viewName = viewName,
            keyValues = ctx.key,
            params = params,
            values = values,
            resultFilter = resultFilter,
          )(ctx)
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
    val params = extractParams(ctxWithViewAndState)
    val ctxWithViewAndStateAndFilter =
      if (ctxWithViewAndState.resultFilter == null)
        addResultFilter(ctxWithViewAndState, params)
      else ctxWithViewAndState
    dwa(ctxWithViewAndStateAndFilter, params)
  }

  def toMapForViewEntityDecoder(ctx: WabaseRequestContext): Future[Map[String, Any]] = {
    import ctx._
    implicit val mat = as
    implicit val ec = as.dispatcher
    val vd = wabase.qe.viewDef(viewName)
    vd.decoder match {
      case AppMetadata.DefaultDecoder =>
        def defaultContent = wabase.toMapUnmarshallerForView(viewName)(req.entity)
        def mappedContent  = wabase.toMapUnmarshaller(req.entity).map(m => wabase.qe.toCompatibleMap(m, vd))
        req.entity.contentType match {
          case ContentTypes.`application/json` => defaultContent
          case ContentTypes.`application/x-www-form-urlencoded` =>
            mappedContent
          case multipartFormData if WabaseUnmarshallers.isMultipartFormData(multipartFormData.mediaType) =>
            mappedContent
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

  def toStringEntityDecoder(ctx: WabaseRequestContext): Future[String] = {
    import ctx._
    implicit val mat = as
    implicit val ec = as.dispatcher
    Unmarshaller.stringUnmarshaller(req.entity)
  }

  def toMapEntityDecoder(ctx: WabaseRequestContext): Future[Map[String, Any]] =
    toAnyEntityDecoder(ctx).mapTo[Map[String, Any]]

  def toSeqEntityDecoder(ctx: WabaseRequestContext): Future[Seq[Any]] =
    toAnyEntityDecoder(ctx).mapTo[Seq[Any]]

  private def toAnyEntityDecoder(ctx: WabaseRequestContext): Future[Any] = {
    import ctx._
    implicit val mat = as
    implicit val ec = as.dispatcher
    def decodeJs = Unmarshaller.byteStringUnmarshaller
      .map { b => CborOrJsonAnyValueDecoder.decode(b) }
    req.entity.contentType match {
      case ContentTypes.`application/json` => decodeJs(req.entity)
      case ContentTypes.`application/x-www-form-urlencoded` =>
        wabase.toMapUnmarshaller(req.entity)
      case multipartFormData if WabaseUnmarshallers.isMultipartFormData(multipartFormData.mediaType) =>
        wabase.toMapUnmarshaller(req.entity)
      case _ => decodeJs(req.entity)
    }
  }

  def toReadableString(path: Path): String = {
    @tailrec def trs(p: Path, sb: StringBuilder): String = p match {
      case Path.Empty => sb.toString
      case _: Path => trs(p.tail, sb.append(p.head))
    }
    trs(path, new StringBuilder())
  }

  def buildRequestHandler(cn: String, fn: String, ih: RequestHandler): RequestHandler = wrc => {
    wrc.logger.debug(s"Invoking handler $cn.$fn for request: ${wrc.req}")
    implicit val ec: ExecutionContext = wrc.as.dispatcher
    def missingHandlerError = sys.error(s"Handler argument missing for invocation: '$cn.$fn'")
    val (paramList, paramFunction) = handlerParameters(wrc, ih, missingHandlerError)
    val result = org.wabase.invokeFunction(cn, fn, paramList, paramFunction)
    handlerResult(wrc, result).flatMap {
      case c: WabaseRequestContext => if (ih == null) missingHandlerError else ih(c)
      case r: HttpResponse => Future.successful(r)
      case h: RequestHandler@unchecked => h(wrc)
    }
  }

  def handlerParameters(
    wrc: WabaseRequestContext,
    innerHandler: RequestHandler,
    missingHandlerError: => Nothing,
  ): (Seq[(Class[_], () => Any)], PartialFunction[Parameter, Any]) = {
    implicit val ec: ExecutionContext = wrc.as.dispatcher
    val paramList = List(
      (classOf[WabaseRequestContext], () => wrc),
      (classOf[HttpRequest], () => wrc.req),
      (classOf[HttpResponse], () => if (innerHandler == null) missingHandlerError else innerHandler(wrc)),
      (classOf[Future[HttpResponse]], () => if (innerHandler == null) missingHandlerError else innerHandler(wrc)),
      (classOf[ActorSystem], () => wrc.as),
      (classOf[ExecutionContext], () => ec),
      (classOf[RequestHandler], () => innerHandler),
      (classOf[WabaseUser], () => wrc.user),
      (classOf[ApplicationState], () => wrc.applicationState),
      (classOf[Uri], () => wrc.req.uri),
      (classOf[Map[String, Any]], () => toMapEntityDecoder(wrc)), // map is function so it comes after request handler
      (classOf[Seq[Any]], () => toSeqEntityDecoder(wrc)), // seq is function so it comes after request handler
      (classOf[java.util.Map[_, _]], () => toMapEntityDecoder(wrc).map(_.asJava)),
      (classOf[java.util.List[_]], () => toSeqEntityDecoder(wrc).map(_.asJava)),
      (classOf[String], () => toStringEntityDecoder(wrc)),
    )
    val paramFunction = AppQuerease.dtoParameterFromMapF(() => toMapEntityDecoder(wrc))(wrc.wabase.qio)
    (paramList, paramFunction)
  }

  def handlerResult(wrc: WabaseRequestContext, res: Any): Future[Any] = {
    implicit val ec: ExecutionContext = wrc.as.dispatcher
    def processResult(r: Any): Future[Any] = r match {
      case c: WabaseRequestContext => Future.successful(c)
      case req: HttpRequest => processResult(wrc.copy(req = req))
      case resp: HttpResponse => Future.successful(resp)
      case f: Future[_] => f.flatMap(processResult)
      case s: String => Future.successful(HttpResponse(entity = s))
      case _: Map[_, _] | _: scala.collection.mutable.Map[_, _] => Future.successful(jsonResponse(r))
      case s: Iterable[_] => Future.successful(jsonResponse(s.map { case e: Dto => e.toMap(wrc.wabase.qe) case x => x }))
      case rh: RequestHandler@unchecked => Future.successful(rh)
      case d: Dto => Future.successful(jsonResponse(d.toMap(wrc.wabase.qe)))
      case m: java.util.Map[_, _] => processResult(m.asScala)
      case s: java.util.List[_] => processResult(s.asScala)
      case uri: Uri => processResult(wrc.copy(req = wrc.req.withUri(uri)))
      case st: ApplicationState => processResult(wrc.copy(applicationState = st))
      case u: WabaseUser => processResult(wrc.copy(user = u))
      case x => error(s"Request transformer must return either WabaseRequestContext or HttpRequest or Future of them." +
        s" Instead got: $x")
    }
    processResult(res)
  }

  def jsonResponse(resp: Any): HttpResponse =
    HttpResponse(entity = HttpEntity(MediaTypes.`application/json`, ResultEncoder.encodeAnyToJsonString(resp)))

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
