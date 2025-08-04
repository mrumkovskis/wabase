package org.wabase

import org.apache.pekko.http.scaladsl.model.HttpMethods._
import org.apache.pekko.http.scaladsl.model.Uri.Path
import org.apache.pekko.http.scaladsl.model.Uri.Path.{Empty, Segment, SlashOrEmpty}
import AppMetadata._
import com.typesafe.scalalogging.Logger
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.marshalling.{Marshal, ToResponseMarshallable}
import org.apache.pekko.http.scaladsl.model.headers.{Cookie, EntityTag, HttpCookie, `Set-Cookie`, `Timeout-Access`}
import org.apache.pekko.http.scaladsl.model.{ContentType, ContentTypes, DateTime, HttpEntity, HttpHeader, HttpMessage, HttpRequest, HttpResponse, MediaTypes, StatusCode, StatusCodes, Uri}
import org.apache.pekko.http.scaladsl.server.directives.ContentTypeResolver
import org.apache.pekko.http.scaladsl.server.directives.FileAndResourceDirectives.ResourceFile
import org.apache.pekko.http.scaladsl.unmarshalling.Unmarshaller
import org.apache.pekko.stream.scaladsl.StreamConverters
import org.apache.pekko.util.ByteString
import org.mojoz.metadata.ViewDef
import org.slf4j.LoggerFactory
import org.tresql.parsing.QueryParsers
import org.wabase.AppMetadata.{Action, RouteDef}
import org.wabase.WabaseService.Wabase
import org.wabase.CacheConditionHandlers._

import java.util.Locale
import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal
import scala.util.parsing.input.CharSequenceReader

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
  resultFilter: ResultRenderer.ResultFilter = null,
  logger: Logger = null,
) {
  def withResultFilter(resFil: ResultRenderer.ResultFilter): WabaseRequestContext =
    copy(resultFilter = resFil)
}

case class Deferred(
  deferredControl: WabaseDeferredControl = null,
  deferredModule: String = WabaseDeferredControl.defaultModuleId
)

class HttpException(val status: StatusCode, message: String) extends Exception(message)

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
    ctx.logger.debug(s"Matching route for path: ${req.uri.path}")
    findRoute(ctx).map(doRoute).getOrElse {
      ctx.logger.debug(s"Route not found for path: ${req.uri.path}")
      WabaseService.notFound
    }
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
      inv.args match {
        case inv_args => inv_args.splitAt(inv_args.size - 1) match {
          case (args, List(innerInv: Action.Invocation)) =>
            buildRequestHandler(inv.className, inv.function, HandlerArgsParser.argValues(args),
              invokeHandlerBuilderChain(innerInv, innerHandler))
          case _ =>
            buildRequestHandler(inv.className, inv.function, HandlerArgsParser.argValues(inv_args), innerHandler)
        }
      }
    }

    def errorHandler(wrc: WabaseRequestContext): WabaseService.ErrorHandler =
      WabaseService.errorHandler(wrc) orElse ({ case NonFatal(e) =>
        wrc.logger.error(s"[${WabaseErrorHandler.ctxDebugInfo(wrc)}] Internal server error, sending http 500", e)
        Future.successful(HttpResponse(status = StatusCodes.InternalServerError))
    }: WabaseService.ErrorHandler)

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

  val okResponse: HttpResponse = HttpResponse(StatusCodes.OK)
  def statusResponse(statusCode: Int): HttpResponse = HttpResponse(statusCode)
  def statusAndTextResponse(statusCode: Int, text: String): HttpResponse = HttpResponse(statusCode, entity = ByteString(text))

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
    resp.mapHeaders(_ ++ Seq(`Set-Cookie`(cookie.withValue("").withExpires(DateTime.MinValue))))
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

  def doRequest(handlerName: String, ctx: WabaseRequestContext): Future[HttpResponse] = {
    val (cn, fn) = OpParser.classNameFunctionName(handlerName)
    val key = WabaseService.key(ctx.req.uri.path, handlerName)
    buildRequestHandler(cn, fn, Nil, null)(ctx.copy(key = key))
  }

  def pathMatchedGroups(ctx: WabaseRequestContext): Option[List[String]] = {
    ctx.route.path.unapplySeq(toReadableString(ctx.req.uri.path))
  }

  def api(ctx: WabaseRequestContext): HttpResponse = {
    val json = ctx.wabase._api(ctx.user)
    HttpResponse(entity = HttpEntity.Strict(ContentTypes.`application/json`, ByteString(json.compactPrint)))
  }

  def metadata(viewName: String, ctx: WabaseRequestContext): RequestHandler = {
    conditional(EntityTag(ctx.wabase.app.metadataVersionString), DateTime(ctx.wabase.app.startupTimeMillis), _ => {
      implicit val user:  WabaseUser       = ctx.user
      implicit val state: ApplicationState = ctx.applicationState
      import ctx.wabase
      val json = if (viewName == "*") wabase._apiMetadata else wabase._metadata(viewName)
      Future.successful(
        HttpResponse(entity = HttpEntity.Strict(ContentTypes.`application/json`, ByteString(json.compactPrint)))
      )
    })
  }

  private def conditionalFor(length: Long, lastModified: Long, innerHandler: RequestHandler): RequestHandler = {
    // extractSettings.flatMap(settings =>
      // if (settings.fileGetConditional) {
        val tag = java.lang.Long.toHexString(lastModified ^ java.lang.Long.reverse(length))
        val lastModifiedDateTime = DateTime(math.min(lastModified, System.currentTimeMillis))
        conditional(EntityTag(tag), lastModifiedDateTime, innerHandler)
      // } else pass)
  }
  private val classLoader = this.getClass.getClassLoader
  def getFromResource(resourcesRootPath: String, resourcePathAndName: String): RequestHandler = {
    val resourceName = s"${resourcesRootPath}${resourcePathAndName}"
    val contentType = ContentTypeResolver.Default(resourceName)
    if (!resourceName.endsWith("/"))
        Option(classLoader.getResource(resourceName)).flatMap(ResourceFile.apply) match {
          case Some(ResourceFile(url, length, lastModified)) =>
            conditionalFor(length, lastModified, _ => {
              if (length > 0) {
                // TODO withRangeSupportAndPrecompressedMediaTypeSupport {
                Future.successful(
                  HttpResponse(entity =
                    HttpEntity.Default(contentType, length,
                      StreamConverters.fromInputStream(() => url.openStream()))
                  )
                )
              } else Future.successful(HttpResponse(entity = HttpEntity.Empty))
            })
          case _ => (_: WabaseRequestContext) => Future.successful(HttpResponse(StatusCodes.NotFound)) // not found or directory
        }
    else (_: WabaseRequestContext) => Future.successful(HttpResponse(StatusCodes.NotFound))
  }

  /** Extract segments as list from path after segment matching prefix */
  def key(path: Path, prefix: String): Seq[String] = {
    def key_path(path: Path): Path = path match {
      case Segment(head, tail) =>
        if (head contains prefix) tail
        else key_path(tail)
      case Empty => Empty
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
  def keyFromQueryToPath(ctx: WabaseRequestContext): WabaseRequestContext = {
    val request = ctx.req
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
        ctx.copy(req = request.withUri(uri))
      case _ => ctx
    }
  }

  def viewActionKey(view_action: String, ctx: WabaseRequestContext): WabaseRequestContext = {
    import ctx._
    val viewDefs = wabase.qe.nameToViewDef
    val (viewNameAndActionStr, view_name, create_count_action) = try {
      val CreateCountActionAndViewRegex(cca, vn) = view_action
      if (viewDefs.contains(vn)) (view_action, vn, cca)
      else (null, null, null)
     } catch {
      case ex: scala.MatchError =>
        throw new RuntimeException(s"Unsupported view_name: $view_action", ex)
     }

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
        case x        => error(StatusCodes.MethodNotAllowed, s"Unsupported http method $x for request '${req.uri}'")
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

  def doAction(view_action: String, reqCtx: WabaseRequestContext): Future[HttpResponse] = {
    def extractParams(ctx: WabaseRequestContext) = {
      import ctx._
      AppServiceBase.filterParams(
        wabase.qe.metadataConventions, AppServiceBase.NamesForInts, AppServiceBase.escapeReflectedXss
      )(WabaseService.parameterMultiMap(req))
    }
    def dwa(ctx: WabaseRequestContext, params: Map[String, Any]) = {
      if (ctx.viewName == null || !ctx.wabase.qe.nameToViewDef.contains(ctx.viewName))
        if (ctx.viewName != null)
          error(StatusCodes.NotFound, s"View '${ctx.wabase.sanitizedViewName(ctx.viewName)}' not found!")
        else error(StatusCodes.NotFound, s"View not found!")
      else {
        val updatedCtx = withReqTimeout(withReqMaxContentSize(ctx))
        import updatedCtx._
        implicit val ec = as.dispatcher
        val valuesF =
          if (Set(Action.Insert, Action.Update, Action.Save).contains(action))
            toMapForViewEntityDecoder(updatedCtx)
          else  Future.successful(Map[String, Any]())
        valuesF.flatMap { values =>
          updatedCtx.wabase.app.doAction(
            actionName = action,
            viewName = viewName,
            keyValues = updatedCtx.key,
            params = params,
            values = values,
            resultFilter = resultFilter,
          )(updatedCtx)
        }.flatMap { result =>
          Marshal(result).toResponseFor(updatedCtx.req)(wabase.toResponseWabaseResultMarshaller, ec)
        }
      }
    }
    val ctxWithView = if (reqCtx.viewName == null) viewActionKey(view_action, reqCtx) else reqCtx
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

  def doActionWithKeyToPath(view_action: String, reqCtx: WabaseRequestContext): Future[HttpResponse] = {
    doAction(view_action, keyFromQueryToPath(reqCtx))
  }

  def withReqMaxContentSize(ctx: WabaseRequestContext): WabaseRequestContext = {
    if (ctx.viewName == null) ctx else {
      val vd = ctx.wabase.qe.viewDef(ctx.viewName)
      if (vd.maxContentSize == null) ctx else {
        val req = ctx.req.withEntity(ctx.req.entity.withSizeLimit(vd.maxContentSize))
        ctx.copy(req = req)
      }
    }
  }

  def withReqTimeout(ctx: WabaseRequestContext): WabaseRequestContext = {
    if (ctx.viewName == null) ctx else {
      val vd = ctx.wabase.qe.viewDef(ctx.viewName)
      if(vd.timeout == null) ctx else {
        ctx.req.header[`Timeout-Access`].map(_.timeoutAccess.updateTimeout(vd.timeout))
          .getOrElse(ctx.logger.warn(s"request timeout is defined for view ${vd.name}, however no request-timeout http header is set!"))
        ctx
      }
    }
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

  def buildRequestHandler(cn: String, fn: String,
                          invocationArgs: List[HandlerArgsParser.HandlerArg],
                          ih: RequestHandler): RequestHandler = wrc => {
    wrc.logger.debug(s"Invoking handler $cn.$fn for request: ${wrc.req}")
    implicit val ec: ExecutionContext = wrc.as.dispatcher
    def missingHandlerError = sys.error(s"Handler argument missing for invocation: '$cn.$fn'")
    val (paramList, paramFunction) = handlerParameters(wrc, ih, invocationArgs, missingHandlerError)
    val result = invokeFunction(cn, fn, paramList, paramFunction)
    handlerResult(wrc, result).flatMap {
      case c: WabaseRequestContext => if (ih == null) missingHandlerError else ih(c)
      case r: HttpResponse => Future.successful(r)
      case h: RequestHandler@unchecked => h(wrc)
    }
  }

  def handlerParameters(
    wrc: WabaseRequestContext,
    innerHandler: RequestHandler,
    invocationArgs: List[HandlerArgsParser.HandlerArg],
    missingHandlerError: => Nothing,
  ): (Seq[(Class[_], () => Any)], InvocationParameterFun) = {
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

    val paramFunction = invocationArgs.map {
      case HandlerArgsParser.StringArg(s) => s
      case HandlerArgsParser.RegexGroupRef(nr) =>
        pathMatchedGroups(wrc).flatMap(_.lift(nr - 1))
          .getOrElse(sys.error(
              s"Group nr '$nr' not found in route '${wrc.route.path}' for path '${wrc.req.uri.path}'"))
      case HandlerArgsParser.NumberArg(n) => n
    }.zipWithIndex.map { case (value, idx) =>
      { case (par, i) if idx == i => wrc.wabase.qe.convertToType(value, par.getType) }:InvocationParameterFun
    }.foldLeft(PartialFunction.empty[InvocationParameter, Any])(_ orElse _) orElse
      AppQuerease.dtoParameterFromMapF(() => toMapEntityDecoder(wrc))(wrc.wabase.qio)

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
      case x => sys.error(s"Request transformer must return either WabaseRequestContext or HttpRequest or Future of them." +
        s" Instead got: $x")
    }
    processResult(res)
  }

  def jsonResponse(resp: Any): HttpResponse =
    HttpResponse(entity = HttpEntity(MediaTypes.`application/json`, ResultEncoder.encodeAnyToJsonString(resp)))

  def errorHandler(wrc: WabaseRequestContext): WabaseService.ErrorHandler = {
    implicit val ec: ExecutionContext = wrc.as.dispatcher
    val eh = wrc.route.errorHandler
    invokeFunction(eh.className, eh.function, Seq((classOf[WabaseRequestContext], () => wrc))) match {
      case h: WabaseService.ErrorHandler@unchecked => h
      case x => sys.error(s"Error handler for route ${wrc.route.path} must return value of type:" +
        s" WabaseService.ErrorHandler, instead got '$x' of type '${x.getClass}'")
    }
  }

  def error(status: StatusCode, msg: String) = throw new HttpException(status, msg)
}

object ApplicationStateExtractor {
  def extractState(ctx: WabaseRequestContext): ApplicationState =
    extractStateForPrefix(AppServiceBase.ApplicationStateCookiePrefix, ctx)
  def extractStateForPrefix(prefix: String, ctx: WabaseRequestContext): ApplicationState = {
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

object HandlerArgsParser extends QueryParsers {
  trait HandlerArg
  case class RegexGroupRef(nr: Int) extends HandlerArg
  case class StringArg(str: String) extends HandlerArg
  case class NumberArg(value: Long) extends HandlerArg
  def groupRef: MemParser[RegexGroupRef] = "\\$(\\d+)".r ^^ {
    gr => RegexGroupRef(gr.substring(1).toInt)
  } named "regex-group-arg"
  def stringArg: MemParser[StringArg] = stringLiteral ^^ StringArg named "string-arg"
  def numberArg: MemParser[NumberArg] = "\\d+".r ^^ (v => NumberArg(v.toLong)) named "number-arg"
  def arg: MemParser[HandlerArg] = stringArg | groupRef | numberArg
  def parsArg(value: String): HandlerArg = {
    phrase(arg)(new CharSequenceReader(value)) match {
      case Success(r, _) => r
      case x => sys.error(x.toString)
    }
  }
  def argValues(args: List[Action.Op]): List[HandlerArg] = args.map {
    case t: Action.Tresql => parsArg(t.tresql)
    case x => sys.error(s"Invalid handler arg: '$x'. Only string constants or regexp group refs allowed")
  }
}
